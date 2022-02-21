/*
bsearch provides binary search functionality for line-ordered byte streams
by prefix (e.g. for searching `LC_ALL=C` sorted text files).

TODO: can we change the comparison function to support UTF-8 ordered keys?
      e.g. BytesEqual, StringEqual, BytesLessEqual, StringLessEqual
*/

package bsearch

import (
	"bytes"
	"errors"
	"io"
	"os"
	"path/filepath"
	"regexp"

	"github.com/rs/zerolog"
	"golang.org/x/sys/unix"
	"launchpad.net/gommap"
)

var (
	ErrFileNotFound        = errors.New("filepath not found")
	ErrNotFile             = errors.New("filepath exists but is not a file")
	ErrFileCompressed      = errors.New("filepath exists but is compressed")
	ErrNotFound            = errors.New("key not found")
	ErrKeyExceedsBlocksize = errors.New("key length exceeds blocksize")
	ErrUnknownDelimiter    = errors.New("cannot guess delimiter from filename")

	reCompressedUnsupported = regexp.MustCompile(`\.(zst|gz|bz2|xz|zip)$`)
)

// SearcherOptions struct for use with NewSearcherOptions
type SearcherOptions struct {
	MatchLE bool            // use less-than-or-equal-to match semantics
	Logger  *zerolog.Logger // debug logger
	// Index options (used to check index or build new one)
	Delimiter []byte // delimiter separating fields in dataset
	Header    bool   // first line of dataset is header and should be ignored
}

// Searcher provides binary search functionality on byte-ordered CSV-style
// delimited text files.
type Searcher struct {
	r        io.ReaderAt     // data reader
	l        int64           // data length
	mmap     []byte          // data mmap
	filepath string          // filename path
	Index    *Index          // bsearch index
	matchLE  bool            // LinePosition uses less-than-or-equal-to match semantics
	logger   *zerolog.Logger // debug logger
}

//buf      []byte          // data buffer
//bufOffset int64       // data buffer offset
//dbuf       []byte          // decompressed data buffer
//dbufOffset int64           // decompressed data buffer offset

// setOptions sets the given options on searcher
func (s *Searcher) setOptions(options SearcherOptions) {
	if options.MatchLE {
		s.matchLE = true
	}
	if options.Logger != nil {
		s.logger = options.Logger
	}
}

// NewSearcher returns a new Searcher for path using default options.
// The caller is responsible for calling *Searcher.Close() when finished.
func NewSearcher(path string) (*Searcher, error) {
	return NewSearcherOptions(path, SearcherOptions{})
}

// NewSearcherOptions returns a new Searcher for path using opt.
// The caller is responsible for calling *Searcher.Close() when finished.
func NewSearcherOptions(path string, opt SearcherOptions) (*Searcher, error) {
	path, err := filepath.Abs(path)
	if err != nil {
		return nil, err
	}

	// Get file length and epoch
	stat, err := os.Stat(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, ErrFileNotFound
		}
		return nil, err
	}
	if stat.IsDir() {
		return nil, ErrNotFile
	}
	filesize := stat.Size()

	// Open file
	rdr, err := os.Open(path)
	if err != nil {
		return nil, err
	}

	// Mmap file
	mmap, err := gommap.Map(rdr.Fd(), gommap.PROT_READ, gommap.MAP_PRIVATE)
	if err != nil {
		return nil, err
	}

	s := Searcher{
		r:        rdr,
		l:        filesize,
		mmap:     mmap,
		filepath: path,
	}
	//buf:  nil,
	//bufOffset: -1,
	//dbufOffset: -1,
	s.setOptions(opt)

	// Load index
	s.Index, err = LoadIndex(path)
	if err != nil && err != ErrNotFound &&
		err != ErrIndexExpired && err != ErrIndexPathMismatch {
		return nil, err
	}
	if err == nil {
		// Existing index found/loaded - sanity check against explicit options
		// (or we fallthrough and re-create the index below)
		if (len(opt.Delimiter) == 0 ||
			bytes.Compare(opt.Delimiter, s.Index.Delimiter) == 0) &&
			(opt.Header == false || opt.Header == s.Index.Header) {
			return &s, nil
		}
	}

	// ErrNotFound, or an expired/mismatched index of some kind
	if s.logger != nil {
		s.logger.Debug().
			Bool("expired", err == ErrIndexExpired).
			Bool("path_mismatch", err == ErrIndexPathMismatch).
			Str("path", path).
			Msg("expired/mismatched index")
	}
	// Check that we have write permissions to the index
	idxErr := err
	idxpath, err := IndexPath(path)
	if err != nil {
		return nil, err
	}
	err = unix.Access(idxpath, unix.W_OK)
	if err != nil {
		// If we cannot write to the index, return the original idxErr
		return nil, idxErr
	}

	idxopt := IndexOptions{
		Delimiter: opt.Delimiter,
		Header:    opt.Header,
	}
	s.Index, err = NewIndexOptions(path, idxopt)
	if err != nil {
		return nil, err
	}
	err = s.Index.Write()
	if err != nil {
		return nil, err
	}

	return &s, nil
}

func getNBytesFrom(buf []byte, length int, delim []byte) []byte {
	segment := buf[:length]

	// If segment includes a delimiter, truncate it there
	if d := bytes.Index(segment, delim); d > -1 {
		return segment[:d]
	}

	return segment
}

// scanLinesWithKey returns the first n lines beginning with key from buf.
func (s *Searcher) scanLinesWithKey(buf, key []byte, n int) [][]byte {
	// This differs from the old scanLinesMatching in that it assumes
	// that buf contains *all* lines we might need, rather than just
	// an initial block.
	var lines [][]byte

	// Skip lines with a key < ours
	keyde := append(key, s.Index.Delimiter...)
	offset := 0
	for offset < len(buf) {
		k := getNBytesFrom(buf[offset:], len(key), s.Index.Delimiter)
		if bytes.Compare(k, key) > -1 {
			break
		}
		nlidx := bytes.IndexByte(buf[offset:], '\n')
		if nlidx == -1 {
			// If no new newline is found, there are no more lines to check
			return lines
		}
		offset += nlidx + 1
	}

	// Collate up to n lines beginning with keyde
	for offset < len(buf) && bytes.HasPrefix(buf[offset:], keyde) {
		nlidx := bytes.IndexByte(buf[offset:], '\n')
		if nlidx == -1 {
			// If no newline found, read to end of buf
			nlidx = len(buf) - offset
		}
		lines = append(lines, clonebs(buf[offset:offset+nlidx]))
		if n > 0 && len(lines) >= n {
			break
		}
		offset += nlidx + 1
	}

	return lines
}

// scanIndexedLines returns the first n lines from reader that begin with key.
// Returns a slice of byte slices on success.
func (s *Searcher) scanIndexedLines(key []byte, n int) ([][]byte, error) {
	var lines [][]byte
	var entry IndexEntry
	var e int
	var err error
	if s.Index.KeysIndexFirst {
		// If index entries always have the first instance of a key, we
		// can use the more efficient less-than-or-equal-to block lookup
		e, entry, err = s.Index.blockEntryLE(key)
		if err != nil {
			return lines, err
		}
	} else {
		e, entry = s.Index.blockEntryLT(key)
	}
	if s.logger != nil {
		blockEntry := "blockEntryLT"
		if s.Index.KeysIndexFirst {
			blockEntry = "blockEntryLE"
		}
		s.logger.Trace().
			Bytes("key", key).
			Int("entryIndex", e).
			Str("entry.Key", entry.Key).
			Int64("entry.Offset", entry.Offset).
			//Int64("entry.Length", entry.Length).
			Str("blockEntry", blockEntry).
			Msg("scanIndexedLines blockEntryXX returned")
	}

	lines = s.scanLinesWithKey(s.mmap[entry.Offset:], key, n)
	if len(lines) == 0 {
		return lines, ErrNotFound
	}
	return lines, nil
}

// Line returns the first line in the reader that begins with key,
// using a binary search (data must be bytewise-ordered).
func (s *Searcher) Line(key []byte) ([]byte, error) {
	lines, err := s.LinesN(key, 1)
	if err != nil || len(lines) < 1 {
		return []byte{}, err
	}
	return lines[0], nil
}

// Lines returns all lines in the reader that begin with the byte
// slice b, using a binary search (data must be bytewise-ordered).
func (s *Searcher) Lines(b []byte) ([][]byte, error) {
	return s.LinesN(b, 0)
}

// LinesN returns the first n lines in the reader that begin with key,
// using a binary search (data must be bytewise-ordered).
func (s *Searcher) LinesN(key []byte, n int) ([][]byte, error) {
	// If keys are unique max(n) is 1
	if n == 0 && s.Index.KeysUnique {
		n = 1
	}

	/*
		// FIXME: revisit compression
		if s.isCompressed() {
			if s.Index == nil {
				return [][]byte{}, ErrIndexNotFound
			}
			return s.scanCompressedLines(key, n)
		}
	*/

	// If no index exists, build and use a temporary one (but don't write)
	if s.Index == nil {
		index, err := NewIndex(s.filepath)
		if err != nil {
			return [][]byte{}, err
		}
		s.Index = index
	}

	return s.scanIndexedLines(key, n)
}

// Close closes the searcher's reader (if applicable)
func (s *Searcher) Close() {
	if closer, ok := s.r.(io.Closer); ok {
		closer.Close()
	}
}

// prefixCompare compares the initial sequence of bufa matches b
// (up to len(b) only).
func prefixCompare(bufa, b []byte) int {
	// If len(bufa) < len(b) we compare up to len(bufa), but disallow equality
	if len(bufa) < len(b) {
		cmp := bytes.Compare(bufa, b[:len(bufa)])
		if cmp == 0 {
			// An equal match here is short, so actually a less than
			return -1
		}
		return cmp
	}

	return bytes.Compare(bufa[:len(b)], b)
}

// clonebs returns a copy of the given byte slice
func clonebs(b []byte) []byte {
	c := make([]byte, len(b))
	copy(c, b)
	return c
}
