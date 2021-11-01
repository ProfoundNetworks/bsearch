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
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"

	"github.com/DataDog/zstd"
	"github.com/rs/zerolog"
	"launchpad.net/gommap"
)

var (
	ErrNotFound            = errors.New("not found")
	ErrKeyExceedsBlocksize = errors.New("key length exceeds blocksize")
	ErrNotFile             = errors.New("filepath exists but is not a file")
	ErrUnknownDelimiter    = errors.New("cannot guess delimiter from filename")
)

var reCompressed = regexp.MustCompile(`\.zst$`)

type SearcherOptions struct {
	MatchLE bool            // use less-than-or-equal-to match semantics
	Logger  *zerolog.Logger // debug logger
	// Index options
	Delimiter []byte // delimiter separating fields in dataset
	Header    bool   // first line of dataset is header and should be ignored
}

// Searcher provides binary search functionality on byte-ordered CSV-style
// delimited text files.
type Searcher struct {
	r          io.ReaderAt     // data reader
	l          int64           // data length
	mmap       []byte          // data mmap
	buf        []byte          // data buffer
	bufOffset  int64           // data buffer offset
	dbuf       []byte          // decompressed data buffer
	dbufOffset int64           // decompressed data buffer offset
	filepath   string          // filename path
	Index      *Index          // bsearch index
	matchLE    bool            // LinePosition uses less-than-or-equal-to match semantics
	logger     *zerolog.Logger // debug logger
}

// setOptions sets the given options on searcher
func (s *Searcher) setOptions(options SearcherOptions) {
	if options.MatchLE {
		s.matchLE = true
	}
	if options.Logger != nil {
		s.logger = options.Logger
	}
}

// isCompressed returns true if there is an underlying file that is compressed
func (s *Searcher) isCompressed() bool {
	if s.filepath != "" {
		if reCompressed.MatchString(s.filepath) {
			return true
		}
		return false
	}
	if reCompressed.MatchString(s.Index.Filepath) {
		return true
	}
	return false
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
		return nil, err
	}
	if stat.IsDir() {
		return nil, ErrNotFile
	}
	filesize := stat.Size()

	// Open file
	//rdr, err := mmap.Open(path)
	rdr, err := os.Open(path)
	if err != nil {
		return nil, err
	}

	//buf := make([]byte, defaultBlocksize+1)
	mmap, err := gommap.Map(rdr.Fd(), gommap.PROT_READ, gommap.MAP_PRIVATE)
	if err != nil {
		return nil, err
	}

	s := Searcher{
		r:          rdr,
		l:          filesize,
		mmap:       mmap,
		buf:        nil,
		bufOffset:  -1,
		dbufOffset: -1,
		filepath:   path,
	}
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

func (s *Searcher) readBlockEntryMmap(entry IndexEntry) error {
	s.buf = s.mmap[entry.Offset : entry.Offset+entry.Length]
	return nil
}

func (s *Searcher) readBlockEntry(entry IndexEntry) error {
	// Noop if already done
	if s.bufOffset == entry.Offset {
		return nil
	}

	if entry.Length > int64(cap(s.buf)) {
		s.buf = make([]byte, entry.Length)
	} else {
		s.buf = s.buf[:entry.Length]
	}

	bytesread, err := s.r.ReadAt(s.buf, entry.Offset)
	if err != nil && err != io.EOF {
		s.bufOffset = -1
		return err
	}
	if int64(bytesread) < entry.Length {
		s.bufOffset = -1
		return fmt.Errorf("error reading block entry - read %d bytes, expected %d\n", bytesread, entry.Length)
	}

	s.bufOffset = entry.Offset
	return nil
}

func (s *Searcher) decompressBlockEntry(entry IndexEntry) error {
	// Noop if already done
	if s.dbufOffset == entry.Offset {
		return nil
	}

	// Read entry block into s.buf
	err := s.readBlockEntry(entry)
	if err != nil {
		return err
	}
	//fmt.Printf("+ readBlockEntry ok, len %d\n", len(s.buf))

	// Decompress
	s.dbuf, err = zstd.Decompress(s.dbuf, s.buf)
	if err != nil {
		s.dbufOffset = -1
		return err
	}

	s.dbufOffset = entry.Offset
	return nil
}

func getNBytesFrom(buf []byte, length int, delim []byte) []byte {
	segment := buf[:length]

	// If segment includes a delimiter, truncate it there
	if d := bytes.Index(segment, delim); d > -1 {
		return segment[:d]
	}

	return segment
}

// scanLineOffset returns the offset of the first line within buf that
// begins with the key k.
// If not found and the MatchLE is not set, it returns an offset of -1.
// If not found and the MatchLE flag IS set, it returns the last line
// position with a byte sequence < b.
// It also returns a terminate flag which is true we have reached a
// termination condition (e.g. a byte sequence > b).
func (s *Searcher) scanLineOffset(buf []byte, k []byte) (int, bool) {
	var trailing int = -1
	offset := 0
	terminate := false
	delim := s.Index.Delimiter

	// Scan lines until we find one >= b
	for offset < len(buf) {
		offsetPrefix := getNBytesFrom(buf[offset:], len(k), delim)
		cmp := prefixCompare(offsetPrefix, k)
		/*
			if s.logger != nil {
				s.logger.Trace().
					Int("offset", offset).
					Str("offsetPrefix", string(offsetPrefix)).
					Str("key", string(k)).
					Int("cmp", cmp).
					Msg("scanLineOffset loop check")
			}
		*/
		if cmp == 0 {
			return offset, false
		} else if cmp == 1 {
			terminate = true
			break
		}

		// Current line < b - find next newline
		nlidx := bytes.IndexByte(buf[offset:], '\n')
		if nlidx == -1 {
			// If no new newline is found, we're done
			break
		}

		// Newline found - update offset
		trailing = offset
		offset += nlidx + 1
	}

	// If using less-than-or-equal-to semantics, return trailingPosition if set
	if s.matchLE && trailing > -1 {
		return trailing, terminate
	}

	return -1, terminate
}

// scanLinesMatching returns the first n lines beginning with key from buf.
// Also returns a terminate flag which is true if we have reached a
// termination condition (e.g. a byte sequence > key, or we hit n).
func (s *Searcher) scanLinesMatching(buf, key []byte, n int) ([][]byte, bool) {
	// Find the offset of the first line in buf beginning with b
	offset, terminate := s.scanLineOffset(buf, key)
	if offset == -1 || terminate {
		return [][]byte{}, terminate
	}
	if s.logger != nil {
		s.logger.Debug().
			Bytes("key", key).
			Int("offset", offset).
			Msg("scanLinesMatching line1")
	}

	delim := s.Index.Delimiter
	var lines [][]byte
	for offset < len(buf) {
		if n > 0 && len(lines) >= n {
			return lines[:n], true
		}

		offsetPrefix := getNBytesFrom(buf[offset:], len(key), delim)
		cmp := prefixCompare(offsetPrefix, key)
		nlidx := bytes.IndexByte(buf[offset:], '\n')
		/*
			if s.logger != nil {
				s.logger.Trace().
					Bytes("key", key).
					Int("offset", offset).
					Str("offsetPrefix", string(offsetPrefix)).
					Int("cmp", cmp).
					Int("nlidx", nlidx).
					Msg("scanLinesMatching loop")
			}
		*/
		if cmp < 0 {
			if nlidx == -1 {
				break
			}
			offset += nlidx + 1
			continue
		}
		if cmp == 0 {
			// Key search, so check the next segment is our delimiter
			// (if it's not this is a prefix match, not a key match - break)
			i := offset + len(offsetPrefix)
			if !bytes.HasPrefix(buf[i:], s.Index.Delimiter) {
				break
			}

			if nlidx == -1 {
				lines = append(lines, clone(buf[offset:]))
				break
			}

			lines = append(lines, clone(buf[offset:offset+nlidx]))
			offset += nlidx + 1
			continue
		}
		// cmp > 0
		terminate = true
		break
	}
	return lines, terminate
}

// scanIndexedLines returns the first n lines from reader that begin with key.
// Returns a slice of byte slices on success.
func (s *Searcher) scanIndexedLines(key []byte, n int) ([][]byte, error) {
	var lines [][]byte
	var entry IndexEntry
	var e int
	var err error
	if s.Index.KeysIndexFirst {
		// If index entries always index have the first instance of a key,
		// we can use a more efficient less-than-or-equal-to block lookup
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
		s.logger.Debug().
			Bytes("key", key).
			Int("entryIndex", e).
			Str("entry.Key", entry.Key).
			Int64("entry.Offset", entry.Offset).
			Int64("entry.Length", entry.Length).
			Str("blockEntry", blockEntry).
			Msg("scanIndexedLines blockEntryXX returned")
	}

	var l [][]byte
	var terminate, ok bool
	// Loop because we may need to read multiple blocks
	for {
		// Read entry block into s.buf
		err := s.readBlockEntryMmap(entry)
		if err != nil {
			if s.logger != nil {
				s.logger.Error().
					Str("error", err.Error()).
					Msg("readBlockEntry error")
			}
			return lines, err
		}

		// Scan matching lines
		l, terminate = s.scanLinesMatching(s.buf, key, n)
		if len(l) > 0 {
			lines = append(lines, l...)
		}
		if terminate {
			break
		}

		// Check next block
		e++
		entry, ok = s.Index.blockEntryN(e)
		if !ok {
			break
		}
	}

	if len(lines) == 0 {
		return lines, ErrNotFound
	}

	return lines, nil
}

// scanCompressedLines returns all decompressed lines in the reader that
// begin with the key k (data must be bytewise-ordered).
// Note that the index ensures blocks always finish cleanly on newlines.
// Returns a slice of byte slices on success, and an empty slice of
// byte slices and an error on error.
func (s *Searcher) scanCompressedLines(k []byte, n int) ([][]byte, error) {
	e, entry := s.Index.blockEntryLT(k)

	var lines, l [][]byte
	var terminate, ok bool
	// Loop because we may need to read multiple blocks
	for {
		// Decompress block from entry into s.dbuf
		err := s.decompressBlockEntry(entry)
		if err != nil {
			return [][]byte{}, err
		}
		//fmt.Printf("+ block for entry %d decompressed\n", entry.Offset)

		// Scan matching lines
		l, terminate = s.scanLinesMatching(s.dbuf, k, n)
		if len(l) > 0 {
			lines = append(lines, l...)
		}
		if terminate {
			break
		}

		entry, ok = s.Index.blockEntryN(e + 1)
		if !ok {
			break
		}
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

	if s.isCompressed() {
		if s.Index == nil {
			return [][]byte{}, ErrIndexNotFound
		}
		return s.scanCompressedLines(key, n)
	}

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

/*
// Reader returns the searcher's reader
func (s *Searcher) Reader() io.ReaderAt {
	return s.r
}
*/

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

// clone returns a copy of the given byte slice
func clone(b []byte) []byte {
	c := make([]byte, len(b))
	copy(c, b)
	return c
}
