/*
bsearch provides binary search functionality for line-ordered byte streams
by prefix (e.g. for searching `LC_COLLATE=C` sorted text files).

TODO: can we change the comparison function to support UTF-8 ordered keys?
      e.g. BytesEqual, StringEqual, BytesLessEqual, StringLessEqual
TODO: should we check for/warn on non-ordered data?
*/

package bsearch

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"regexp"

	"github.com/DataDog/zstd"
)

const (
	defaultBlocksize = 4096
)

var (
	ErrNotFound            = errors.New("not found")
	ErrKeyExceedsBlocksize = errors.New("key length exceeds blocksize")
	ErrNotFile             = errors.New("filename exists but is not a file")
	ErrCompressedNoIndex   = errors.New("compressed file without an index file")
)

var reCompressed = regexp.MustCompile(`\.zst$`)

type Options struct {
	Blocksize int64                 // data blocksize used for binary search
	Compare   func(a, b []byte) int // prefix comparison function
	Header    bool                  // first line of dataset is header and should be ignored
	Boundary  bool                  // search string must be followed by a word boundary
	MatchLE   bool                  // LinePosition uses less-than-or-equal-to match semantics
	Index     IndexSemantics        // Index semantics: 1=Require, 2=Create, 3=None
}

// Searcher provides binary search functionality for line-ordered byte streams by prefix.
type Searcher struct {
	r          io.ReaderAt           // data reader
	l          int64                 // data length
	blocksize  int64                 // data blocksize used for binary search
	buf        []byte                // data buffer
	bufOffset  int64                 // data buffer offset
	dbuf       []byte                // decompressed data buffer
	dbufOffset int64                 // decompressed data buffer offset
	filepath   string                // filename path
	indexOpt   IndexSemantics        // index option: 1=Require, 2=Create, 3=None
	Index      *Index                // optional block index
	compare    func(a, b []byte) int // prefix comparison function
	header     bool                  // first line of dataset is header and should be ignored
	boundary   bool                  // search string must be followed by a word boundary
	matchLE    bool                  // LinePosition uses less-than-or-equal-to match semantics
	reWord     *regexp.Regexp        // regexp used for boundary matching
}

// setOptions sets the given options on searcher
func (s *Searcher) setOptions(options Options) {
	if options.Blocksize > 0 {
		s.blocksize = options.Blocksize
	}
	if options.Compare != nil {
		s.compare = options.Compare
	}
	if options.Header {
		s.header = true
	}
	if options.Boundary {
		s.boundary = true
		s.reWord = regexp.MustCompile(`\w`)
	}
	if options.MatchLE {
		s.matchLE = true
	}
	if options.Index > 0 && options.Index <= 3 {
		s.indexOpt = options.Index
	}
}

// isCompressed returns true if there is an underlying file that is compressed
// (and which also requires we have an associated index).
func (s *Searcher) isCompressed() bool {
	if s.filepath == "" && s.Index == nil {
		return false
	}
	if s.filepath != "" {
		if reCompressed.MatchString(s.filepath) {
			return true
		}
		return false
	}
	if reCompressed.MatchString(s.Index.Filename) {
		return true
	}
	return false
}

// NewSearcher returns a new Searcher for filename, using default options.
// NewSearcher opens the file and determines its length using os.Open and
// os.Stat - any errors are returned to the caller. The caller is responsible
// for calling *Searcher.Close() when finished (e.g. via defer).
func NewSearcher(filename string) (*Searcher, error) {
	// Get file length
	stat, err := os.Stat(filename)
	if err != nil {
		return nil, err
	}
	if stat.IsDir() {
		return nil, ErrNotFile
	}
	filesize := stat.Size()

	// Open file
	fh, err := os.Open(filename)
	if err != nil {
		return nil, err
	}

	s := Searcher{
		r:          fh,
		l:          filesize,
		blocksize:  defaultBlocksize,
		buf:        make([]byte, defaultBlocksize+1),
		bufOffset:  -1,
		dbufOffset: -1,
		compare:    PrefixCompare,
		filepath:   filename,
	}

	// Load index if one exists
	index, _ := NewIndexLoad(filename)
	if index != nil {
		s.Index = index
	}

	return &s, nil
}

// NewSearcherOptions returns a new Searcher for filename f, using options.
func NewSearcherOptions(filename string, options Options) (*Searcher, error) {
	s, err := NewSearcher(filename)
	if err != nil {
		return nil, err
	}
	s.setOptions(options)

	// Discard index if s.indexOpt == IndexNone
	if s.Index != nil && s.indexOpt == IndexNone {
		s.Index = nil
	}
	// Return an error if s.indexOpt == IndexRequired and we have no index
	if s.Index == nil && s.indexOpt == IndexRequired {
		return nil, ErrNoIndexFound
	}
	// If we have no index and IndexCreate is specified, create one
	if s.Index == nil && s.indexOpt == IndexCreate {
		index, err := NewIndex(filename)
		if err != nil {
			return nil, err
		}
		err = index.Write()
		if err != nil {
			return nil, err
		}
		s.Index = index
	}

	return s, nil
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

// scanLineOffset returns the offset of the first line within buf
// that begins with the byte sequence b.
// If not found, normally returns -1.
// If not found and the MatchLE flag is set, it returns the last line
// position with a byte sequence < b.
// Also returns a terminate flag which is true we have reached a termination
// condition (e.g. a byte sequence > b).
func (s *Searcher) scanLineOffset(buf []byte, b []byte) (int, bool) {
	var trailing int = -1
	begin := 0
	terminate := false

	// Scan lines until we find one >= b
	for begin < len(buf) {
		cmp := s.compare(buf[begin:begin+len(b)], b)
		//fmt.Fprintf(os.Stderr, "+ comparing %q (begin %d) vs. %q == %d\n", string(buf[begin:begin+len(b)]), begin, string(b), cmp)
		if cmp == 0 {
			return begin, false
		} else if cmp == 1 {
			terminate = true
			break
		}

		// Current line < b - find next newline
		nlidx := bytes.IndexByte(buf[begin:], '\n')
		if nlidx == -1 {
			// If no new newline is found, we're done
			break
		}

		// Newline found - update begin
		trailing = begin
		begin += nlidx + 1
	}

	// If using less-than-or-equal-to semantics, return trailingPosition if that is set
	if s.matchLE && trailing > -1 {
		return trailing, terminate
	}

	return -1, terminate
}

// scanLinesMatching returns all lines beginning with byte sequence b from buf.
// Also returns a terminate flag which is true if we have reached a termination
// condition (e.g. a byte sequence > b, or we hit maxlines).
func (s *Searcher) scanLinesMatching(buf, b []byte, maxlines int) ([][]byte, bool) {
	// Find the offset of the first line in buf beginning with b
	begin, terminate := s.scanLineOffset(buf, b)
	if begin == -1 || terminate {
		return [][]byte{}, terminate
	}
	//fmt.Printf("+ line offset for %q in buf: %d\n", string(b), begin)

	var lines [][]byte
	for begin < len(buf) {
		if maxlines > 0 && len(lines) >= maxlines {
			return lines[:maxlines], true
		}

		cmp := s.compare(buf[begin:begin+len(b)], b)
		nlidx := bytes.IndexByte(buf[begin:], '\n')
		//fmt.Printf("+ comparing %q (begin %d, nlidx %d) vs. %q == %d\n", string(buf[begin:begin+len(b)]), begin, nlidx, string(b), cmp)
		if cmp < 0 {
			if nlidx == -1 {
				break
			}
			begin += nlidx + 1
			continue
		}
		if cmp == 0 {
			// Boundary checking
			if s.boundary && len(buf) > begin+len(b) {
				// FIXME: does this need to done rune-wise, rather than byte-wise?
				blast := buf[begin+len(b)-1 : begin+len(b)]
				bnext := buf[begin+len(b) : begin+len(b)+1]
				if (s.reWord.Match(blast) && s.reWord.Match(bnext)) ||
					(!s.reWord.Match(blast) && !s.reWord.Match(bnext)) {
					// Boundary check fails - skip this line
					if nlidx == -1 {
						break
					} else {
						begin += nlidx + 1
						continue
					}
				}
			}

			if nlidx == -1 {
				lines = append(lines, clone(buf[begin:]))
				break
			}

			lines = append(lines, clone(buf[begin:begin+nlidx]))
			begin += nlidx + 1
			continue
		}
		// cmp > 0
		terminate = true
		break
	}
	return lines, terminate
}

// Line returns the first line in the reader that begins with the byte
// slice b, using a binary search (data must be bytewise-ordered).
// Returns an empty byte slice and bsearch.ErrNotFound if no matching line
// is found, and an empty byte slice and the error on any other error.
func (s *Searcher) Line(b []byte) ([]byte, error) {
	lines, err := s.LinesLimited(b, 1)
	if err != nil || len(lines) < 1 {
		return []byte{}, err
	}
	return lines[0], nil
}

// linesReadNextBlock is a helper function to read the next block and
// distinguish between eof and other errors, to simplify processing.
func linesReadNextBlock(r io.ReaderAt, b []byte, pos int64) (bytesread int, eof bool, err error) {
	bytesread, err = r.ReadAt(b, pos)
	if err != nil && err == io.EOF {
		return bytesread, true, nil
	}
	if err != nil {
		return bytesread, false, err
	}
	return bytesread, false, nil
}

// scanIndexedLines returns all lines in s.r that begin with the byte slice b
// (data must be bytewise-ordered).
// Note that the index ensures blocks always finish cleanly on newlines.
// Returns a slice of byte slices on success, and an empty slice of byte slices
// and an error on error.
func (s *Searcher) scanIndexedLines(b []byte, maxlines int) ([][]byte, error) {
	e, entry := s.Index.BlockEntry(b)
	//fmt.Printf("+ s.Index.BlockEntry(%q) returned: %d, %s/%d/%d\n", string(b), e, entry.Key, entry.Offset, entry.Length)

	var lines, l [][]byte
	var terminate, ok bool
	// Loop because we may need to read multiple blocks
	for {
		// Read entry block into s.buf
		err := s.readBlockEntry(entry)
		if err != nil {
			return [][]byte{}, err
		}

		// Scan matching lines
		l, terminate = s.scanLinesMatching(s.buf, b, maxlines)
		if len(l) > 0 {
			lines = append(lines, l...)
		}
		if terminate {
			break
		}

		// Check next block
		e++
		entry, ok = s.Index.BlockEntryN(e)
		if !ok {
			break
		}
	}

	if len(lines) == 0 {
		return lines, ErrNotFound
	}

	return lines, nil
}

// scanCompressedLines returns all decompressed lines in s.r that begin
// with the byte slice b (data must be bytewise-ordered).
// Note that the index ensures blocks always finish cleanly on newlines.
// Returns a slice of byte slices on success, and an empty slice of
// byte slices and an error on error.
func (s *Searcher) scanCompressedLines(b []byte, maxlines int) ([][]byte, error) {
	e, entry := s.Index.BlockEntry(b)

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
		l, terminate = s.scanLinesMatching(s.dbuf, b, maxlines)
		if len(l) > 0 {
			lines = append(lines, l...)
		}
		if terminate {
			break
		}

		entry, ok = s.Index.BlockEntryN(e + 1)
		if !ok {
			break
		}
	}

	return lines, nil
}

// LinesLimited returns the first maxlines lines in the reader that
// begin with the byte slice b, using a binary search (data must be
// bytewise-ordered).
// Returns an empty slice of byte slices and bsearch.ErrNotFound
// if no matching line is found, and an empty slice of byte slices
// and the error on any other error.
func (s *Searcher) LinesLimited(b []byte, maxlines int) ([][]byte, error) {
	if s.isCompressed() {
		if s.Index == nil {
			return [][]byte{}, ErrCompressedNoIndex
		}
		return s.scanCompressedLines(b, maxlines)
	}

	// If no index exists, build and use a temporary one (but don't write)
	if s.Index == nil {
		index, err := NewIndex(s.filepath)
		if err != nil {
			return [][]byte{}, err
		}
		s.Index = index
	}

	return s.scanIndexedLines(b, maxlines)
}

// Lines returns all lines in the reader that begin with the byte
// slice b, using a binary search (data must be bytewise-ordered).
// Returns an empty slice of byte slices and bsearch.ErrNotFound
// if no matching line is found, and an empty slice of byte slices
// and the error on any other error.
func (s *Searcher) Lines(b []byte) ([][]byte, error) {
	return s.LinesLimited(b, 0)
}

// checkPrefixMatch checks that the initial sequences of bufa matches b
// (up to len(b) only).
// Returns an error if the bufa prefix < b (the underlying data is
// incorrectly sorted), returns brk=true if bufa prefix > b, and a
// copy of bufa if bufa prefix == b.
func (s *Searcher) checkPrefixMatch(bufa, b []byte) (clonea []byte, brk bool, err error) {
	cmp := PrefixCompare(bufa, b)
	if cmp < 0 {
		// This should never happen unless the file is wrongly sorted
		return []byte{}, false,
			fmt.Errorf("Error: badly sorted file? (%q < expected %q)", bufa, b)
	} else if cmp > 0 {
		// End of matching lines - we're done
		return []byte{}, true, nil
	}

	// Prefix matches. If s.Boundary is set we also require a word boundary.
	if s.boundary && len(bufa) > len(b) {
		// FIXME: this might need to done rune-wise, rather than byte-wise?
		blast := bufa[len(b)-1 : len(b)]
		bnext := bufa[len(b) : len(b)+1]
		if (s.reWord.Match(blast) && s.reWord.Match(bnext)) ||
			(!s.reWord.Match(blast) && !s.reWord.Match(bnext)) {
			// Returning an empty byteslice here will cause this line to be skipped
			return []byte{}, false, nil
		}
	}

	return clone(bufa), false, nil
}

// Reader returns the searcher's underlying reader
func (s *Searcher) Reader() io.ReaderAt {
	return s.r
}

// Close closes the searcher's underlying reader (if applicable)
func (s *Searcher) Close() {
	if closer, ok := s.r.(io.Closer); ok {
		closer.Close()
	}
}

// PrefixCompare compares the initial sequence of bufa matches b
// (up to len(b) only).
// Used as the default compare function in NewSearcher.
func PrefixCompare(bufa, b []byte) int {
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

// PrefixCompareString compares the initial bytes of bufa matches b
// (up to len(b) only), after conversion to strings.
// Can be used as the compare function via NewSearcherOptions.
func PrefixCompareString(bufa, b []byte) int {
	sa := string(bufa)
	sb := string(b)

	// If len(sa) < len(sb) we compare up to len(sa), but disallow equality
	if len(sa) < len(sb) {
		sb = sb[:len(sa)]
		switch {
		case sa < sb:
			return -1
		case sa == sb:
			// An equal match here is short, so actually a less than
			return -1
		}
		return 1
	}

	sa = sa[:len(sb)]
	switch {
	case sa < sb:
		return -1
	case sa == sb:
		return 0
	}
	return 1
}

// clone returns a copy of the given byte slice
func clone(b []byte) []byte {
	c := make([]byte, len(b))
	copy(c, b)
	return c
}
