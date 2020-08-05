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
	"io"
	"os"
)

const (
	defaultBlocksize = 4096
)

var (
	ErrNotFound             = errors.New("not found")
	ErrLineExceedsBlocksize = errors.New("line length exceeds blocksize")
	ErrNotFile              = errors.New("filename exists but is not a file")
)

type Options struct {
	Blocksize int64                 // data blocksize used for binary search
	Compare   func(a, b []byte) int // prefix comparison function
	MatchLE   bool                  // LinePosition uses less-than-or-equal-to match semantics
}

// Searcher provides binary search functionality for line-ordered byte streams by prefix.
type Searcher struct {
	r         io.ReaderAt           // data reader
	l         int64                 // data length
	blocksize int64                 // data blocksize used for binary search
	buf       []byte                // data buffer (blocksize+1)
	compare   func(a, b []byte) int // prefix comparison function
	matchLE   bool                  // LinePosition uses less-than-or-equal-to match semantics
}

// setOptions sets the given options on searcher
func (s *Searcher) setOptions(options Options) {
	if options.Blocksize > 0 {
		s.blocksize = options.Blocksize
	}
	if options.Compare != nil {
		s.compare = options.Compare
	}
	if options.MatchLE {
		s.matchLE = true
	}
}

// NewSearcher returns a new Searcher for the ReaderAt r for data of length bytes,
// using default options.
func NewSearcher(r io.ReaderAt, length int64) *Searcher {
	s := Searcher{r: r, l: length, blocksize: defaultBlocksize, compare: PrefixCompare}
	s.buf = make([]byte, s.blocksize+1) // we read blocksize+1 bytes to check for a preceding newline
	return &s
}

// NewSearcherOptions returns a new Searcher for the ReaderAt r for data of length bytes,
// overriding the default options with those in options.
func NewSearcherOptions(r io.ReaderAt, length int64, options Options) *Searcher {
	s := Searcher{r: r, l: length, blocksize: defaultBlocksize, compare: PrefixCompare}
	s.setOptions(options)
	s.buf = make([]byte, s.blocksize+1) // we read blocksize+1 bytes to check for a preceding newline
	return &s
}

// NewSearcherFile returns a new Searcher for filename, using default options.
// NewSearcherFile opens the file and determines its length using os.Open and
// os.Stat - any errors are returned to the caller. The caller is responsible
// for calling *Searcher.Close() when finished (e.g. via defer).
func NewSearcherFile(filename string) (*Searcher, error) {
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

	s := Searcher{r: fh, l: filesize, blocksize: defaultBlocksize, compare: PrefixCompare}
	s.buf = make([]byte, s.blocksize+1) // we read blocksize+1 bytes to check for a preceding newline
	return &s, nil
}

// NewSearcherFileOptions returns a new Searcher for filename f, using options.
func NewSearcherFileOptions(filename string, options Options) (*Searcher, error) {
	s, err := NewSearcherFile(filename)
	if err != nil {
		return nil, err
	}
	s.setOptions(options)
	return s, nil
}

// BlockPosition does a block-based binary search on its underlying reader,
// comparing only the first line of each block. It returns the byte offset
// of last block whose first line begins with a byte sequence less than b
// (using a bytewise comparison). The underlying data must therefore be
// bytewise-sorted (e.g. using a sort with `LC_COLLATE=C` set).
//
// BlockPosition does not check whether a line beginning with b actually
// exists, so it will always returns an offset unless an error occurs. On
// error, BlockPosition returns an offset of -1, and a non-nil error.
//
// Typical use cases probably want to use Line() or LinePosition() instead.
func (s *Searcher) BlockPosition(b []byte) (int64, error) {
	var begin, mid, end int64
	begin = 0
	end = s.l

	for end-begin > 0 {
		// Read data from the midpoint between begin and end (truncated to a multiple of blocksize)
		mid = ((end - begin) / 2) + begin
		mid = mid - (mid % s.blocksize)

		//fmt.Fprintf(os.Stderr, "+ %s: begin %d, end %d, mid %d\n", string(b), begin, end, mid)

		// If mid == begin, check the next block up
		if mid == begin && end > mid+s.blocksize {
			mid = mid + s.blocksize
		}
		if mid == begin || mid == end {
			//fmt.Fprintf(os.Stderr, "+ %s: mid break condition met\n", string(b))
			break
		}

		// Read block at mid (actually read from mid-1 to allow checking for a previous newline)
		bytesread, err := s.r.ReadAt(s.buf, mid-1)
		if err != nil && err != io.EOF {
			return -1, err
		}

		// If the first byte read is not a newline, we are in the middle of a line - skip to first '\n'
		cmpBegin := 1
		if s.buf[0] != '\n' {
			if idx := bytes.IndexByte(s.buf[1:bytesread], '\n'); idx == -1 {
				// If new newline is found we're either at EOF, or we have a block with no newlines at all(?!)
				if err != nil && err == io.EOF {
					break
				} else {
					// Corner case - no newlines in non-final block - just bump mid and continue?
					if end > mid+s.blocksize {
						mid = mid + s.blocksize
						continue
					} else {
						break
					}
				}
			} else {
				cmpBegin += idx + 1
			}
		} else {
			// fmt.Fprintf(os.Stderr, "+ %s: block break == line break condition!\n", string(b))
		}

		// Compare line data vs. b
		cmpEnd := cmpBegin + len(b)
		if cmpEnd > bytesread {
			// Corner case: very long lines or keys - we don't have enough in buf to do a full key comparison
			// For now just fail - not sure trying to fix/handle this case is worth it
			return 0, ErrLineExceedsBlocksize
		}

		//fmt.Fprintf(os.Stderr, "+ %s: comparing vs. %q\n", string(b), string(s.buf[cmpBegin:cmpEnd]))
		cmp := s.compare(s.buf[cmpBegin:cmpEnd], b)

		// Check line against searchStr
		if cmp == -1 {
			begin = mid
		} else {
			end = mid
		}
	}

	return begin, nil
}

// LinePosition returns the byte offset in the reader for the first line
// that begins with the byte slice b, using a binary search (data must be
// bytewise-ordered).
//
// If no line with prefix b is found, LinePosition returns -1 and
// bsearch.ErrNotFound if OptionsMatchLE is false (the default).
// If no line with prefix b is found and Options.MatchLE is true,
// LinePosition returns the byte offset of the line immediately before
// where a matching line should be i.e. the last line with a prefix less
// than b.
//
// On any other error, LinePosition returns -1 and the error.
func (s *Searcher) LinePosition(b []byte) (int64, error) {
	// Get BlockPosition to start from
	blockPosition, err := s.BlockPosition(b)
	if err != nil {
		return -1, err
	}

	// Start one byte back in case we have a block-end newline
	if blockPosition > 0 {
		blockPosition--
	}

	// Loop in case we need to read more than one block
	var trailingPosition int64 = -1
BLOCK:
	for {
		// Read next block
		bytesread, err := s.r.ReadAt(s.buf, blockPosition)
		if err != nil && err != io.EOF {
			return -1, err
		}
		readErr := err

		// Skip till first newline
		begin := 0
		if blockPosition > 0 {
			idx := bytes.IndexByte(s.buf[:bytesread], '\n')
			if idx == -1 {
				// If no new newline is found we're either at EOF, or we have a block with no newlines at all(?!)
				if err != nil && err == io.EOF {
					break
				} else {
					// Corner case - no newlines in non-eof block(?) - increment blockPosition and retry
					blockPosition += s.blocksize
					continue
				}
			}

			begin = idx + 1
		}

		// Scan lines from begin until we find one >= b
		for {
			// Compare buf from begin vs. b
			cmp := s.compare(s.buf[begin:begin+len(b)], b)
			//fmt.Fprintf(os.Stderr, "+ comparing %q (begin %d) vs. %q == %d\n",
			//	string(s.buf[begin:begin+len(b)]), begin, string(b), cmp)
			if cmp == 0 {
				return blockPosition + int64(begin), nil
			} else if cmp == 1 {
				break BLOCK
			}

			// Current line < b; find next newline
			idx := bytes.IndexByte(s.buf[begin:bytesread], '\n')
			if idx == -1 {
				if readErr == io.EOF {
					break BLOCK
				}
				// No newline found - re-read from current position
				trailingPosition = blockPosition + int64(begin)
				blockPosition = blockPosition + int64(begin) - 1
				//fmt.Fprintf(os.Stderr, "+ no newline re-read - begin=%d, blockPosition => %d\n",
				//	begin, blockPosition)
				continue BLOCK
			}

			trailingPosition = blockPosition + int64(begin)
			begin += idx + 1

			// Out of data - re-read from current newline
			if begin+len(b) > bytesread {
				if readErr == io.EOF {
					break BLOCK
				}
				//fmt.Fprintf(os.Stderr,
				//	"+ out-of-data re-read - tp=%d, bp=%d, begin=%d, limit=%d, bytesread=%d, bp => %d\n",
				//	trailingPosition, blockPosition, begin, begin+len(b), bytesread, blockPosition+int64(begin)-1)
				blockPosition = blockPosition + int64(begin) - 1
				continue BLOCK
			}
		}
	}

	// If using less-than-or-equal-to semantics, return trailingPosition if that is set
	if s.matchLE && trailingPosition > -1 {
		return trailingPosition, nil
	}

	return -1, ErrNotFound
}

// Line returns the first line in the reader that begins with the byte
// slice b, using a binary search (data must be bytewise-ordered).
// Returns an empty byte slice and bsearch.ErrNotFound if no matching line
// is found, and an empty byte slice and the error on any other error.
func (s *Searcher) Line(b []byte) ([]byte, error) {
	pos, err := s.LinePosition(b)
	if err != nil {
		return []byte{}, err
	}

	// Reread data at pos, to make sure we can read a full line
	bytesread, err := s.r.ReadAt(s.buf, pos)
	if err != nil && err != io.EOF {
		return []byte{}, err
	}

	idx := bytes.IndexByte(s.buf[:bytesread], '\n')
	if idx == -1 {
		// No newline found
		if err != nil && err == io.EOF {
			// EOF w/o newline is okay - return current buffer
			return clone(s.buf[:bytesread]), nil
		} else if int64(bytesread) == s.blocksize {
			// No newline found in entire block
			return []byte{}, ErrLineExceedsBlocksize
		}
	}

	// Newline found at idx
	return clone(s.buf[:idx]), nil
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

// PrefixCompare compares the given byte slices (truncated to the length of the shorter)
// Used as the default compare function in NewSearcher.
func PrefixCompare(a, b []byte) int {
	switch {
	case len(a) < len(b):
		b = b[:len(a)]
	case len(b) < len(a):
		a = a[:len(b)]
	}
	return bytes.Compare(a, b)
}

// PrefixCompareString compares the given byte slices (truncated to the length of the shorter)
// after conversion to strings. Can be used as the compare function via NewSearcherOptions.
func PrefixCompareString(a, b []byte) int {
	switch {
	case len(a) < len(b):
		b = b[:len(a)]
	case len(b) < len(a):
		a = a[:len(b)]
	}
	sa := string(a)
	sb := string(b)
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