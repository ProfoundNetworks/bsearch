/*
Package bsearch provides binary search functionality for line-ordered byte streams
by prefix (e.g. for searching sorted text files).
*/

package bsearch

import (
	"bytes"
	"errors"
	"io"
)

// TODO: how do we change the comparison function? e.g. BytesEqual, StringEqual, BytesLessEqual, StringLessEqual
// TODO: should we check for/warn on non-ordered data?

const (
	defaultBlocksize = 4096
)

var (
	ErrNotFound             = errors.New("not found")
	ErrLineExceedsBlocksize = errors.New("line length exceeds blocksize")
)

type Options struct {
	blocksize int64                 // data blocksize used for binary search
	compare   func(a, b []byte) int // prefix comparison function
}

// Searcher provides binary search functionality for line-ordered byte streams by prefix.
type Searcher struct {
	r         io.ReaderAt           // data reader
	l         int64                 // data length
	blocksize int64                 // data blocksize used for binary search
	buf       []byte                // data buffer (blocksize+1)
	compare   func(a, b []byte) int // prefix comparison function
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
	if options.blocksize > 0 {
		s.blocksize = options.blocksize
	}
	if options.compare != nil {
		s.compare = options.compare
	}
	s.buf = make([]byte, s.blocksize+1) // we read blocksize+1 bytes to check for a preceding newline
	return &s
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
// Returns -1 and bsearch.ErrNotFound if no matching line is found, and -1
// and the error on any other error.
func (s *Searcher) LinePosition(b []byte) (int64, error) {
	// Get BlockPosition to start from
	blockPosition, err := s.BlockPosition(b)
	if err != nil {
		return -1, err
	}
	//fmt.Fprintf(os.Stderr, "+ %s: blockPosition %d\n", string(b), blockPosition)

	// Start one byte back in case we have a block-end newline
	if blockPosition > 0 {
		blockPosition--
	}

	// Loop in case we have to read more than one block
BLOCK:
	for {
		// Read next block
		bytesread, err := s.r.ReadAt(s.buf, blockPosition)
		if err != nil && err != io.EOF {
			return -1, err
		}

		// Skip till first newline
		begin := 0
		if blockPosition > 0 {
			idx := bytes.IndexByte(s.buf[:bytesread], '\n')
			if idx == -1 {
				// If no new newline is found we're either at EOF, or we have a block with no newlines at all(?!)
				if err != nil && err == io.EOF {
					return -1, ErrNotFound
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
			//fmt.Fprintf(os.Stderr, "+ comparing %q vs. %q == %d\n",
			//	string(s.buf[begin:begin+len(b)]), string(b), cmp)
			if cmp == 0 {
				//fmt.Fprintf(os.Stderr, "+ found %q: LinePosition %d\n", string(b), blockPosition+int64(begin))
				return blockPosition + int64(begin), nil
			} else if cmp == 1 {
				break BLOCK
			}

			// Current line < b; find next newline
			idx := bytes.IndexByte(s.buf[begin:bytesread], '\n')
			if idx == -1 {
				// No newline found - re-read from current position
				blockPosition += int64(begin)
				//fmt.Fprintf(os.Stderr, "+ no newline re-read - begin=%d, blockPosition => %d\n",
				//	begin, blockPosition)
				continue BLOCK
			}

			begin += idx + 1
			//fmt.Fprintf(os.Stderr, "+ begin incremented by %d to %d\n", idx+1, begin)

			// Out of data - re-read from current newline
			if begin+len(b) > bytesread {
				blockPosition += int64(begin) - 1
				//fmt.Fprintf(os.Stderr, "+ out-of-data re-read - begin=%d, blockPosition => %d\n",
				//	begin-1, blockPosition)
				continue BLOCK
			}
		}
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
			// EOF w/o a newline is okay - return current buffer
			return clone(s.buf[:bytesread]), nil
		} else if int64(bytesread) == s.blocksize {
			// No newline found in entire block
			return []byte{}, ErrLineExceedsBlocksize
		}
	}

	return clone(s.buf[:idx]), nil
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
