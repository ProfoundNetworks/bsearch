/*
Package bsearch provides binary search functionality for line-ordered byte streams
(e.g. sorted text files).
*/

package bsearch

import (
	"bytes"
	"errors"
	"io"
)

// TODO: how do we change the comparison function? e.g. BytesEqual, StringEqual, BytesLessEqual, StringLessEqual
// TODO: should we allow changing the blocksize?
// TODO: should we check for/warn on non-ordered data?

const (
	defaultBlockSize = 4096
)

var (
	ErrNotFound             = errors.New("not found")
	ErrLineExceedsBlockSize = errors.New("line length exceeds blocksize")
)

type Searcher struct {
	r   io.ReaderAt // data reader
	l   int64       // data length
	bsz int64       // data blocksize
	buf []byte      // data buffer (bsz+1)
}

func NewSearcher(r io.ReaderAt, length int64) *Searcher {
	s := Searcher{r: r, l: length, bsz: defaultBlockSize}
	s.buf = make([]byte, s.bsz+1) // we read bsz+1 bytes to check for a preceding newline
	return &s
}

// BlockPosition does a block-based binary search on its underlying reader,
// comparing only the first line of each block. It returns the byte offset
// of last block whose first line begins with a byte sequence less than b
// (using a bytewise comparison). The underlying data must therefore be
// bytewise-sorted (e.g. using a sort with `LC_COLLATE=C` set)))))))).
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
		// Read data from the midpoint between begin and end (truncated to a multiple of bsz)
		mid = ((end - begin) / 2) + begin
		mid = mid - (mid % s.bsz)

		//fmt.Fprintf(os.Stderr, "+ %s: begin %d, end %d, mid %d\n", string(b), begin, end, mid)

		// If mid == begin, check the next block up
		if mid == begin && end > mid+s.bsz {
			mid = mid + s.bsz
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
					if end > mid+s.bsz {
						mid = mid + s.bsz
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
			return 0, ErrLineExceedsBlockSize
		}

		//fmt.Fprintf(os.Stderr, "+ %s: comparing vs. %q\n", string(b), string(s.buf[cmpBegin:cmpEnd]))
		cmp := bytes.Compare(s.buf[cmpBegin:cmpEnd], b)

		// Check line against searchStr
		if cmp == -1 {
			begin = mid
		} else {
			end = mid
		}
	}

	return begin, nil
}

// LinePosition returns the byte offset within our reader of the first line
// that begins with the byte slice `b`, using a binary search (data must be
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
					blockPosition += s.bsz
					continue
				}
			}

			begin = idx + 1
		}

		// Scan lines from `begin` until we find one >= b
		for {
			// Compare buf from begin vs. b
			cmp := bytes.Compare(s.buf[begin:begin+len(b)], b)
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

// Line returns the first line within our reader that begins with the byte
// slice `b`, using a binary search (data must be bytewise-ordered).
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
		} else if int64(bytesread) == s.bsz {
			// No newline found in entire block
			return []byte{}, ErrLineExceedsBlockSize
		}
	}

	return clone(s.buf[:idx]), nil
}

func clone(b []byte) []byte {
	c := make([]byte, len(b))
	copy(c, b)
	return c
}
