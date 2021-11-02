// +build skip

/*
bsearch compression-related functions
*/

package bsearch

import (
	"bytes"
	"fmt"
	"io"
	"regexp"

	"github.com/DataDog/zstd"
)

var (
	reCompressedSupported = regexp.MustCompile(`\.zst$`)
)

// isCompressed returns true if there is an underlying file that is compressed
func (s *Searcher) isCompressed() bool {
	if s.filepath != "" {
		if reCompressedSupported.MatchString(s.filepath) {
			return true
		}
		return false
	}
	if reCompressedSupported.MatchString(s.Index.Filepath) {
		return true
	}
	return false
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
	// Find the offset of the first line in buf beginning with key
	offset, terminate := s.scanLineOffset(buf, key)
	if offset == -1 || terminate {
		return [][]byte{}, terminate
	}
	/*
		if s.logger != nil {
			s.logger.Debug().
				Bytes("key", key).
				Int("offset", offset).
				Msg("scanLinesMatching line1")
		}
	*/

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
