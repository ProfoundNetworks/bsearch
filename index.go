/*
Index provides an optional bsearch index implementation.
*/

package bsearch

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"regexp"

	yaml "gopkg.in/yaml.v3"
)

const (
	indexSuffix = "bsx"
)

var (
	ErrIndexExpired = errors.New("Index is out of date")
)

type IndexEntry struct {
	Key    string `yaml:"k"`
	Offset int64  `yaml:"o"`
}

type Index struct {
	Delimiter string       `yaml:"delim"`
	Epoch     int64        `yaml:"epoch"`
	Filename  string       `yaml:"filename"`
	Filedir   string       `yaml:"filedir"`
	Header    bool         `yaml:"header"`
	List      []IndexEntry `yaml:"list"`
}

// epoch returns the modtime for filename in epoch/unix format
func epoch(filename string) (int64, error) {
	stat, err := os.Stat(filename)
	if err != nil {
		return 0, err
	}
	return stat.ModTime().Unix(), nil
}

// IndexFile returns the basename of the index file associated with filename
func IndexFile(filename string) string {
	reNonSuffix := regexp.MustCompile(`^[^.]+`)
	matches := reNonSuffix.FindStringSubmatch(filepath.Base(filename))
	idxfile := matches[0] + "." + indexSuffix
	return idxfile
}

// processBlock processes the block in buf[:bytesread] and returns an IndexEntry
// for the first line it finds.
func processBlock(reader io.ReaderAt, buf []byte, bytesread int, blockPosition int64, delim string) (IndexEntry, error) {
	var err error
	nlidx := -1

	// Find first newline
	if blockPosition > 0 {
		nlidx = bytes.IndexByte(buf[:bytesread], '\n')
		if nlidx == -1 {
			// If no newline exists in block, we can skip the block entirely
			blockPosition += int64(bytesread)
			bytesread, err = reader.ReadAt(buf, blockPosition)
			if err != nil {
				return IndexEntry{}, err
			}
			return processBlock(reader, buf, bytesread, blockPosition, delim)
		}
	}

	// Find delimiter
	didx := bytes.IndexByte(buf[nlidx+1:bytesread], byte(delim[0]))
	if didx == -1 {
		// If no delimiter is found in block, re-read from nlidx
		if nlidx == -1 {
			return IndexEntry{}, ErrKeyExceedsBlocksize
		}
		blockPosition += int64(nlidx)
		bytesread, err = reader.ReadAt(buf, blockPosition)
		if err != nil {
			return IndexEntry{}, err
		}
		return processBlock(reader, buf, bytesread, blockPosition, delim)
	}
	didx += nlidx + 1

	// Check that there's no newline in this chunk
	if nlidx2 := bytes.IndexByte(buf[nlidx+1:didx], '\n'); nlidx2 != -1 {
		return IndexEntry{}, fmt.Errorf("Error: line without delimiter found:\n%s\n", string(buf[nlidx+1:nlidx2]))
	}

	// Create entry
	entry := IndexEntry{}
	entry.Key = string(buf[nlidx+1 : didx])
	entry.Offset = blockPosition + int64(nlidx) + 1

	// On the first block only, check for the presence of a header
	if blockPosition == 0 {
		nlidx = bytes.IndexByte(buf[:bytesread], '\n')
		if nlidx == -1 {
			// Corner case - no newline found in block
			return IndexEntry{}, errors.New("Missing first block nlidx handling not yet implemented")
		}
		entry2, err := processBlock(reader, buf[nlidx:bytesread], bytesread-(nlidx), blockPosition+int64(nlidx), delim)
		if err != nil {
			return IndexEntry{}, err
		}
		// If the entry.Key > entry2.Key, assume the first is a header
		if entry.Key > entry2.Key {
			return entry2, nil
		}
	}

	return entry, nil
}

// NewIndex creates a new Index for the filename dataset from scratch
func NewIndex(filename string) (*Index, error) {
	reader, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	epoch, err := epoch(filename)
	if err != nil {
		return nil, err
	}
	filedir, err := filepath.Abs(filepath.Dir(filename))
	if err != nil {
		return nil, err
	}

	index := Index{}
	// index.Collation = "C"
	index.Delimiter = ","
	index.Epoch = epoch
	index.Filename = filepath.Base(filename)
	index.Filedir = filedir
	index.Header = false

	// Process dataset in blocks
	buf := make([]byte, defaultBlocksize)
	list := []IndexEntry{}
	var blockPosition int64 = 0
	firstBlock := true
	prevKey := ""
	var entry IndexEntry
	for {
		bytesread, err := reader.ReadAt(buf, blockPosition)
		if err != nil && err != io.EOF {
			return nil, err
		}
		if bytesread > 0 {
			entry, err = processBlock(reader, buf, bytesread, blockPosition, index.Delimiter)
			if err != nil {
				return nil, err
			}
			// Check that all entry keys are sorted as we expect
			if prevKey < entry.Key {
				list = append(list, entry)
			} else if prevKey > entry.Key {
				return nil, fmt.Errorf("Error: key sort violation - %q > %q\n", prevKey, entry.Key)
			}
			// Set prevKey and blockPosition
			prevKey = entry.Key
			blockPosition += int64(bytesread)
			// If the first offset is not zero we've skipped a header
			if firstBlock && entry.Offset > 0 {
				index.Header = true
			}
		}
		if err != nil && err == io.EOF {
			break
		}
		firstBlock = false
	}

	// Output
	index.List = list
	return &index, nil
}

// NewIndexLoad loads Index from the associated index file for filename.
// Returns ErrNotFound if no index file exists.
// Returns ErrIndexExpired if filename is newer than the index file.
func NewIndexLoad(filename string) (*Index, error) {
	idxpath := filepath.Join(filepath.Dir(filename), IndexFile(filename))

	_, err := os.Stat(idxpath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, ErrNotFound
		} else {
			return nil, err
		}
	}

	data, err := ioutil.ReadFile(idxpath)
	if err != nil {
		return nil, err
	}
	index := Index{List: []IndexEntry{}}
	yaml.Unmarshal(data, &index)

	// Check index.Epoch is still valid
	fe, err := epoch(filename)
	if err != nil {
		return nil, err
	}
	if fe > index.Epoch {
		return nil, ErrIndexExpired
	}

	return &index, nil
}

// BlockPosition does a binary search on the block entries in the index List
// and returns the offset of the last entry with a Key less than b. If no such
// entry exists, it returns the offset of the first entry. (This matches the
// main Searcher.BlockPosition semantics, which are conservative because the
// first block may include a header.)
func (i *Index) BlockPosition(b []byte) (int64, error) {
	var begin, mid, end int
	list := i.List
	begin = 0
	end = len(list) - 1

	for end-begin > 0 {
		mid = ((end - begin) / 2) + begin
		// If mid == begin, skip to next
		if mid == begin {
			mid++
		}
		//fmt.Fprintf(os.Stderr, "+ %s: begin %d, end %d, mid %d\n", string(b), begin, end, mid)

		cmp := PrefixCompare([]byte(list[mid].Key), b)
		//fmt.Fprintf(os.Stderr, "+ %s: [%d] comparing vs. %q, cmp %d\n", string(b), mid, list[mid].Key, cmp)
		if cmp == -1 {
			begin = mid
		} else {
			if end == mid {
				break
			}
			end = mid
		}
	}

	return list[begin].Offset, nil
}

// Write writes the index to disk
func (i *Index) Write() error {
	data, err := yaml.Marshal(i)
	if err != nil {
		return err
	}
	idxpath := filepath.Join(i.Filedir, IndexFile(i.Filename))
	err = ioutil.WriteFile(idxpath, data, 0644)
	if err != nil {
		return err
	}
	return nil
}
