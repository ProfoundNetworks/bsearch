/*
Index provides an index implementation for bsearch.

The index file is a zstd-compressed yaml file. It has the same name and
location as the associated dataset, but with all '.' characters changed
to '_', and a '.bsx' suffix e.g. the index for `test_foobar.csv` is
`test_foobar_csv.bsx`.
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

	"github.com/DataDog/zstd"
	yaml "gopkg.in/yaml.v3"
)

const (
	indexVersion     = 2
	indexSuffix      = "bsx"
	defaultBlocksize = 4096
)

var (
	ErrIndexNotFound     = errors.New("index file not found")
	ErrIndexExpired      = errors.New("index file out of date")
	ErrIndexPathMismatch = errors.New("index file path mismatch")
)

type IndexOptions struct {
	Blocksize int64
	Delimiter []byte
	Header    bool
}

type IndexEntry struct {
	Key    string `yaml:"k"`
	Offset int64  `yaml:"o"`
	Length int64  `yaml:"l"`
}

// Index provides index metadata for the Filepath dataset
type Index struct {
	Blocksize      int64        `yaml:"blocksize"`
	Delimiter      []byte       `yaml:"delim"`
	Epoch          int64        `yaml:"epoch"`
	Filepath       string       `yaml:"filepath"`
	Header         bool         `yaml:"header"`
	KeysIndexFirst bool         `yaml:"keys_index_first"`
	KeysUnique     bool         `yaml:"keys_unique"`
	Length         int          `yaml:"length"`
	List           []IndexEntry `yaml:"list"`
	Version        int          `yaml:"version"`
}

// epoch returns the modtime for path in epoch/unix format
func epoch(path string) (int64, error) {
	stat, err := os.Stat(path)
	if err != nil {
		return 0, err
	}
	return stat.ModTime().Unix(), nil
}

// indexFile returns the index file associated with filename
func indexFile(filename string) string {
	reDot := regexp.MustCompile(`\.`)
	basename := reDot.ReplaceAllString(filename, "_")
	return basename + "." + indexSuffix
}

// IndexPath returns the filepath of the index assocated with path
func IndexPath(path string) (string, error) {
	var err error
	path, err = filepath.Abs(path)
	if err != nil {
		return "", err
	}
	dir, base := filepath.Split(path)
	return filepath.Join(dir, indexFile(base)), nil
}

// processBlock processes the block in buf[:bytesread] and returns an IndexEntry
// for the first line it finds.
func processBlock(reader io.ReaderAt, buf []byte, bytesread int,
	blockPosition int64, delim []byte, eof bool) (IndexEntry, int64, error) {
	var err error
	nlidx := -1

	// Find first newline
	if blockPosition > 0 {
		nlidx = bytes.IndexByte(buf[:bytesread], '\n')
		if nlidx == -1 {
			// If no newline exists in block, we can skip the block entirely (right?)
			blockPosition += int64(bytesread)
			bytesread, err = reader.ReadAt(buf, blockPosition)
			if err != nil && err != io.EOF {
				return IndexEntry{}, blockPosition, err
			}
			return processBlock(reader, buf, bytesread, blockPosition,
				delim, err == io.EOF)
		}
	}

	//d := delim
	didx := bytes.Index(buf[nlidx+1:bytesread], delim)
	if didx == -1 {
		// If no delimiter is found in block, assume we have a partial line,
		// and re-read from nlidx
		if nlidx == -1 {
			return IndexEntry{}, blockPosition, ErrKeyExceedsBlocksize
		}
		blockPosition += int64(nlidx)
		bytesread, err = reader.ReadAt(buf, blockPosition)
		if err != nil && err != io.EOF {
			return IndexEntry{}, blockPosition, err
		}
		return processBlock(reader, buf, bytesread, blockPosition,
			delim, err == io.EOF)
	}
	didx += nlidx + 1

	// Check that there's no newline in this chunk
	if nlidx2 := bytes.IndexByte(buf[nlidx+1:didx], '\n'); nlidx2 != -1 {
		return IndexEntry{}, blockPosition,
			fmt.Errorf("Error: line without delimiter found:\n%s\n",
				string(buf[nlidx+1:nlidx2]))
	}

	// Create entry
	entry := IndexEntry{}
	entry.Key = string(buf[nlidx+1 : didx])
	entry.Offset = blockPosition + int64(nlidx) + 1
	entry.Length = int64(bytesread - nlidx - 1)

	// On the first block only, check for the presence of a header
	if blockPosition == 0 {
		nlidx = bytes.IndexByte(buf[:bytesread], '\n')
		if nlidx == -1 {
			// Corner case - no newline found in block
			return IndexEntry{}, blockPosition, errors.New("Missing first block nlidx handling not yet implemented")
		}
		entry2, bp2, err := processBlock(reader, buf[nlidx:bytesread],
			bytesread-(nlidx), blockPosition+int64(nlidx), delim, eof)
		if err != nil {
			return IndexEntry{}, blockPosition + int64(nlidx), err
		}
		// If the entry.Key > entry2.Key, assume the first is a header
		if entry.Key > entry2.Key {
			return entry2, bp2, nil
		}
	}

	return entry, blockPosition, nil
}

// deriveDelimiter tries to guess an appropriate delimiter from filename
// It returns the delimiter on success, or an error on failure.
func deriveDelimiter(filename string) ([]byte, error) {
	reCSV := regexp.MustCompile(`\.csv(\.zst)?$`)
	rePSV := regexp.MustCompile(`\.psv(\.zst)?$`)
	reTSV := regexp.MustCompile(`\.tsv(\.zst)?$`)
	if reCSV.MatchString(filename) {
		return []byte{','}, nil
	}
	if rePSV.MatchString(filename) {
		return []byte{'|'}, nil
	}
	if reTSV.MatchString(filename) {
		return []byte{'\t'}, nil
	}
	return []byte{}, ErrUnknownDelimiter
}

func generateBlockIndex(index *Index, reader io.ReaderAt) error {
	// Process dataset in blocks
	buf := make([]byte, index.Blocksize)
	list := []IndexEntry{}
	var blockPosition int64 = 0
	firstBlock := true
	var entry, prev IndexEntry
	for {
		bytesread, err := reader.ReadAt(buf, blockPosition)
		if err != nil && err != io.EOF {
			return err
		}
		if bytesread > 0 {
			entry, blockPosition, err = processBlock(reader, buf, bytesread,
				blockPosition, index.Delimiter, err == io.EOF)
			if err != nil {
				return err
			}
			// Check that all entry keys are sorted as we expect
			if prev.Key <= entry.Key {
				if prev.Key == entry.Key && prev.Offset == entry.Offset {
					fmt.Fprintf(os.Stderr, "Warning: duplicate index entry found - skipping\n%v\n%v\n",
						prev, entry)
				} else {
					list = append(list, entry)
				}
			} else if prev.Key > entry.Key {
				return fmt.Errorf("Error: key sort violation - %q > %q\n",
					prev.Key, entry.Key)
			}
			// Set prev and blockPosition
			prev = entry
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

	// Reset all but the last entry lengths (this gives us blocks that
	// finish cleanly on newlines)
	for i := 0; i < len(list)-1; i++ {
		list[i].Length = list[i+1].Offset - list[i].Offset
	}

	index.List = list
	index.Length = len(list)

	return nil
}

// NewIndex creates a new Index for the path dataset
func NewIndex(path string) (*Index, error) {
	return NewIndexOptions(path, IndexOptions{})
}

// NewIndexOptions creates a new Index for path with delim as the delimiter
func NewIndexOptions(path string, opt IndexOptions) (*Index, error) {
	var err error
	path, err = filepath.Abs(path)
	if err != nil {
		return nil, err
	}
	reader, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	epoch, err := epoch(path)
	if err != nil {
		return nil, err
	}

	delim := opt.Delimiter
	if len(delim) == 0 {
		delim, err = deriveDelimiter(path)
		if err != nil {
			return nil, err
		}
	}

	index := Index{}
	if opt.Blocksize > 0 {
		index.Blocksize = opt.Blocksize
	} else {
		index.Blocksize = defaultBlocksize
	}
	index.Delimiter = delim
	index.Epoch = epoch
	index.Filepath = path
	// FIXME: do we honour index.Header if true??
	index.Header = opt.Header
	index.Version = indexVersion

	err = generateBlockIndex(&index, reader)
	if err != nil {
		return nil, err
	}

	return &index, nil
}

// LoadIndex loads Index from the associated index file for path.
// Returns ErrIndexNotFound if no index file exists.
// Returns ErrIndexExpired if path is newer than the index file.
// Returns ErrIndexPathMismatch if index filepath does not equal path.
func LoadIndex(path string) (*Index, error) {
	path, err := filepath.Abs(path)
	if err != nil {
		return nil, err
	}
	idxpath, err := IndexPath(path)
	if err != nil {
		return nil, err
	}

	_, err = os.Stat(idxpath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, ErrIndexNotFound
		} else {
			return nil, err
		}
	}

	var reader io.ReadCloser
	fh, err := os.Open(idxpath)
	if err != nil {
		return nil, err
	}
	defer fh.Close()
	reader = zstd.NewReader(fh)
	defer reader.Close()

	data, err := ioutil.ReadAll(reader)
	if err != nil {
		return nil, err
	}
	index := Index{List: []IndexEntry{}}
	yaml.Unmarshal(data, &index)

	// Check index.Filepath == path
	if index.Filepath != path {
		return nil, ErrIndexPathMismatch
	}

	// Check index.Epoch is still valid
	fe, err := epoch(path)
	if err != nil {
		return nil, err
	}
	if fe > index.Epoch {
		return nil, ErrIndexExpired
	}

	// Set index.Version to 1 if unset
	if index.Version == 0 {
		index.Version = 1
	}

	return &index, nil
}

// blockEntry does a binary search on the block entries in the index
// List and returns the last entry with a Key less than b, and its
// position in the List.
// FIXME: If no such entry exists, it returns the first entry.
// (This matches the old Searcher.BlockPosition semantics, which were
// conservative because the first block may include a header.)
func (i *Index) blockEntry(b []byte) (int, IndexEntry) {
	var begin, mid, end int
	list := i.List
	begin = 0
	end = len(list) - 1

	// Trim trailing delimiter
	if bytes.HasSuffix(b, i.Delimiter) {
		b = bytes.TrimSuffix(b, i.Delimiter)
	}

	for end-begin > 0 {
		mid = ((end - begin) / 2) + begin
		// If mid == begin, skip to next
		if mid == begin {
			mid++
		}
		//fmt.Fprintf(os.Stderr, "+ %s: begin %d, end %d, mid %d\n", string(b), begin, end, mid)

		cmp := prefixCompare([]byte(list[mid].Key), b)
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

	return begin, list[begin]
}

// blockEntryN returns the nth IndexEntry in index.List, and an ok flag,
// which is false if no entry is found.
func (i *Index) blockEntryN(n int) (IndexEntry, bool) {
	if n < 0 || n >= len(i.List) {
		return IndexEntry{}, false
	}
	return i.List[n], true
}

// Write writes the index to disk
func (i *Index) Write() error {
	data, err := yaml.Marshal(i)
	if err != nil {
		return err
	}

	filedir, filename := filepath.Split(i.Filepath)
	idxpath := filepath.Join(filedir, indexFile(filename))
	var writer io.WriteCloser
	fh, err := os.OpenFile(idxpath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		return err
	}
	writer = zstd.NewWriter(fh)
	defer fh.Close()

	_, err = writer.Write(data)
	if err != nil {
		return err
	}

	err = writer.Close()
	if err != nil {
		return err
	}

	return nil
}
