/*
Index provides an index implementation for bsearch.

The index file is a zstd-compressed yaml file. It has the same name and
location as the associated dataset, but with all '.' characters changed
to '_', and a '.bsx' suffix e.g. the index for `test_foobar.csv` is
`test_foobar_csv.bsx`.
*/

package bsearch

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"regexp"
	"strings"

	"github.com/DataDog/zstd"
	"github.com/rs/zerolog"
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
	ErrIndexEmpty        = errors.New("index contains no entries")
	ErrIndexPathMismatch = errors.New("index file path mismatch")
)

type IndexOptions struct {
	Blocksize int64
	Delimiter []byte
	Header    bool
	Logger    *zerolog.Logger // debug logger
}

type IndexEntry struct {
	Key    string `yaml:"k"`
	Offset int64  `yaml:"o"` // file offset for start-of-block
}

// Index provides index metadata for the Filepath dataset
type Index struct {
	Blocksize      int64           `yaml:"blocksize"`
	Delimiter      []byte          `yaml:"delim"`
	Epoch          int64           `yaml:"epoch"`
	Filepath       string          `yaml:"filepath"`
	Header         bool            `yaml:"header"`
	KeysIndexFirst bool            `yaml:"keys_index_first"`
	KeysUnique     bool            `yaml:"keys_unique"`
	Length         int             `yaml:"length"`
	List           []IndexEntry    `yaml:"list"`
	Version        int             `yaml:"version"`
	logger         *zerolog.Logger // debug logger
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

// generateLineIndex processes the input from reader line-by-line,
// generating index entries for the first full line in each block
// (or the first instance of that key, if repeating)
func generateLineIndex(index *Index, reader io.ReaderAt) error {
	// Process dataset line-by-line
	buf := make([]byte, index.Blocksize)
	scanner := bufio.NewScanner(reader.(io.Reader))
	scanner.Buffer(buf, int(index.Blocksize))
	list := []IndexEntry{}
	var blockPosition int64 = 0
	var blockNumber int64 = -1
	prevKey := []byte{}
	var firstOffset int64 = -1
	index.KeysUnique = true
	// If index.Header is set, skip the first line of the dataset,
	// begin indexing from the second
	skipHeader := index.Header
	for scanner.Scan() {
		line := scanner.Bytes()

		if skipHeader {
			skipHeader = false
			blockPosition += int64(len(line) + 1)
			continue
		}

		elt := bytes.SplitN(line, index.Delimiter, 2)
		key := elt[0]
		if index.logger != nil {
			index.logger.Debug().
				Int64("blockNumber", blockNumber).
				Int64("blockPosition", blockPosition).
				Bytes("prevKey", prevKey).
				Bytes("key", key).
				Msg("generateLineIndex loop")
		}

		// Check key ordering
		dupKeyBlock := false
		switch bytes.Compare(prevKey, key) {
		case 1:
			// Special case - allow second record out-of-order due to header
			// FIXME: should we have an option to disallow this?
			if blockNumber == 0 && !index.Header {
				index.Header = true
				// Reset list and blockNumber to restart
				list = []IndexEntry{}
				blockNumber = -1
			} else {
				// prevKey > key
				return fmt.Errorf("Error: key sort violation - %q > %q\n",
					prevKey, key)
			}
		case 0:
			// prevKey == key
			index.KeysUnique = false
			dupKeyBlock = true
		}

		// Add the first line of each block to our index
		currentBlockNumber := blockPosition / index.Blocksize
		if currentBlockNumber > blockNumber {
			offset := blockPosition
			if dupKeyBlock {
				offset = firstOffset
			}

			var last_offset int64 = -1
			if len(list) > 0 {
				last_offset = list[len(list)-1].Offset
			}
			if last_offset != offset {
				entry := IndexEntry{
					Key:    string(key),
					Offset: offset,
				}
				list = append(list, entry)
			}

			blockNumber = currentBlockNumber
		}

		if !dupKeyBlock {
			firstOffset = blockPosition
			prevKey = clonebs(key)
		}
		blockPosition += int64(len(line) + 1)
	}
	if err := scanner.Err(); err != nil {
		return err
	}
	if len(list) == 0 {
		return ErrIndexEmpty
	}

	index.KeysIndexFirst = true
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
	if opt.Logger != nil {
		index.logger = opt.Logger
	}

	err = generateLineIndex(&index, reader)
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

// blockEntryLE does a binary search on the block entries in the index
// List and returns the last entry with a Key less-than-or-equal-to key,
// and its position in the List.
// If no matching entry is found (i.e. the first index entry Key is
// greater than key), returns ErrNotFound.
func (i *Index) blockEntryLE(key []byte) (int, IndexEntry, error) {
	keystr := string(key)
	if i.List[0].Key > keystr { // index List cannot be empty
		return 0, IndexEntry{}, ErrNotFound
	}

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
		//fmt.Fprintf(os.Stderr, "+ %s: begin %d, end %d, mid %d\n",
		// string(b), begin, end, mid)

		cmp := strings.Compare(list[mid].Key, keystr)
		//fmt.Fprintf(os.Stderr, "+ %s: [%d] comparing vs. %q, cmp %d\n",
		// string(b), mid, list[mid].Key, cmp)
		if cmp <= 0 {
			begin = mid
		} else {
			if end == mid {
				break
			}
			end = mid
		}
	}

	return begin, list[begin], nil
}

// blockEntryLT does a binary search on the block entries in the index
// List and returns the last entry with a Key less-than key, and its
// position in the List.
// FIXME: If no such entry exists, it returns the first entry.
// (This matches the old Searcher.BlockPosition semantics, which were
// conservative because the first block may include a header.)
func (i *Index) blockEntryLT(key []byte) (int, IndexEntry) {
	var begin, mid, end int
	list := i.List
	begin = 0
	end = len(list) - 1

	/* FIXME: this is wrong now we're assuming key semantics, right?
	// Trim trailing delimiter
	if bytes.HasSuffix(key, i.Delimiter) {
		key = bytes.TrimSuffix(key, i.Delimiter)
	}
	*/

	for end-begin > 0 {
		mid = ((end - begin) / 2) + begin
		// If mid == begin, skip to next
		if mid == begin {
			mid++
		}
		//fmt.Fprintf(os.Stderr, "+ %s: begin %d, end %d, mid %d\n", string(b), begin, end, mid)

		cmp := prefixCompare([]byte(list[mid].Key), key)
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
// which is false if no Nth entry exists.
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
