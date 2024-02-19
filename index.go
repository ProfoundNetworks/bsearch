/*
Index provides an index implementation for bsearch.

The index file is an uncompressed json+tsv file. It has the same name and
location as the associated dataset, but with all '.' characters changed
to '_', and a '.bsy' suffix e.g. the index for `test_foobar.csv` is
`test_foobar_csv.bsy`.
*/

package bsearch

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"

	"github.com/rs/zerolog"
)

const (
	indexVersion     = 4
	indexSuffix      = "bsy"
	defaultBlocksize = 2048
	recordSeparator  = '\n'
	fieldSeparator   = '\t'
)

var (
	ErrIndexNotFound     = errors.New("index file not found")
	ErrIndexExpired      = errors.New("index file out of date")
	ErrIndexEmpty        = errors.New("index contains no entries")
	ErrIndexPathMismatch = errors.New("index file path mismatch")
)

type IndexOptions struct {
	Blocksize int
	Delimiter []byte
	Header    bool
	Logger    *zerolog.Logger // debug logger
}

type IndexEntry struct {
	Key    string
	Offset int64 // file offset for start-of-block
}

// Index provides index metadata for the filepath dataset
type Index struct {
	Blocksize int
	// FIXME: Delimiter should really be a rune, not an arbitrarily-length []byte
	// Can we change without bumping the index version?
	Delimiter []byte
	Epoch     int64
	// Filepath is no longer exported (it is explicitly emptied in Write()),
	// but we keep it capitalised to accept old indices that used it
	// instead of Filename
	Filepath       string `json:",omitempty"`
	Filename       string
	Header         bool
	KeysIndexFirst bool
	KeysUnique     bool
	Length         int
	List           []IndexEntry `json:"-"`
	Version        int
	HeaderFields   []string        `json:",omitempty"`
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

// IndexPath returns the absolute filepath of the index file assocated with path
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
	//
	// buf: A buffer large enough to hold an entire block
	// scanner:
	// list: Our resulting list of IndexEntry objects
	// blockPosition: The offset of the current block from the beginning of the file
	// blockNumber: The ordinal number of the current block
	// prevKey:
	// firstOffset:
	// skipHeader: Set to true if the file contains a header that should be skipped
	//
	// Process dataset line-by-line
	buf := make([]byte, index.Blocksize)
	scanner := bufio.NewScanner(reader.(io.Reader))
	scanner.Buffer(buf, index.Blocksize)
	list := []IndexEntry{}
	var blockPosition int64 = 0
	var blockNumber int64 = -1
	prevKey := []byte{}
	prevLine := []byte{}
	var firstOffset int64 = -1
	index.KeysUnique = true
	skipHeader := index.Header
	for scanner.Scan() {
		line := scanner.Bytes()

		if skipHeader {
			// If index.Header is set, skip the first line of the dataset,
			// begin indexing from the second
			skipHeader = false
			blockPosition += int64(len(line) + 1)
			index.HeaderFields = strings.Split(string(line),
				string(index.Delimiter))
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
				index.HeaderFields = strings.Split(string(prevLine),
					string(index.Delimiter))
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
		currentBlockNumber := blockPosition / int64(index.Blocksize)
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
		if blockNumber == 0 {
			prevLine = clonebs(line)
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
	index.Filename = filepath.Base(path)
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

	fh, err := os.Open(idxpath)
	if err != nil {
		return nil, err
	}
	defer fh.Close()

	reader := bufio.NewReader(fh)

	firstLine, err := reader.ReadBytes('\n')
	if err != nil {
		return nil, err
	}
	var index Index
	err = json.Unmarshal(firstLine, &index)
	if err != nil {
		return nil, err
	}
	// New indices set Filename, and we derive Filepath
	if index.Filename != "" {
		index.Filepath = filepath.Join(filepath.Dir(path), index.Filename)
	} else if index.Filepath != "" {
		// Whereas old indices used Filepath instead, so derive Filename
		index.Filename = filepath.Base(index.Filepath)
	}

	// Check that the file paths match to ensure that the index we loaded
	// actually belongs with the file stored at path, since otherwise the
	// search results will be junk.
	if (index.Version >= 4 && index.Filepath != path) ||
		(index.Version == 3 && filepath.Base(index.Filepath) != index.Filename) {
		fmt.Fprintf(os.Stderr, "ErrIndexPathMismatch: path %q, index.Filepath %q",
			path, index.Filepath)
		return nil, ErrIndexPathMismatch
	}

	fe, err := epoch(path)
	if err != nil {
		return nil, err
	}
	ie, err := epoch(idxpath)
	if err != nil {
		return nil, err
	}
	if fe > ie {
		return nil, ErrIndexExpired
	}

	if index.Version == 0 {
		index.Version = 1
	}

	for counter := 0; counter < index.Length; counter++ {
		line, err := reader.ReadString(recordSeparator)
		lineNum := counter + 1
		if err == io.EOF {
			return nil, fmt.Errorf("malformed index: premature EOF on line %d", lineNum)
		}
		line = strings.TrimRight(line, string(recordSeparator))
		pair := strings.SplitN(line, string(fieldSeparator), 2)
		if len(pair) != 2 {
			return nil, fmt.Errorf("malformed index: line %d (%q) contains a malformed pair", lineNum, line)
		}

		offset, err := strconv.ParseInt(pair[0], 10, 64)
		if err != nil {
			return nil, fmt.Errorf("malformed index: line %d contains a bad offset: %w", lineNum, err)
		}
		key, err := strconv.Unquote(pair[1])
		if err != nil {
			return nil, fmt.Errorf("malformed index: line %d contains a bad key: %w", lineNum, err)
		}
		index.List = append(index.List, IndexEntry{Key: key, Offset: offset})
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
	filedir := filepath.Dir(i.Filepath)
	idxpath := filepath.Join(filedir, indexFile(i.Filename))

	fh, err := os.OpenFile(idxpath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		return err
	}

	abort := func() { os.Remove(idxpath) }

	// Reset Filepath, since it's not required for reads
	i.Filepath = ""

	data, err := json.Marshal(i)
	if err != nil {
		return err
	}

	writer := bufio.NewWriter(fh)
	_, err = writer.Write(data)
	if err != nil {
		abort()
		return err
	}

	err = writer.WriteByte(recordSeparator)
	if err != nil {
		abort()
		return err
	}

	for _, entry := range i.List {
		record := fmt.Sprintf(
			"%d%c%s%c",
			entry.Offset,
			fieldSeparator,
			strconv.Quote(entry.Key),
			recordSeparator,
		)
		_, err = writer.WriteString(record)
		if err != nil {
			abort()
			return err
		}
	}

	writer.Flush()
	err = fh.Close()
	if err != nil {
		abort()
		return err
	}

	return nil
}
