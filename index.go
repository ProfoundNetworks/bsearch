/*
Index provides an optional bsearch index implementation.
*/

package bsearch

import (
	"errors"
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
	Header    bool         `yaml:"header"`
	List      []IndexEntry `yaml:"list"`
}

func epoch(filename string) (int64, error) {
	stat, err := os.Stat(filename)
	if err != nil {
		return 0, err
	}
	return stat.ModTime().Unix(), nil
}

// NewIndex creates a new Index for filename from scratch.
//func NewIndex(filename string) (*Index, error) {
//}

// NewIndexLoad loads Index from the associated index file for filename.
// Returns ErrNotFound if no index file exists.
// Returns ErrIndexExpired if filename is newer than the index file.
func NewIndexLoad(filename string) (*Index, error) {
	reNonSuffix := regexp.MustCompile(`^[^.]+`)
	matches := reNonSuffix.FindStringSubmatch(filepath.Base(filename))
	idxfile := matches[0] + ".bsx"
	idxpath := filepath.Join(filepath.Dir(filename), idxfile)

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
