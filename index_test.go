package bsearch

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
)

// ensureNoIndex removes any existing index, when we don't want to load
func ensureNoIndex(t *testing.T, filename string) {
	idxpath, err := IndexPath(filepath.Join("testdata", filename))
	if err != nil {
		t.Fatalf("%s: %s\n", filename, err.Error())
	}
	_, err = os.Stat(idxpath)
	if err != nil && !os.IsNotExist(err) {
		t.Fatalf("%s: %s\n", filename, err.Error())
	}
	if err == nil {
		// idxpath exists!
		err = os.Remove(idxpath)
		if err != nil && !os.IsNotExist(err) {
			t.Fatalf("removing index %s: %s\n", idxpath, err.Error())
		}
	}
}

// Test LoadIndex()
func TestIndexLoad(t *testing.T) {
	var tests = []struct {
		filename string
		delim    string
		header   bool
		listlen  int
	}{
		{"domains2.csv", ",", true, 1},
		{"foo.csv", ",", false, 22},
	}

	for _, tc := range tests {
		idx, err := LoadIndex(filepath.Join("testdata", tc.filename))
		if err != nil {
			t.Fatalf("%s: %s\n", tc.filename, err.Error())
		}
		filedir, filename := filepath.Split(idx.Filepath)
		assert.Equal(t, tc.filename, filename, tc.filename+" filename")
		assert.Equal(t, "testdata", filepath.Base(filedir), tc.filename+" filedir")
		assert.Equal(t, tc.delim, string(idx.Delimiter), tc.filename+" delimiter")
		assert.Equal(t, tc.header, idx.Header, tc.filename+" header")
		assert.Greater(t, idx.Epoch, int64(0), tc.filename+" epoch")
		assert.Equal(t, tc.listlen, len(idx.List), tc.filename+" listlen")
	}
}

// Test NewIndex()
func TestIndexNew(t *testing.T) {
	var tests = []struct {
		filename string
		delim    string
		header   bool
		listlen  int
	}{
		{"indexme.csv", ",", false, 22},
	}

	for _, tc := range tests {
		ensureNoIndex(t, tc.filename)

		// NewIndex()
		idx, err := NewIndex(filepath.Join("testdata", tc.filename))
		if err != nil {
			t.Fatalf("%s: %s\n", tc.filename, err.Error())
		}
		filedir, filename := filepath.Split(idx.Filepath)
		assert.Equal(t, tc.filename, filename, tc.filename+" filename")
		assert.Equal(t, "testdata", filepath.Base(filedir), tc.filename+" filedir")
		assert.Equal(t, tc.delim, string(idx.Delimiter), tc.filename+" delimiter")
		assert.Equal(t, tc.header, idx.Header, tc.filename+" header")
		assert.Greater(t, idx.Epoch, int64(0), tc.filename+" epoch")
		assert.Equal(t, tc.listlen, len(idx.List), tc.filename+" listlen")
	}
}

// Test NewIndexOptions()
func TestIndexNewDelimiter(t *testing.T) {
	var tests = []struct {
		filename string
		delim    string
		header   bool
		listlen  int
	}{
		{"indexme.csv", ",", false, 22},
	}

	for _, tc := range tests {
		ensureNoIndex(t, tc.filename)

		o := IndexOptions{Delimiter: []byte(tc.delim)}
		idx, err := NewIndexOptions(filepath.Join("testdata", tc.filename), o)
		if err != nil {
			t.Fatalf("%s: %s\n", tc.filename, err.Error())
		}
		filedir, filename := filepath.Split(idx.Filepath)
		assert.Equal(t, tc.filename, filename, tc.filename+" filename")
		assert.Equal(t, "testdata", filepath.Base(filedir), tc.filename+" filedir")
		assert.Equal(t, tc.delim, string(idx.Delimiter), tc.filename+" delimiter")
		assert.Equal(t, tc.header, idx.Header, tc.filename+" header")
		assert.Greater(t, idx.Epoch, int64(0), tc.filename+" epoch")
		assert.Equal(t, tc.listlen, len(idx.List), tc.filename+" listlen")
	}
}
