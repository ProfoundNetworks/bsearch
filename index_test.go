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

// Test blockEntryLE() on rir_clc_ipv_range.csv
func TestIndexBlockEntryLE(t *testing.T) {
	var tests = []struct {
		key         string
		entryKey    string
		entryOffset int64
	}{
		{"000.001.000.000", "000.001.000.000", 0},
		{"001.001.000.000", "000.001.000.000", 0},
		{"002.055.255.255", "000.001.000.000", 0},
		{"002.056.000.000", "002.056.000.000", 4113},
		{"002.057.000.000", "002.056.000.000", 4113},
		{"002.057.084.000", "002.057.084.000", 8213},
		{"223.130.000.000", "223.130.000.000", 6504496},
		{"255.255.255.255", "223.130.000.000", 6504496},
		// Error case - should return ErrIndexEntryNotFound
		{"000.000.000.000", "", -1},
	}

	dataset := "rir_clc_ipv_range.csv"
	idx, err := LoadIndex(filepath.Join("testdata", dataset))
	if err != nil {
		t.Fatalf("%s: %s\n", dataset, err.Error())
	}
	assert.Equal(t, true, idx.KeysIndexFirst, dataset+" KeysIndexFirst")
	assert.Equal(t, true, idx.KeysUnique, dataset+" KeysUnique")

	for _, tc := range tests {
		_, entry, err := idx.blockEntryLE([]byte(tc.key))
		if tc.entryKey == "" {
			assert.Equal(t, err, ErrNotFound,
				tc.key+" returns ErrNotFound")
			continue
		}
		assert.Equal(t, tc.entryKey, entry.Key, tc.key+" entryKey")
		assert.Equal(t, tc.entryOffset, entry.Offset, tc.key+" entryOffset")
	}
}
