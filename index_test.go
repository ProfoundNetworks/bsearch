package bsearch

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

// ensureNoIndex removes any existing index, when we don't want to load
func ensureNoIndex(t *testing.T, filename string) {
	t.Helper()
	idxpath, err := IndexPath(filepath.Join("testdata", filename))
	if err != nil {
		t.Fatalf("ensureNoIndex %s: %s\n", filename, err.Error())
	}
	_, err = os.Stat(idxpath)
	if err != nil && !os.IsNotExist(err) {
		t.Fatalf("ensureNoIndex %s: %s\n", filename, err.Error())
	}
	if err == nil {
		// idxpath exists!
		err = os.Remove(idxpath)
		if err != nil && !os.IsNotExist(err) {
			t.Fatalf("removing index %s: %s\n", idxpath, err.Error())
		}
	}
}

// ensureIndex checks that an appropriate index exists for filename and
// creates it if not
func ensureIndex(t *testing.T, filename string) {
	t.Helper()
	path := filepath.Join("testdata", filename)
	idxpath, err := IndexPath(path)
	if err != nil {
		t.Fatalf("ensureIndex %s: %s\n", filename, err.Error())
	}
	stat, err := os.Stat(idxpath)
	if err != nil && !os.IsNotExist(err) {
		t.Fatalf("ensureIndex %s: %s\n", filename, err.Error())
	}
	if err != nil && os.IsNotExist(err) {
		idx, err := NewIndex(path)
		if err != nil {
			t.Fatalf("ensureIndex NewIndex %s: %s\n", filename, err.Error())
		}
		err = idx.Write()
		if err != nil {
			t.Fatalf("ensureIndex index Write %s: %s\n", filename, err.Error())
		}
		return
	}
	// Index exists - check it is newer than file or recreate
	fe, err := epoch(path)
	if err != nil {
		t.Fatalf("ensureIndex epoch %s: %s", filename, err.Error())
	}
	ie := stat.ModTime().Unix()
	if fe > ie {
		idx, err := NewIndex(path)
		if err != nil {
			t.Fatalf("ensureIndex NewIndex on %q: %s\n", path, err.Error())
		}
		err = idx.Write()
		if err != nil {
			t.Fatalf("ensureIndex index Write for %q: %s\n", path, err.Error())
		}
	}
}

func TestIndexPath(t *testing.T) {
	var tests = []struct {
		filepath  string
		indexpath string
	}{
		{"bar.csv", "bar_csv.bsy"},
		{"/a/b/c/bar.csv", "/a/b/c/bar_csv.bsy"},
	}
	for _, tc := range tests {
		ipath, err := IndexPath(tc.filepath)
		if err != nil {
			t.Fatal(err)
		}
		if !filepath.IsAbs(ipath) {
			t.Errorf("IndexPath for %q is not absolute: %s",
				tc.filepath, ipath)
		}
		if filepath.IsAbs(tc.indexpath) {
			if ipath != tc.indexpath {
				t.Errorf("IndexPath for %q is %q, want %q",
					tc.filepath, ipath, tc.indexpath)
			}
		} else {
			if !strings.HasSuffix(ipath, "/"+filepath.Base(tc.indexpath)) {
				t.Errorf("IndexPath for %q is %q - does not have expected base %q",
					tc.filepath, ipath, filepath.Base(tc.indexpath))
			}
		}
	}
}

// Test LoadIndex on v4 index files
func TestLoadIndexV4(t *testing.T) {
	var tests = []struct {
		filename     string
		delim        string
		header       bool
		listlen      int
		headerFields []string
	}{
		{"foo.csv", ",", true, 2, []string{"label", "lineno"}},
	}

	for _, tc := range tests {
		ensureIndex(t, tc.filename)

		idx, err := LoadIndex(filepath.Join("testdata", tc.filename))
		if err != nil {
			t.Fatalf("%s: %s\n", tc.filename, err.Error())
		}

		assert.Equal(t, tc.filename, idx.Filename, tc.filename+" filename")
		assert.Equal(t, tc.delim, string(idx.Delimiter), tc.filename+" delimiter")
		assert.Equal(t, tc.header, idx.Header, tc.filename+" header")
		assert.Greater(t, idx.Epoch, int64(0), tc.filename+" epoch")
		assert.Equal(t, tc.listlen, len(idx.List), tc.filename+" listlen")
		assert.Equal(t, tc.headerFields, idx.HeaderFields, tc.filename+" headerFields")
	}
}

// Test LoadIndex on v3 index files
func TestLoadIndexV3(t *testing.T) {
	var tests = []struct {
		filename string
		delim    string
		header   bool
		listlen  int
	}{
		{"domains2.csv", ",", true, 1},
		{"foo3.csv", ",", true, 2},
	}

	for _, tc := range tests {
		ensureIndex(t, tc.filename)

		idx, err := LoadIndex(filepath.Join("testdata", tc.filename))
		if err != nil {
			t.Fatalf("%s: %s\n", tc.filename, err.Error())
		}

		assert.Equal(t, tc.filename, idx.Filename, tc.filename+" filename")
		assert.Equal(t, tc.delim, string(idx.Delimiter), tc.filename+" delimiter")
		assert.Equal(t, tc.header, idx.Header, tc.filename+" header")
		assert.Greater(t, idx.Epoch, int64(0), tc.filename+" epoch")
		assert.Equal(t, tc.listlen, len(idx.List), tc.filename+" listlen")
	}
}

// Test NewIndex()
func TestNewIndex(t *testing.T) {
	var tests = []struct {
		filename string
		delim    string
		header   bool
		listlen  int
	}{
		{"indexme.csv", ",", false, 1},
	}

	for _, tc := range tests {
		ensureNoIndex(t, tc.filename)

		// NewIndex()
		idx, err := NewIndex(filepath.Join("testdata", tc.filename))
		if err != nil {
			t.Fatalf("%s: %s\n", tc.filename, err.Error())
		}
		assert.Equal(t, tc.filename, idx.Filename, tc.filename+" filename")
		assert.Equal(t, tc.delim, string(idx.Delimiter), tc.filename+" delimiter")
		assert.Equal(t, tc.header, idx.Header, tc.filename+" header")
		assert.Greater(t, idx.Epoch, int64(0), tc.filename+" epoch")
		assert.Equal(t, tc.listlen, len(idx.List), tc.filename+" listlen")
	}
}

// Test NewIndexOptions()
func TestNewIndexOptions(t *testing.T) {
	var tests = []struct {
		filename     string
		delim        string
		header       bool
		listlen      int
		headerFields []string
	}{
		{"indexme.csv", ",", false, 1, []string(nil)},
		{"foo2.csv", ",", true, 2, []string{"label1", "label2, with comma", "lineno"}},
	}

	for _, tc := range tests {
		ensureNoIndex(t, tc.filename)

		o := IndexOptions{Delimiter: []byte(tc.delim), Header: tc.header}
		idx, err := NewIndexOptions(filepath.Join("testdata", tc.filename), o)
		if err != nil {
			t.Fatalf("%s: %s\n", tc.filename, err.Error())
		}
		assert.Equal(t, tc.filename, idx.Filename, tc.filename+" filename")
		assert.Equal(t, tc.delim, string(idx.Delimiter), tc.filename+" delimiter")
		assert.Equal(t, tc.header, idx.Header, tc.filename+" header")
		assert.Greater(t, idx.Epoch, int64(0), tc.filename+" epoch")
		assert.Equal(t, tc.listlen, len(idx.List), tc.filename+" listlen")
		assert.Equal(t, tc.headerFields, idx.HeaderFields, tc.filename+" headerFields")
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
		{"002.055.255.255", "001.024.000.000", 2079},
		{"002.056.000.000", "002.056.000.000", 4113},
		{"002.057.000.000", "002.056.171.000", 6163},
		{"002.057.084.000", "002.057.084.000", 8213},
		{"223.130.000.000", "223.130.000.000", 6504496},
		{"255.255.255.255", "223.223.000.000", 6506532},
		// Error case - should return ErrIndexEntryNotFound
		{"000.000.000.000", "", -1},
	}

	dataset := "rir_clc_ipv_range.csv"
	ensureIndex(t, dataset)
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
