package bsearch

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
)

// Test LoadIndex()
func TestLoadIndex(t *testing.T) {
	var tests = []struct {
		filepath string
		delim    string
		header   bool
		listlen  int
	}{
		{"testdata/domains2.csv", ",", true, 1},
	}

	for _, tc := range tests {
		idx, err := LoadIndex(tc.filepath)
		if err != nil {
			t.Fatalf("%s: %s\n", tc.filepath, err.Error())
		}
		assert.Equal(t, filepath.Base(tc.filepath), idx.Filename)
		assert.Equal(t, filepath.Dir(tc.filepath), filepath.Base(idx.Filedir))
		assert.Equal(t, tc.delim, string(idx.Delimiter))
		assert.Equal(t, tc.header, idx.Header)
		assert.Greater(t, idx.Epoch, int64(0))
		assert.Equal(t, tc.listlen, len(idx.List))
	}
}
