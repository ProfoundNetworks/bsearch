package main

import (
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/ProfoundNetworks/bsearch"
	"github.com/stretchr/testify/assert"
)

// Test the index generated for testdata/foo.csv
func TestIndexFoo(t *testing.T) {
	index, err := bsearch.NewIndex("testdata/foo.csv")
	if err != nil {
		t.Fatal(err)
	}

	assert.Equal(t, "foo.csv", filepath.Base(index.Filepath))
	assert.Equal(t, 22, len(index.List))

	fh, err := os.Open("testdata/foo.csv")
	if err != nil {
		t.Fatal(err)
	}
	defer fh.Close()

	// Iterate over index entries
	for i, e := range index.List {
		// All entries should have key == "foo"
		assert.Equal(t, "foo", e.Key, `key == "foo"`)

		// Entry blocks should begin with "foo" and end with a newline
		buf := make([]byte, e.Length)
		bytesread, err := fh.ReadAt(buf, e.Offset)
		if err != nil && err != io.EOF {
			t.Fatal(err)
		}
		if int64(bytesread) < e.Length {
			t.Fatalf("bytesread error reading entry %d - read %d bytes, expected %d\n", i, bytesread, e.Length)
		}
		assert.Equal(t, "foo,", string(buf[:4]))
		assert.Equal(t, "\n", string(buf[len(buf)-1]))

		// Check the first line
		if i == 0 {
			expect := "foo,1"
			l := len(expect)
			assert.Equal(t, expect, string(buf[:l]))
		}

		// Check the last line
		if i == index.Length-1 {
			expect := "foo,10000"
			l := len(expect) + 1
			assert.Equal(t, expect, string(buf[len(buf)-l:len(buf)-1]))
		}
	}
}

// Test the index generated for testdata/rir_clc_ipv_range.csv
func TestIndexRIR(t *testing.T) {
	idxopt := bsearch.IndexOptions{Delimiter: []byte(",")}
	index, err := bsearch.NewIndexOptions("testdata/rir_clc_ipv_range.csv", idxopt)
	if err != nil {
		t.Fatal(err)
	}

	assert.Equal(t, "rir_clc_ipv_range.csv", filepath.Base(index.Filepath))
	assert.Equal(t, int64(4096), index.Blocksize)
	assert.Equal(t, 1589, index.Length)
	assert.Equal(t, 2, index.Version)

	fh, err := os.Open("testdata/rir_clc_ipv_range.csv")
	if err != nil {
		t.Fatal(err)
	}
	defer fh.Close()

	// Iterate over index entries
	for i, e := range index.List {
		// All entries should be zero-filled ips
		assert.Equal(t, 15, len(e.Key), "key length == 15")

		// Entry blocks should begin with key followed by a delimiter,
		// and end with a newline
		buf := make([]byte, e.Length)
		bytesread, err := fh.ReadAt(buf, e.Offset)
		if err != nil && err != io.EOF {
			t.Fatal(err)
		}
		if int64(bytesread) < e.Length {
			t.Fatalf("bytesread error reading entry %d - read %d bytes, expected %d\n", i, bytesread, e.Length)
		}
		assert.Equal(t, e.Key+string(index.Delimiter), string(buf[:len(e.Key)+1]))
		assert.Equal(t, "\n", string(buf[len(buf)-1]))

		// Check the first line
		if i == 0 {
			expect := "000.000.000.000,000.255.255.000,,IANA,RESERVED"
			l := len(expect)
			assert.Equal(t, expect, string(buf[:l]))
		}

		// Check the last line
		if i == index.Length-1 {
			expect := "224.000.000.000,255.255.255.000,,IANA,RESERVED"
			l := len(expect) + 1
			assert.Equal(t, expect, string(buf[len(buf)-l:len(buf)-1]))
		}
	}
}
