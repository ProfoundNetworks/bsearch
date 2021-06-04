package main

import (
	"io"
	"os"
	"testing"

	"github.com/ProfoundNetworks/bsearch"
	"github.com/stretchr/testify/assert"
)

// Test the index generated for testdata/foo.csv
func TestFooIndex(t *testing.T) {
	index, err := bsearch.NewIndex("testdata/foo.csv")
	if err != nil {
		t.Fatal(err)
	}

	assert.Equal(t, "foo.csv", index.Filename)
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

		if i == 0 {
			expect := "foo,1"
			l := len(expect)
			assert.Equal(t, expect, string(buf[:l]))
		}

		if i == len(index.List)-1 {
			expect := "foo,10000\n"
			l := len(expect)
			assert.Equal(t, expect, string(buf[len(buf)-l:]))
		}
	}
}
