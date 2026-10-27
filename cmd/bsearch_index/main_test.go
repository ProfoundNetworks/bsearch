package main

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/ProfoundNetworks/bsearch"
	"github.com/stretchr/testify/assert"
)

// Set by TestMain
var (
	bsearchIndexBin string // freshly built bsearch_index binary
	tmpdir          string // scratch dir holding bsearchIndexBin
)

// TestMain builds bsearch_index into a temporary directory, so tests never run
// against a stale ./bsearch_index binary
func TestMain(m *testing.M) {
	var err error
	tmpdir, err = os.MkdirTemp("", "bsearch_index_test")
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

	bsearchIndexBin = filepath.Join(tmpdir, "bsearch_index")
	out, err := exec.Command("go", "build", "-o", bsearchIndexBin, ".").CombinedOutput()
	if err != nil {
		fmt.Fprintf(os.Stderr, "building bsearch_index: %s\n%s", err, out)
		os.RemoveAll(tmpdir)
		os.Exit(1)
	}

	code := m.Run()
	os.RemoveAll(tmpdir)
	os.Exit(code)
}

// TestParserGrammar exercises the real kong grammar from newParser. A
// []bool Verbose would hang here (kong trap 5), so run with -timeout.
func TestParserGrammar(t *testing.T) {
	tests := []struct {
		args   []string
		expect Options
	}{
		{[]string{"data.csv"}, Options{}},
		{[]string{"-v", "data.csv"}, Options{Verbose: 1}},
		{[]string{"-vv", "data.csv"}, Options{Verbose: 2}},
		{[]string{"--verbose", "--verbose", "data.csv"}, Options{Verbose: 2}},
		{[]string{"-t", "|", "data.csv"}, Options{Delim: "|"}},
		{[]string{"-t|", "data.csv"}, Options{Delim: "|"}},
		{[]string{"--sep", ",", "data.csv"}, Options{Delim: ","}},
		{[]string{"--sep=,", "data.csv"}, Options{Delim: ","}},
		{[]string{"--hdr", "data.csv"}, Options{Header: true}},
		{[]string{"-f", "data.csv"}, Options{Force: true}},
		{[]string{"--force", "data.csv"}, Options{Force: true}},
		{[]string{"-c", "data.csv"}, Options{Cat: true}},
		{[]string{"--cat", "data.csv"}, Options{Cat: true}},
		{[]string{"-b", "4", "data.csv"}, Options{Blocksize: 4}},
		{[]string{"-b4", "data.csv"}, Options{Blocksize: 4}},
		{[]string{"--bs", "8", "data.csv"}, Options{Blocksize: 8}},
		{[]string{"--bs=8", "data.csv"}, Options{Blocksize: 8}},
		{[]string{"-vfc", "data.csv", "-b", "4"},
			Options{Verbose: 1, Force: true, Cat: true, Blocksize: 4}},
	}
	for _, tc := range tests {
		t.Run(strings.Join(tc.args, " "), func(t *testing.T) {
			var got Options
			parser, err := newParser(&got)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := parser.Parse(tc.args); err != nil {
				t.Fatal(err)
			}
			tc.expect.Args.Filename = "data.csv"
			if got != tc.expect {
				t.Errorf("got %+v, expected %+v", got, tc.expect)
			}
		})
	}

	// Filename is required; -H is not a bsearch_index flag; --bs is an int
	for _, args := range [][]string{{}, {"-H", "data.csv"}, {"-b", "x", "data.csv"}, {"--bogus", "data.csv"}} {
		var got Options
		parser, err := newParser(&got)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := parser.Parse(args); err == nil {
			t.Errorf("%q: expected parse error, got %+v", args, got)
		}
	}
}

// runBin runs bin with args, returning stdout, stderr and the exit status
// separately (never combined, so stream mixups are visible)
func runBin(t *testing.T, bin string, args ...string) (string, string, int) {
	t.Helper()
	cmd := exec.Command(bin, args...)
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	err := cmd.Run()
	if _, ok := err.(*exec.ExitError); err != nil && !ok {
		t.Fatal(err)
	}
	return stdout.String(), stderr.String(), cmd.ProcessState.ExitCode()
}

// TestCLIFlagConventions checks help and usage-error streams and statuses
func TestCLIFlagConventions(t *testing.T) {
	for _, flag := range []string{"--help", "-h"} {
		stdout, stderr, code := runBin(t, bsearchIndexBin, flag)
		if code != 0 {
			t.Errorf("%s: expected exit 0, got %d", flag, code)
		}
		if !strings.Contains(stdout, "Usage:") {
			t.Errorf("%s: expected usage on stdout, got %q", flag, stdout)
		}
		if stderr != "" {
			t.Errorf("%s: expected empty stderr, got %q", flag, stderr)
		}
	}

	for _, args := range [][]string{{"--bogus"}, {}} {
		stdout, stderr, code := runBin(t, bsearchIndexBin, args...)
		if code != 2 {
			t.Errorf("%q: expected exit 2, got %d", args, code)
		}
		if stdout != "" {
			t.Errorf("%q: expected empty stdout, got %q", args, stdout)
		}
		if !strings.Contains(stderr, "Usage:") || !strings.Contains(stderr, "error:") {
			t.Errorf("%q: expected error and usage on stderr, got %q", args, stderr)
		}
	}
}

// Test the index generated for testdata/foo.csv
func TestIndexFoo(t *testing.T) {
	index, err := bsearch.NewIndex("testdata/foo.csv")
	if err != nil {
		t.Fatal(err)
	}

	assert.Equal(t, "foo.csv", index.Filename, "Filename")
	if !strings.HasSuffix(index.Filepath, "/testdata/foo.csv") {
		t.Errorf("foo.csv Filepath %q does not end with %q",
			index.Filepath, "/testdata/foo.csv")
	}
	assert.Equal(t, true, index.Header)
	assert.Equal(t, true, index.KeysIndexFirst)
	assert.Equal(t, false, index.KeysUnique)
	assert.Equal(t, 2, len(index.List))
	assert.Equal(t, 4, index.Version)

	fh, err := os.Open("testdata/foo.csv")
	if err != nil {
		t.Fatal(err)
	}
	defer fh.Close()

	// Iterate over index entries
	for i, e := range index.List {
		var length int64
		if i+1 < index.Length {
			length = index.List[i+1].Offset - e.Offset
		} else {
			stat, err := os.Stat(index.Filepath)
			assert.Nil(t, err)
			length = stat.Size() - e.Offset
		}

		// Entry blocks should begin with key followed by a delimiter,
		// and end with a newline
		buf := make([]byte, length)
		bytesread, err := fh.ReadAt(buf, e.Offset)
		if err != nil && err != io.EOF {
			t.Fatal(err)
		}
		if int64(bytesread) < length {
			t.Fatalf("bytesread error reading entry %d - read %d bytes, expected %d\n", i, bytesread, length)
		}
		assert.Equal(t, e.Key+string(index.Delimiter), string(buf[:len(e.Key)+1]))
		assert.Equal(t, "\n", string(buf[len(buf)-1]))

		// Check the first line
		if i == 0 {
			expect := "bar,1"
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

	// Write resets Filepath, so capture and restore below
	path := index.Filepath
	indexpath, err := bsearch.IndexPath(path)
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(indexpath)
	err = index.Write()
	if err != nil {
		t.Fatalf("could not write index to %q: %s", indexpath, err)
	}

	loaded, err := bsearch.LoadIndex("testdata/foo.csv")
	if err != nil {
		t.Fatalf("could not load index from %q: %s", indexpath, err)
	}
	// Restore index.Filepath
	index.Filepath = path
	assert.Equal(t, index, loaded)
}

// Test the index generated for testdata/rir_clc_ipv_range.csv
func TestIndexRIR(t *testing.T) {
	idxopt := bsearch.IndexOptions{
		Delimiter: []byte(","),
	}
	index, err := bsearch.NewIndexOptions("testdata/rir_clc_ipv_range.csv", idxopt)
	if err != nil {
		t.Fatal(err)
	}

	assert.Equal(t, "rir_clc_ipv_range.csv", filepath.Base(index.Filepath))
	assert.Equal(t, 2048, index.Blocksize)
	assert.Equal(t, true, index.KeysIndexFirst)
	assert.Equal(t, true, index.KeysUnique)
	assert.Equal(t, 3178, index.Length)
	assert.Equal(t, 4, index.Version)

	fh, err := os.Open("testdata/rir_clc_ipv_range.csv")
	if err != nil {
		t.Fatal(err)
	}
	defer fh.Close()

	// Iterate over index entries
	for i, e := range index.List {
		// All entries should be zero-filled ips
		assert.Equal(t, 15, len(e.Key), "key length == 15")

		var length int64
		if i+1 < index.Length {
			length = index.List[i+1].Offset - e.Offset
		} else {
			stat, err := os.Stat(index.Filepath)
			assert.Nil(t, err)
			length = stat.Size() - e.Offset
		}

		// Entry blocks should begin with key followed by a delimiter,
		// and end with a newline
		buf := make([]byte, length)
		bytesread, err := fh.ReadAt(buf, e.Offset)
		if err != nil && err != io.EOF {
			t.Fatal(err)
		}
		if int64(bytesread) < length {
			t.Fatalf("bytesread error reading entry %d - read %d bytes, expected %d\n",
				i, bytesread, length)
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

// Test the index generated for testdata/rir_clc_ipv_range.csv with Header true
func TestIndexRIRHeader(t *testing.T) {
	idxopt := bsearch.IndexOptions{
		Delimiter: []byte(","),
		Header:    true,
	}
	index, err := bsearch.NewIndexOptions("testdata/rir_clc_ipv_range.csv", idxopt)
	if err != nil {
		t.Fatal(err)
	}

	assert.Equal(t, "rir_clc_ipv_range.csv", filepath.Base(index.Filepath))
	assert.Equal(t, 2048, index.Blocksize)
	assert.Equal(t, true, index.Header)
	assert.Equal(t, true, index.KeysIndexFirst)
	assert.Equal(t, true, index.KeysUnique)
	assert.Equal(t, 3178, index.Length)
	assert.Equal(t, 4, index.Version)

	fh, err := os.Open("testdata/rir_clc_ipv_range.csv")
	if err != nil {
		t.Fatal(err)
	}
	defer fh.Close()

	// Iterate over index entries
	for i, e := range index.List {
		// All entries should be zero-filled ips
		assert.Equal(t, 15, len(e.Key), "key length == 15")

		var length int64
		if i+1 < index.Length {
			length = index.List[i+1].Offset - e.Offset
		} else {
			stat, err := os.Stat(index.Filepath)
			assert.Nil(t, err)
			length = stat.Size() - e.Offset
		}

		// Entry blocks should begin with key followed by a delimiter,
		// and end with a newline
		buf := make([]byte, length)
		bytesread, err := fh.ReadAt(buf, e.Offset)
		if err != nil && err != io.EOF {
			t.Fatal(err)
		}
		if int64(bytesread) < length {
			t.Fatalf("bytesread error reading entry %d - read %d bytes, expected %d\n",
				i, bytesread, length)
		}
		assert.Equal(t, e.Key+string(index.Delimiter), string(buf[:len(e.Key)+1]))
		assert.Equal(t, "\n", string(buf[len(buf)-1]))

		// Check the first line (skipping the 'header')
		if i == 0 {
			expect := "001.000.000.000,001.000.000.000,AU,APNIC,ASSIGNED"
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
