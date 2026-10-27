package main

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/ProfoundNetworks/bsearch"
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

	if index.Filename != "foo.csv" {
		t.Errorf("Filename: got %q, expected %q", index.Filename, "foo.csv")
	}
	if !strings.HasSuffix(index.Filepath, "/testdata/foo.csv") {
		t.Errorf("foo.csv Filepath %q does not end with %q",
			index.Filepath, "/testdata/foo.csv")
	}
	if !index.Header {
		t.Errorf("Header: got %v, expected %v", index.Header, true)
	}
	if !index.KeysIndexFirst {
		t.Errorf("KeysIndexFirst: got %v, expected %v", index.KeysIndexFirst, true)
	}
	if index.KeysUnique {
		t.Errorf("KeysUnique: got %v, expected %v", index.KeysUnique, false)
	}
	if len(index.List) != 2 {
		t.Errorf("len(List): got %d, expected %d", len(index.List), 2)
	}
	if index.Version != 4 {
		t.Errorf("Version: got %d, expected %d", index.Version, 4)
	}

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
			if err != nil {
				t.Fatalf("stat %q: got error %v, expected nil", index.Filepath, err)
			}
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
		if got, expect := string(buf[:len(e.Key)+1]), e.Key+string(index.Delimiter); got != expect {
			t.Errorf("entry %d block start: got %q, expected %q", i, got, expect)
		}
		if got := string(buf[len(buf)-1]); got != "\n" {
			t.Errorf("entry %d block end: got %q, expected %q", i, got, "\n")
		}

		// Check the first line
		if i == 0 {
			expect := "bar,1"
			l := len(expect)
			if got := string(buf[:l]); got != expect {
				t.Errorf("entry %d first line: got %q, expected %q", i, got, expect)
			}
		}

		// Check the last line
		if i == index.Length-1 {
			expect := "foo,10000"
			l := len(expect) + 1
			if got := string(buf[len(buf)-l : len(buf)-1]); got != expect {
				t.Errorf("entry %d last line: got %q, expected %q", i, got, expect)
			}
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
	if !reflect.DeepEqual(loaded, index) {
		t.Errorf("loaded index differs from written index:\ngot      %+v\nexpected %+v", loaded, index)
	}
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

	if filepath.Base(index.Filepath) != "rir_clc_ipv_range.csv" {
		t.Errorf("Filepath base: got %q, expected %q", filepath.Base(index.Filepath), "rir_clc_ipv_range.csv")
	}
	if index.Blocksize != 2048 {
		t.Errorf("Blocksize: got %d, expected %d", index.Blocksize, 2048)
	}
	if !index.KeysIndexFirst {
		t.Errorf("KeysIndexFirst: got %v, expected %v", index.KeysIndexFirst, true)
	}
	if !index.KeysUnique {
		t.Errorf("KeysUnique: got %v, expected %v", index.KeysUnique, true)
	}
	if index.Length != 3178 {
		t.Errorf("Length: got %d, expected %d", index.Length, 3178)
	}
	if index.Version != 4 {
		t.Errorf("Version: got %d, expected %d", index.Version, 4)
	}

	fh, err := os.Open("testdata/rir_clc_ipv_range.csv")
	if err != nil {
		t.Fatal(err)
	}
	defer fh.Close()

	// Iterate over index entries
	for i, e := range index.List {
		// All entries should be zero-filled ips
		if len(e.Key) != 15 {
			t.Errorf("entry %d key length == 15: got %d (%q), expected %d", i, len(e.Key), e.Key, 15)
		}

		var length int64
		if i+1 < index.Length {
			length = index.List[i+1].Offset - e.Offset
		} else {
			stat, err := os.Stat(index.Filepath)
			if err != nil {
				t.Fatalf("stat %q: got error %v, expected nil", index.Filepath, err)
			}
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
		if got, expect := string(buf[:len(e.Key)+1]), e.Key+string(index.Delimiter); got != expect {
			t.Errorf("entry %d block start: got %q, expected %q", i, got, expect)
		}
		if got := string(buf[len(buf)-1]); got != "\n" {
			t.Errorf("entry %d block end: got %q, expected %q", i, got, "\n")
		}

		// Check the first line
		if i == 0 {
			expect := "000.000.000.000,000.255.255.000,,IANA,RESERVED"
			l := len(expect)
			if got := string(buf[:l]); got != expect {
				t.Errorf("entry %d first line: got %q, expected %q", i, got, expect)
			}
		}

		// Check the last line
		if i == index.Length-1 {
			expect := "224.000.000.000,255.255.255.000,,IANA,RESERVED"
			l := len(expect) + 1
			if got := string(buf[len(buf)-l : len(buf)-1]); got != expect {
				t.Errorf("entry %d last line: got %q, expected %q", i, got, expect)
			}
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

	if filepath.Base(index.Filepath) != "rir_clc_ipv_range.csv" {
		t.Errorf("Filepath base: got %q, expected %q", filepath.Base(index.Filepath), "rir_clc_ipv_range.csv")
	}
	if index.Blocksize != 2048 {
		t.Errorf("Blocksize: got %d, expected %d", index.Blocksize, 2048)
	}
	if !index.Header {
		t.Errorf("Header: got %v, expected %v", index.Header, true)
	}
	if !index.KeysIndexFirst {
		t.Errorf("KeysIndexFirst: got %v, expected %v", index.KeysIndexFirst, true)
	}
	if !index.KeysUnique {
		t.Errorf("KeysUnique: got %v, expected %v", index.KeysUnique, true)
	}
	if index.Length != 3178 {
		t.Errorf("Length: got %d, expected %d", index.Length, 3178)
	}
	if index.Version != 4 {
		t.Errorf("Version: got %d, expected %d", index.Version, 4)
	}

	fh, err := os.Open("testdata/rir_clc_ipv_range.csv")
	if err != nil {
		t.Fatal(err)
	}
	defer fh.Close()

	// Iterate over index entries
	for i, e := range index.List {
		// All entries should be zero-filled ips
		if len(e.Key) != 15 {
			t.Errorf("entry %d key length == 15: got %d (%q), expected %d", i, len(e.Key), e.Key, 15)
		}

		var length int64
		if i+1 < index.Length {
			length = index.List[i+1].Offset - e.Offset
		} else {
			stat, err := os.Stat(index.Filepath)
			if err != nil {
				t.Fatalf("stat %q: got error %v, expected nil", index.Filepath, err)
			}
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
		if got, expect := string(buf[:len(e.Key)+1]), e.Key+string(index.Delimiter); got != expect {
			t.Errorf("entry %d block start: got %q, expected %q", i, got, expect)
		}
		if got := string(buf[len(buf)-1]); got != "\n" {
			t.Errorf("entry %d block end: got %q, expected %q", i, got, "\n")
		}

		// Check the first line (skipping the 'header')
		if i == 0 {
			expect := "001.000.000.000,001.000.000.000,AU,APNIC,ASSIGNED"
			l := len(expect)
			if got := string(buf[:l]); got != expect {
				t.Errorf("entry %d first line: got %q, expected %q", i, got, expect)
			}
		}

		// Check the last line
		if i == index.Length-1 {
			expect := "224.000.000.000,255.255.255.000,,IANA,RESERVED"
			l := len(expect) + 1
			if got := string(buf[len(buf)-l : len(buf)-1]); got != expect {
				t.Errorf("entry %d last line: got %q, expected %q", i, got, expect)
			}
		}
	}
}
