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
	"time"

	"github.com/ProfoundNetworks/bsearch"
	yaml "gopkg.in/yaml.v3"
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

// copyToTemp copies the file at src into a fresh t.TempDir(), returning the
// path of the copy
func copyToTemp(t *testing.T, src string) string {
	t.Helper()
	data, err := os.ReadFile(src)
	if err != nil {
		t.Fatal(err)
	}
	return writeTemp(t, filepath.Base(src), data)
}

// writeTemp writes data to name in a fresh t.TempDir(), returning its path
func writeTemp(t *testing.T, name string, data []byte) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), name)
	if err := os.WriteFile(path, data, 0644); err != nil {
		t.Fatal(err)
	}
	return path
}

// indexPath returns the index path for dataset path
func indexPath(t *testing.T, path string) string {
	t.Helper()
	idxpath, err := bsearch.IndexPath(path)
	if err != nil {
		t.Fatal(err)
	}
	return idxpath
}

// runIndex runs bsearch_index with args and fails unless it exits 0 with
// empty stdout and stderr
func runIndex(t *testing.T, args ...string) {
	t.Helper()
	stdout, stderr, code := runBin(t, bsearchIndexBin, args...)
	if code != 0 {
		t.Fatalf("%q: expected exit 0, got %d (stderr %q)", args, code, stderr)
	}
	if stdout != "" {
		t.Errorf("%q: expected empty stdout, got %q", args, stdout)
	}
	if stderr != "" {
		t.Errorf("%q: expected empty stderr, got %q", args, stderr)
	}
}

// ageIndex backdates path to two hours ago and its index to one hour ago,
// so the index is still up to date but a rewrite would change its mtime.
// It returns the index's new mtime.
func ageIndex(t *testing.T, path string) time.Time {
	t.Helper()
	now := time.Now().Truncate(time.Second)
	dataTime, idxTime := now.Add(-2*time.Hour), now.Add(-time.Hour)
	if err := os.Chtimes(path, dataTime, dataTime); err != nil {
		t.Fatal(err)
	}
	if err := os.Chtimes(indexPath(t, path), idxTime, idxTime); err != nil {
		t.Fatal(err)
	}
	return idxTime
}

// Test that bsearch_index writes an index that LoadIndex accepts and that
// matches the library's own index for the dataset
func TestCmdWritesIndex(t *testing.T) {
	path := copyToTemp(t, filepath.Join("..", "..", "testdata", "rdns1.csv"))
	runIndex(t, path)

	loaded, err := bsearch.LoadIndex(path)
	if err != nil {
		t.Fatalf("LoadIndex: %s", err)
	}
	expect, err := bsearch.NewIndex(path)
	if err != nil {
		t.Fatal(err)
	}
	if loaded.Filename != "rdns1.csv" {
		t.Errorf("Filename: got %q, expected %q", loaded.Filename, "rdns1.csv")
	}
	if loaded.Blocksize != 2048 {
		t.Errorf("Blocksize: got %d, expected %d", loaded.Blocksize, 2048)
	}
	if string(loaded.Delimiter) != "," {
		t.Errorf("Delimiter: got %q, expected %q", loaded.Delimiter, ",")
	}
	if loaded.KeysUnique {
		t.Errorf("KeysUnique: got %v, expected %v", loaded.KeysUnique, false)
	}
	if loaded.Length == 0 || loaded.Length != expect.Length {
		t.Errorf("Length: got %d, expected %d", loaded.Length, expect.Length)
	}
	if !reflect.DeepEqual(loaded.List, expect.List) {
		t.Errorf("List differs from bsearch.NewIndex:\ngot      %+v\nexpected %+v",
			loaded.List, expect.List)
	}
}

// Test that re-running on an up-to-date index leaves it alone, and that
// -f/--force rewrites it
func TestCmdUpToDate(t *testing.T) {
	path := copyToTemp(t, filepath.Join("..", "..", "testdata", "domains1.csv"))
	idxpath := indexPath(t, path)
	runIndex(t, path)
	before, err := os.ReadFile(idxpath)
	if err != nil {
		t.Fatal(err)
	}

	t.Run("no-op", func(t *testing.T) {
		idxTime := ageIndex(t, path)
		runIndex(t, path)

		stdout, stderr, code := runBin(t, bsearchIndexBin, "-v", path)
		if code != 0 {
			t.Errorf("-v: expected exit 0, got %d", code)
		}
		if stdout != "" {
			t.Errorf("-v: expected empty stdout, got %q", stdout)
		}
		if !strings.Contains(stderr, "index file found and up to date") {
			t.Errorf("-v: expected up-to-date message on stderr, got %q", stderr)
		}

		stat, err := os.Stat(idxpath)
		if err != nil {
			t.Fatal(err)
		}
		if !stat.ModTime().Equal(idxTime) {
			t.Errorf("index mtime: got %v, expected unchanged %v", stat.ModTime(), idxTime)
		}
		after, err := os.ReadFile(idxpath)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(after, before) {
			t.Errorf("index contents changed:\ngot:\n%s\nexpected:\n%s", after, before)
		}
	})

	for _, flag := range []string{"-f", "--force"} {
		t.Run(flag, func(t *testing.T) {
			idxTime := ageIndex(t, path)
			runIndex(t, flag, path)

			stat, err := os.Stat(idxpath)
			if err != nil {
				t.Fatal(err)
			}
			if !stat.ModTime().After(idxTime.Add(30 * time.Minute)) {
				t.Errorf("index mtime: got %v, expected it rewritten after %v",
					stat.ModTime(), idxTime)
			}
			if _, err := bsearch.LoadIndex(path); err != nil {
				t.Errorf("LoadIndex after %s: %s", flag, err)
			}
		})
	}
}

// Test that -c/--cat prints the index as YAML and writes no index file
func TestCmdCat(t *testing.T) {
	for _, flag := range []string{"-c", "--cat"} {
		t.Run(flag, func(t *testing.T) {
			path := copyToTemp(t, filepath.Join("..", "..", "testdata", "domains1.csv"))
			stdout, stderr, code := runBin(t, bsearchIndexBin, flag, path)
			if code != 0 {
				t.Errorf("expected exit 0, got %d", code)
			}
			if stderr != "" {
				t.Errorf("expected empty stderr, got %q", stderr)
			}

			var index bsearch.Index
			if err := yaml.Unmarshal([]byte(stdout), &index); err != nil {
				t.Fatalf("stdout is not YAML: %s\n%s", err, stdout)
			}
			if index.Filename != "domains1.csv" {
				t.Errorf("Filename: got %q, expected %q", index.Filename, "domains1.csv")
			}
			if index.Version != 4 {
				t.Errorf("Version: got %d, expected %d", index.Version, 4)
			}
			if !index.KeysUnique {
				t.Errorf("KeysUnique: got %v, expected %v", index.KeysUnique, true)
			}
			expectList := []bsearch.IndexEntry{{Key: "accuweather.com", Offset: 0}}
			if !reflect.DeepEqual(index.List, expectList) {
				t.Errorf("List: got %+v, expected %+v", index.List, expectList)
			}

			if _, err := os.Stat(indexPath(t, path)); !os.IsNotExist(err) {
				t.Errorf("expected no index file, got stat error %v", err)
			}
		})
	}
}

// Test that -b sets the blocksize in kB
func TestCmdBlocksize(t *testing.T) {
	path := copyToTemp(t, filepath.Join("..", "..", "testdata", "rdns1.csv"))
	runIndex(t, "-b", "4", path)

	index, err := bsearch.LoadIndex(path)
	if err != nil {
		t.Fatal(err)
	}
	if index.Blocksize != 4096 {
		t.Errorf("Blocksize: got %d, expected %d", index.Blocksize, 4096)
	}
	expect, err := bsearch.NewIndexOptions(path, bsearch.IndexOptions{Blocksize: 4096})
	if err != nil {
		t.Fatal(err)
	}
	if index.Length != expect.Length {
		t.Errorf("Length: got %d, expected %d", index.Length, expect.Length)
	}
}

// Test -t on a dataset whose delimiter can't be guessed, with and without --hdr
func TestCmdSepHeader(t *testing.T) {
	// "KEY" sorts before the data keys, so the header isn't auto-detected
	data := []byte("KEY|VAL\na|1\nb|2\nc|3\n")
	tests := []struct {
		args         []string
		header       bool
		headerFields []string
		firstKey     string
	}{
		{[]string{"-t", "|"}, false, nil, "KEY"},
		{[]string{"--sep", "|", "--hdr"}, true, []string{"KEY", "VAL"}, "a"},
	}
	for _, tc := range tests {
		t.Run(strings.Join(tc.args, " "), func(t *testing.T) {
			path := writeTemp(t, "data.txt", data)
			runIndex(t, append(tc.args, path)...)

			index, err := bsearch.LoadIndex(path)
			if err != nil {
				t.Fatal(err)
			}
			if string(index.Delimiter) != "|" {
				t.Errorf("Delimiter: got %q, expected %q", index.Delimiter, "|")
			}
			if index.Header != tc.header {
				t.Errorf("Header: got %v, expected %v", index.Header, tc.header)
			}
			if !reflect.DeepEqual(index.HeaderFields, tc.headerFields) {
				t.Errorf("HeaderFields: got %q, expected %q", index.HeaderFields, tc.headerFields)
			}
			if len(index.List) != 1 || index.List[0].Key != tc.firstKey {
				t.Errorf("List: got %+v, expected one entry with key %q", index.List, tc.firstKey)
			}
		})
	}
}

// Test bsearch_index failures: exit status, stderr, and no index written
func TestCmdErrors(t *testing.T) {
	tests := []struct {
		name   string
		file   string
		data   string // "" means don't create file
		code   int
		stderr string // substring expected in stderr
	}{
		// The violation is on line 4: an out-of-order line 2 would be
		// taken as a header instead
		{"unsorted", "data.csv", "a,1\nb,2\nc,3\nb,4\n", 1,
			`key sort violation - "c" > "b"`},
		{"missing", "nosuch.csv", "", 1, "no such file or directory"},
		{"no delimiter", "data.txt", "a|1\nb|2\n", 1, "cannot guess delimiter"},
		{"compressed", "data.csv.gz", "a,1\n", 2, "appears to be compressed - cannot binary search"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), tc.file)
			if tc.data != "" {
				if err := os.WriteFile(path, []byte(tc.data), 0644); err != nil {
					t.Fatal(err)
				}
			}
			stdout, stderr, code := runBin(t, bsearchIndexBin, path)
			if code != tc.code {
				t.Errorf("expected exit %d, got %d", tc.code, code)
			}
			if stdout != "" {
				t.Errorf("expected empty stdout, got %q", stdout)
			}
			if !strings.Contains(stderr, tc.stderr) {
				t.Errorf("expected stderr to contain %q, got %q", tc.stderr, stderr)
			}
			if _, err := os.Stat(indexPath(t, path)); !os.IsNotExist(err) {
				t.Errorf("expected no index file, got stat error %v", err)
			}
		})
	}
}

// Test that -v logs progress and -vv adds per-line debug output
func TestCmdVerbose(t *testing.T) {
	for _, tc := range []struct {
		flag  string
		debug bool
	}{{"-v", false}, {"-vv", true}} {
		path := writeTemp(t, "data.csv", []byte("a,1\nb,2\n"))
		stdout, stderr, code := runBin(t, bsearchIndexBin, tc.flag, path)
		if code != 0 || stdout != "" {
			t.Errorf("%s: got exit %d, stdout %q", tc.flag, code, stdout)
		}
		if got := strings.Contains(stderr, "generateLineIndex loop"); got != tc.debug {
			t.Errorf("%s: debug output %v, expected %v; stderr %q", tc.flag, got, tc.debug, stderr)
		}
		if strings.Contains(stderr, "\x1b[") {
			t.Errorf("%s: expected no colour codes when stderr isn't a terminal, got %q", tc.flag, stderr)
		}
	}
}

// Test that a failure to write the index exits 1 and leaves no index
func TestCmdWriteFails(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root ignores directory permissions")
	}
	path := writeTemp(t, "data.csv", []byte("a,1\nb,2\n"))
	dir := filepath.Dir(path)
	if err := os.Chmod(dir, 0555); err != nil {
		t.Fatal(err)
	}
	defer os.Chmod(dir, 0755)

	stdout, stderr, code := runBin(t, bsearchIndexBin, path)
	if code != 1 || stdout != "" {
		t.Errorf("got exit %d, stdout %q; expected exit 1 and no output", code, stdout)
	}
	if !strings.Contains(stderr, "permission denied") {
		t.Errorf("expected a permission error on stderr, got %q", stderr)
	}
	if _, err := os.Stat(indexPath(t, path)); !os.IsNotExist(err) {
		t.Errorf("expected no index file, got stat error %v", err)
	}
}
