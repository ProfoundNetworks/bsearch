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
)

// Set by TestMain
var (
	bsearchBin string // freshly built bsearch binary
	tmpdir     string // scratch dir holding bsearchBin and indexed datasets
)

// TestMain builds bsearch into a temporary directory, so tests never run
// against a stale ./bsearch binary
func TestMain(m *testing.M) {
	var err error
	tmpdir, err = os.MkdirTemp("", "bsearch_test")
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

	bsearchBin = filepath.Join(tmpdir, "bsearch")
	out, err := exec.Command("go", "build", "-o", bsearchBin, ".").CombinedOutput()
	if err != nil {
		fmt.Fprintf(os.Stderr, "building bsearch: %s\n%s", err, out)
		os.RemoveAll(tmpdir)
		os.Exit(1)
	}

	code := m.Run()
	os.RemoveAll(tmpdir)
	os.Exit(code)
}

// indexedCopy copies the dataset at path into tmpdir and writes a fresh
// index alongside it using opt, returning the path of the copy
func indexedCopy(t *testing.T, path string, opt bsearch.IndexOptions) string {
	t.Helper()
	src, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer src.Close()

	dst := filepath.Join(tmpdir, filepath.Base(path))
	fh, err := os.Create(dst)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := io.Copy(fh, src); err != nil {
		fh.Close()
		t.Fatal(err)
	}
	if err := fh.Close(); err != nil {
		t.Fatal(err)
	}

	index, err := bsearch.NewIndexOptions(dst, opt)
	if err != nil {
		t.Fatal(err)
	}
	if err := index.Write(); err != nil {
		t.Fatal(err)
	}
	return dst
}

func TestCmdBsearch(t *testing.T) {
	var tests = []struct {
		name   string
		args   string
		search string
		expect string
	}{
		{"001.000.128.000", "001.000.128.000", "",
			"001.000.128.000,node-0.pool-1-0.dynamic.totinternet.net,202003,totinternet.net"},
		{"001.034.164.000", "001.034.164.000", "",
			"001.034.164.000,1-34-164-0.HINET-IP.hinet.net,202003,hinet.net"},
		{"003.122.207.000", "003.122.207.000", "",
			"003.122.207.000,ec2-3-122-207-0.eu-central-1.compute.amazonaws.com,202003,amazonaws.com"},
		{"003.126.183.000", "003.126.183.000", "",
			"003.126.183.000,ec2-3-126-183-0.eu-central-1.compute.amazonaws.com,202003,amazonaws.com"},
		{"024.066.017.000", "024.066.017.000", "",
			"024.066.017.000,S0106905851b9f0e0.rd.shawcable.net,202003,shawcable.net"},
		{"032.176.184.000", "032.176.184.000", "",
			`032.176.184.000,mobile000.mycingular.net,202003,mycingular.net
032.176.184.000,mobile001.mycingular.net,202003,mycingular.net
032.176.184.000,mobile002.mycingular.net,202003,mycingular.net
032.176.184.000,mobile003.mycingular.net,202003,mycingular.net
032.176.184.000,mobile004.mycingular.net,202003,mycingular.net
032.176.184.000,mobile005.mycingular.net,202003,mycingular.net`},
		{"223.252.003.000", "223.252.003.000", "",
			"223.252.003.000,223-252-3-0.as45671.net,202003,as45671.net"},
	}

	infile := indexedCopy(t, filepath.Join("..", "..", "testdata", "rdns1.csv"),
		bsearch.IndexOptions{})

	for _, tc := range tests {
		args := append(strings.Fields(tc.args+" "+tc.search), infile)

		output, err := exec.Command(bsearchBin, args...).CombinedOutput()
		got := strings.TrimSpace(string(output))
		if err != nil {
			t.Fatalf("%s: %s", err.Error(), got)
		}

		if got != tc.expect {
			t.Errorf("test %q arg test failed:\n\ngot:\n%s\n\nexpected:\n%s\n", tc.name, got, tc.expect)
		}
	}
}

// Test --rev on single-column reversed-domain datasets, with and without
// a header
func TestRev(t *testing.T) {
	var tests = []struct {
		search string
		expect string // "" means not found
	}{
		{"0-0.ca", "0-0.ca"},
		{"3-2-1-0.ca", "3-2-1-0.ca"},
		{"card1000.ca", "card1000.ca"},
		{"ed1000.ca", "ed1000.ca"},
		{"www.ed1000.ca", "www.ed1000.ca"},
		{"fooed1000.ca", ""}, // only www.fooed1000.ca exists
	}

	datasets := []struct {
		filename string
		args     []string
		header   bool
	}{
		{"ca_rev.txt", []string{"-r"}, false},
		{"ca_rev_hdr.txt", []string{"-r", "--hdr"}, true},
	}

	for _, ds := range datasets {
		infile := indexedCopy(t, filepath.Join("testdata", ds.filename),
			bsearch.IndexOptions{Delimiter: []byte(","), Header: ds.header})

		for _, tc := range tests {
			args := append(append([]string{}, ds.args...), tc.search, infile)
			output, err := exec.Command(bsearchBin, args...).CombinedOutput()
			got := strings.TrimSpace(string(output))

			if tc.expect == "" {
				if err == nil || got != "Error: key not found" {
					t.Errorf("%s %q: got %q (err %v), expected not found",
						ds.filename, tc.search, got, err)
				}
				continue
			}
			if err != nil {
				t.Errorf("%s %q: %s: %s", ds.filename, tc.search, err.Error(), got)
				continue
			}
			if got != tc.expect {
				t.Errorf("%s %q: got:\n%s\n\nexpected:\n%s\n",
					ds.filename, tc.search, got, tc.expect)
			}
		}
	}
}

// Test that a dataset with no index gives an error pointing at bsearch_index
func TestMissingIndex(t *testing.T) {
	infile := filepath.Join(t.TempDir(), "noindex.csv")
	if err := os.WriteFile(infile, []byte("a,1\n"), 0644); err != nil {
		t.Fatal(err)
	}

	output, err := exec.Command(bsearchBin, "a", infile).CombinedOutput()
	got := strings.TrimSpace(string(output))
	if err == nil {
		t.Fatalf("expected failure, got success: %s", got)
	}
	expect := fmt.Sprintf("Error: no index found for %q - create one with bsearch_index", infile)
	if got != expect {
		t.Errorf("got:\n%s\n\nexpected:\n%s\n", got, expect)
	}
}

// Test that compressed datasets (including zstd) are rejected up front
func TestCompressed(t *testing.T) {
	dir := t.TempDir()
	for _, filename := range []string{"data.csv.gz", "data.csv.zst"} {
		filename = filepath.Join(dir, filename)
		if err := os.WriteFile(filename, []byte("a,1\n"), 0644); err != nil {
			t.Fatal(err)
		}
		output, err := exec.Command(bsearchBin, "a", filename).CombinedOutput()
		got := strings.TrimSpace(string(output))
		exitErr, ok := err.(*exec.ExitError)
		if !ok || exitErr.ExitCode() != 2 {
			t.Errorf("%s: expected exit status 2, got %v: %s", filename, err, got)
			continue
		}
		expect := fmt.Sprintf("Filename %q appears to be compressed - cannot binary search", filename)
		if got != expect {
			t.Errorf("%s: got:\n%s\n\nexpected:\n%s\n", filename, got, expect)
		}
	}
}

// TestParserGrammar exercises the real kong grammar from newParser. A
// []bool Verbose would hang here (kong trap 5), so run with -timeout.
func TestParserGrammar(t *testing.T) {
	tests := []struct {
		args   []string
		expect Options
	}{
		{[]string{"foo", "data.csv"}, Options{}},
		{[]string{"-v", "foo", "data.csv"}, Options{Verbose: 1}},
		{[]string{"-vv", "foo", "data.csv"}, Options{Verbose: 2}},
		{[]string{"--verbose", "--verbose", "foo", "data.csv"}, Options{Verbose: 2}},
		{[]string{"-H", "foo", "data.csv"}, Options{Header: true}},
		{[]string{"--hdr", "foo", "data.csv"}, Options{Header: true}},
		{[]string{"-r", "foo", "data.csv"}, Options{Rev: true}},
		{[]string{"--rev", "foo", "data.csv"}, Options{Rev: true}},
		{[]string{"-c", "foo", "data.csv"}, Options{Stdin: true}},
		{[]string{"--stdin", "foo", "data.csv"}, Options{Stdin: true}},
		{[]string{"-vHrc", "foo", "data.csv"},
			Options{Verbose: 1, Header: true, Rev: true, Stdin: true}},
		{[]string{"foo", "data.csv", "--rev", "-v"}, Options{Verbose: 1, Rev: true}},
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
			tc.expect.Args.SearchString = "foo"
			tc.expect.Args.Filename = "data.csv"
			if got != tc.expect {
				t.Errorf("got %+v, expected %+v", got, tc.expect)
			}
		})
	}

	// Both positionals are required, even with --stdin
	for _, args := range [][]string{{}, {"foo"}, {"--stdin", "foo"}, {"--bogus", "foo", "data.csv"}} {
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
	return runBinStdin(t, nil, bin, args...)
}

// runBinStdin is runBin with stdin attached to the binary
func runBinStdin(t *testing.T, stdin io.Reader, bin string, args ...string) (string, string, int) {
	t.Helper()
	cmd := exec.Command(bin, args...)
	cmd.Stdin = stdin
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
		stdout, stderr, code := runBin(t, bsearchBin, flag)
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
		stdout, stderr, code := runBin(t, bsearchBin, args...)
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

// The six rdns1.csv lines for the duplicated key 032.176.184.000
const rdnsDupLines = `032.176.184.000,mobile000.mycingular.net,202003,mycingular.net
032.176.184.000,mobile001.mycingular.net,202003,mycingular.net
032.176.184.000,mobile002.mycingular.net,202003,mycingular.net
032.176.184.000,mobile003.mycingular.net,202003,mycingular.net
032.176.184.000,mobile004.mycingular.net,202003,mycingular.net
032.176.184.000,mobile005.mycingular.net,202003,mycingular.net
`

// Test -c/--stdin batch lookups: each key's lines, then a blank line
func TestStdin(t *testing.T) {
	const uniqLine = "001.000.128.000,node-0.pool-1-0.dynamic.totinternet.net,202003,totinternet.net\n"
	tests := []struct {
		name   string
		stdin  string
		expect string
	}{
		{"unique", "001.000.128.000\n", uniqLine + "\n"},
		{"duplicate", "032.176.184.000\n", rdnsDupLines + "\n"},
		{"missing", "999.999.999.999\n", "\n"},
		{"several", "032.176.184.000\n999.999.999.999\n001.000.128.000\n",
			rdnsDupLines + "\n" + "\n" + uniqLine + "\n"},
		{"empty", "", ""},
	}

	infile := indexedCopy(t, filepath.Join("..", "..", "testdata", "rdns1.csv"),
		bsearch.IndexOptions{})

	for _, tc := range tests {
		for _, flag := range []string{"-c", "--stdin"} {
			t.Run(tc.name+" "+flag, func(t *testing.T) {
				stdout, stderr, code := runBinStdin(t, strings.NewReader(tc.stdin),
					bsearchBin, flag, "dummy", infile)
				if code != 0 {
					t.Errorf("expected exit 0, got %d", code)
				}
				if stderr != "" {
					t.Errorf("expected empty stderr, got %q", stderr)
				}
				if stdout != tc.expect {
					t.Errorf("stdout:\ngot      %q\nexpected %q", stdout, tc.expect)
				}
			})
		}
	}
}

// Test that -c reads a final key with no trailing newline
func TestStdinUnterminatedKey(t *testing.T) {
	infile := indexedCopy(t, filepath.Join("..", "..", "testdata", "rdns1.csv"),
		bsearch.IndexOptions{})
	stdout, stderr, code := runBinStdin(t, strings.NewReader("032.176.184.000"),
		bsearchBin, "-c", "dummy", infile)
	if code != 0 {
		t.Errorf("expected exit 0, got %d", code)
	}
	if stderr != "" {
		t.Errorf("expected empty stderr, got %q", stderr)
	}
	if expect := rdnsDupLines + "\n"; stdout != expect {
		t.Errorf("stdout:\ngot      %q\nexpected %q", stdout, expect)
	}
}

// Test -c combined with -r: keys are reversed for the search and the
// results are reversed back for output
func TestStdinRev(t *testing.T) {
	infile := indexedCopy(t, filepath.Join("testdata", "ca_rev.txt"),
		bsearch.IndexOptions{Delimiter: []byte(",")})

	stdin := "0-0.ca\nfooed1000.ca\nwww.ed1000.ca\ned1000.ca\n"
	expect := "0-0.ca\n\n" + "\n" + "www.ed1000.ca\n\n" + "ed1000.ca\n\n"
	stdout, stderr, code := runBinStdin(t, strings.NewReader(stdin),
		bsearchBin, "-c", "-r", "dummy", infile)
	if code != 0 {
		t.Errorf("expected exit 0, got %d", code)
	}
	if stderr != "" {
		t.Errorf("expected empty stderr, got %q", stderr)
	}
	if stdout != expect {
		t.Errorf("stdout:\ngot      %q\nexpected %q", stdout, expect)
	}
}

// Test that -v and -vv log to stderr and leave stdout unchanged
func TestVerbose(t *testing.T) {
	infile := indexedCopy(t, filepath.Join("..", "..", "testdata", "rdns1.csv"),
		bsearch.IndexOptions{})
	idxpath, err := bsearch.IndexPath(infile)
	if err != nil {
		t.Fatal(err)
	}

	quietOut, quietErr, code := runBin(t, bsearchBin, "032.176.184.000", infile)
	if code != 0 {
		t.Fatalf("no -v: expected exit 0, got %d (stderr %q)", code, quietErr)
	}
	if quietOut != rdnsDupLines {
		t.Errorf("no -v: stdout:\ngot:\n%s\nexpected:\n%s", quietOut, rdnsDupLines)
	}
	if quietErr != "" {
		t.Errorf("no -v: expected empty stderr, got %q", quietErr)
	}

	tests := []struct {
		flag    string
		expect  []string // substrings expected in stderr
		exclude []string // substrings not expected in stderr
	}{
		{"-v", []string{"INF", "using index", idxpath}, []string{"DBG"}},
		{"-vv", []string{"INF", "using index", idxpath, "DBG", "blockEntryLE"}, nil},
	}
	for _, tc := range tests {
		t.Run(tc.flag, func(t *testing.T) {
			stdout, stderr, code := runBin(t, bsearchBin, tc.flag, "032.176.184.000", infile)
			if code != 0 {
				t.Errorf("expected exit 0, got %d", code)
			}
			if stdout != quietOut {
				t.Errorf("stdout differs from run without -v:\ngot:\n%s\nexpected:\n%s",
					stdout, quietOut)
			}
			for _, s := range tc.expect {
				if !strings.Contains(stderr, s) {
					t.Errorf("expected stderr to contain %q, got %q", s, stderr)
				}
			}
			for _, s := range tc.exclude {
				if strings.Contains(stderr, s) {
					t.Errorf("expected stderr not to contain %q, got %q", s, stderr)
				}
			}
		})
	}
}
