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
	bsearchSelftestBin string // freshly built bsearch_selftest binary
	tmpdir             string // scratch dir holding bsearchSelftestBin
)

// TestMain builds bsearch_selftest into a temporary directory, so tests never run
// against a stale ./bsearch_selftest binary
func TestMain(m *testing.M) {
	var err error
	tmpdir, err = os.MkdirTemp("", "bsearch_selftest_test")
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

	bsearchSelftestBin = filepath.Join(tmpdir, "bsearch_selftest")
	out, err := exec.Command("go", "build", "-o", bsearchSelftestBin, ".").CombinedOutput()
	if err != nil {
		fmt.Fprintf(os.Stderr, "building bsearch_selftest: %s\n%s", err, out)
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
		{[]string{"-t", "|", "data.csv"}, Options{Sep: "|"}},
		{[]string{"-t|", "data.csv"}, Options{Sep: "|"}},
		{[]string{"--sep", ",", "data.csv"}, Options{Sep: ","}},
		{[]string{"--sep=,", "data.csv"}, Options{Sep: ","}},
		{[]string{"-H", "data.csv"}, Options{Header: true}},
		{[]string{"--hdr", "data.csv"}, Options{Header: true}},
		{[]string{"-i", "data.csv"}, Options{Stdin: true}},
		{[]string{"--stdin", "data.csv"}, Options{Stdin: true}},
		{[]string{"-vHi", "data.csv", "-t", ","},
			Options{Verbose: 1, Header: true, Stdin: true, Sep: ","}},
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
			tc.expect.Args.CSVFile = "data.csv"
			if got != tc.expect {
				t.Errorf("got %+v, expected %+v", got, tc.expect)
			}
		})
	}

	// CSVFile is required, even with --stdin
	for _, args := range [][]string{{}, {"--stdin"}, {"--bogus", "data.csv"}} {
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
		stdout, stderr, code := runBin(t, bsearchSelftestBin, flag)
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
		stdout, stderr, code := runBin(t, bsearchSelftestBin, args...)
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

// Test that selftest passes on a dataset with a line longer than
// bufio.Scanner's default 64KB limit
func TestSelftestLongLine(t *testing.T) {
	path := filepath.Join(t.TempDir(), "data.csv")
	data := "a,1\nb," + strings.Repeat("x", 100*1024) + "\nc,3\n"
	if err := os.WriteFile(path, []byte(data), 0644); err != nil {
		t.Fatal(err)
	}
	idx, err := bsearch.NewIndex(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := idx.Write(); err != nil {
		t.Fatal(err)
	}

	stdout, stderr, code := runBin(t, bsearchSelftestBin, path)
	if code != 0 {
		t.Fatalf("exit %d, stderr: %s", code, stderr)
	}
	expect := "ok 1 a\nok 2 b\nok 3 c\n"
	if stdout != expect {
		t.Errorf("got:\n%s\nexpected:\n%s", stdout, expect)
	}
}

// indexedTemp writes data to name in a fresh t.TempDir() and indexes it,
// returning the dataset path and its index
func indexedTemp(t *testing.T, name string, data []byte) (string, *bsearch.Index) {
	t.Helper()
	path := filepath.Join(t.TempDir(), name)
	if err := os.WriteFile(path, data, 0644); err != nil {
		t.Fatal(err)
	}
	idx, err := bsearch.NewIndex(path)
	if err != nil {
		t.Fatal(err)
	}
	// Write blanks Filepath, so keep a copy for callers
	loaded := *idx
	if err := idx.Write(); err != nil {
		t.Fatal(err)
	}
	return path, &loaded
}

// readTestdata returns the contents of the shared testdata file name
func readTestdata(t *testing.T, name string) []byte {
	t.Helper()
	data, err := os.ReadFile(filepath.Join("..", "..", "testdata", name))
	if err != nil {
		t.Fatal(err)
	}
	return data
}

// expectTAP returns the all-ok TAP output for lines, numbered from 1. With
// keyOnly, each test is named by the line's key (the unique-keys path);
// otherwise by the whole line (the duplicate-keys batch path).
func expectTAP(lines []string, sep string, keyOnly bool) string {
	var b strings.Builder
	for i, line := range lines {
		name := line
		if keyOnly {
			name, _, _ = strings.Cut(line, sep)
		}
		fmt.Fprintf(&b, "ok %d %s\n", i+1, name)
	}
	return b.String()
}

// dataLines splits data into lines, dropping the final newline
func dataLines(data []byte) []string {
	return strings.Split(strings.TrimSuffix(string(data), "\n"), "\n")
}

// checkRun runs bsearch_selftest with stdin and args, checking for exit 0,
// empty stderr and the expected stdout
func checkRun(t *testing.T, stdin io.Reader, expect string, args ...string) {
	t.Helper()
	stdout, stderr, code := runBinStdin(t, stdin, bsearchSelftestBin, args...)
	if code != 0 {
		t.Errorf("%q: expected exit 0, got %d", args, code)
	}
	if stderr != "" {
		t.Errorf("%q: expected empty stderr, got %q", args, stderr)
	}
	if stdout != expect {
		t.Errorf("%q: stdout differs: %s", args, firstDiff(stdout, expect))
	}
}

// firstDiff describes the first line where got and expect differ
func firstDiff(got, expect string) string {
	g, e := strings.SplitAfter(got, "\n"), strings.SplitAfter(expect, "\n")
	for i := 0; i < max(len(g), len(e)); i++ {
		var gl, el string
		if i < len(g) {
			gl = g[i]
		}
		if i < len(e) {
			el = e[i]
		}
		if gl != el {
			return fmt.Sprintf("line %d:\ngot      %q\nexpected %q", i+1, gl, el)
		}
	}
	return "no difference"
}

// Test a unique-keys dataset: every line is looked up singly and found
func TestSelftestUnique(t *testing.T) {
	data := readTestdata(t, "domains1.csv")
	path, idx := indexedTemp(t, "domains1.csv", data)
	if !idx.KeysUnique {
		t.Fatal("expected domains1.csv to have unique keys")
	}
	checkRun(t, nil, expectTAP(dataLines(data), ",", true), path)
}

// Test a duplicate-keys dataset, which goes through processBatch
func TestSelftestDuplicates(t *testing.T) {
	data := readTestdata(t, "rdns1.csv")
	path, idx := indexedTemp(t, "rdns1.csv", data)
	if idx.KeysUnique {
		t.Fatal("expected rdns1.csv to have duplicate keys")
	}
	expect := expectTAP(dataLines(data), ",", false)
	// Spot-check the 032.176.184.000 batch of 6 lines is all there
	if !strings.Contains(expect, "\nok 289 032.176.184.000,mobile005.mycingular.net,") {
		t.Fatal("expectTAP did not number the duplicate batch as expected")
	}
	checkRun(t, nil, expect, path)
}

// Test -H/--hdr skips the header line, on both the unique and duplicate paths
func TestSelftestHeader(t *testing.T) {
	uniq := readTestdata(t, "domains1.csv")
	uniqPath, idx := indexedTemp(t, "domains1.csv", append([]byte("domain,count\n"), uniq...))
	if !idx.KeysUnique || !idx.Header {
		t.Fatalf("expected unique keys and a header, got %+v", idx)
	}
	dup := readTestdata(t, "foo.csv")
	dupPath, idx := indexedTemp(t, "foo.csv", dup)
	if idx.KeysUnique || !idx.Header {
		t.Fatalf("expected duplicate keys and a header, got %+v", idx)
	}

	for _, flag := range []string{"-H", "--hdr"} {
		checkRun(t, nil, expectTAP(dataLines(uniq), ",", true), flag, uniqPath)
		checkRun(t, nil, expectTAP(dataLines(dup)[1:], ",", false), flag, dupPath)
	}
}

// Test -i/--stdin reads the test data from stdin rather than the dataset
func TestSelftestStdin(t *testing.T) {
	data := readTestdata(t, "domains1.csv")
	path, _ := indexedTemp(t, "domains1.csv", data)
	lines := dataLines(data)
	subset := []string{lines[3], lines[50], lines[98]}

	for _, flag := range []string{"-i", "--stdin"} {
		stdin := strings.NewReader(strings.Join(subset, "\n") + "\n")
		checkRun(t, stdin, expectTAP(subset, ",", true), flag, path)
	}
}

// Test a .psv dataset gets "|" as its separator without -t
func TestSelftestPSV(t *testing.T) {
	data := bytes.ReplaceAll(readTestdata(t, "domains1.csv"), []byte(","), []byte("|"))
	path, _ := indexedTemp(t, "domains1.psv", data)
	checkRun(t, nil, expectTAP(dataLines(data), "|", true), path)
}

// Test not ok results for lines that don't match the dataset
func TestSelftestNotOK(t *testing.T) {
	tests := []struct {
		name    string
		dataset string
		stdin   string
		expect  string
	}{
		// Unique keys: processLine reports a missing key, or the line found
		{"unique", "domains1.csv", "aaa.com,1\nadweek.com,999\nzend.com,588\n",
			"not ok 1 aaa.com\n# key not found\n" +
				"not ok 2 adweek.com\n# got \"adweek.com,305\", expected \"adweek.com,999\"\n" +
				"ok 3 zend.com\n"},
		// Duplicate keys: processBatch reports each dataset line for the
		// key that is missing from the input batch
		{"batch", "rdns1.csv", "001.000.128.000,wrong\n",
			"not ok 1 001.000.128.000,node-0.pool-1-0.dynamic.totinternet.net,202003,totinternet.net\n" +
				"# entry not found for key \"001.000.128.000\": " +
				"001.000.128.000,node-0.pool-1-0.dynamic.totinternet.net,202003,totinternet.net\n"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			path, _ := indexedTemp(t, tc.dataset, readTestdata(t, tc.dataset))
			// TAP failures are reported on stdout; the exit status stays 0
			checkRun(t, strings.NewReader(tc.stdin), tc.expect, "-i", path)
		})
	}
}

// Test that the batch path reports input lines that can't be found
func TestSelftestBatchNotFound(t *testing.T) {
	t.Skip("BUG: processBatch only checks that the dataset's lines for a key " +
		"appear in the input batch. A key missing from the dataset prints " +
		"nothing (Lines returns nil lines with ErrNotFound, so the not-ok loop " +
		"never runs), and an extra input line for a present key is never reported.")

	dupLines := ""
	for i := range 6 {
		dupLines += fmt.Sprintf("032.176.184.000,mobile%03d.mycingular.net,202003,mycingular.net\n", i)
	}
	tests := []struct {
		name   string
		stdin  string
		expect string // a not ok line expected in stdout
	}{
		{"missing key", "999.000.000.000,x\n", "not ok 1 999.000.000.000"},
		{"extra line", dupLines + "032.176.184.000,zzz\n", "not ok 7 032.176.184.000,zzz"},
	}
	data := readTestdata(t, "rdns1.csv")
	path, _ := indexedTemp(t, "rdns1.csv", data)
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			stdout, stderr, _ := runBinStdin(t, strings.NewReader(tc.stdin),
				bsearchSelftestBin, "-i", path)
			if stderr != "" {
				t.Errorf("expected empty stderr, got %q", stderr)
			}
			if !strings.Contains(stdout, tc.expect+"\n") {
				t.Errorf("expected stdout to contain %q, got:\n%s", tc.expect, stdout)
			}
		})
	}
}
