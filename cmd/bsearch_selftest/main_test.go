package main

import (
	"bytes"
	"fmt"
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
