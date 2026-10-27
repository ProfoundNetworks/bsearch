package main

import (
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
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
