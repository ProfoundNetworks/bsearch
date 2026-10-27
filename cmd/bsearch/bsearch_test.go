package main

import (
	"flag"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/ProfoundNetworks/bsearch"
)

var update *bool

// Set by TestMain
var (
	bsearchBin string // freshly built bsearch binary
	tmpdir     string // scratch dir holding bsearchBin and indexed datasets
)

func init() {
	testing.Init()
	update = flag.Bool("u", false, "update .golden files")
	flag.Parse()
}

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
