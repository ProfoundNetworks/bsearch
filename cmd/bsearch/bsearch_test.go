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
// index alongside it, returning the path of the copy
func indexedCopy(t *testing.T, path string) string {
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

	index, err := bsearch.NewIndex(dst)
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

	infile := indexedCopy(t, filepath.Join("..", "..", "testdata", "rdns1.csv"))

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

/*
// FIXME: these are non-terminated text files - revisit
func TestRev(t *testing.T) {
	var tests = []struct {
		name   string
		args   string
		search string
		expect string
	}{
		{"0-0.ca", "0-0.ca", "", `0-0.ca
www.0-0.ca
tennis40-0.ca
webdisk.tennis40-0.ca
cpanel.tennis40-0.ca
webmail.tennis40-0.ca
tennis40-0.com.tennis40-0.ca
www.tennis40-0.com.tennis40-0.ca
cpcalendars.tennis40-0.ca
cpcontacts.tennis40-0.ca
tennis40-0.net.tennis40-0.ca
www.tennis40-0.net.tennis40-0.ca`},
		{"3-2-1-0.ca", "3-2-1-0.ca", "", "3-2-1-0.ca"},
		{"3-2-1-0.ca -b", "3-2-1-0.ca", "-b", "3-2-1-0.ca"},
		{"card1000.ca", "card1000.ca", "", "card1000.ca"},
		{"ed1000.ca", "ed1000.ca", "",
			`ed1000.ca
dc-e362ba236494.ed1000.ca
mail.ed1000.ca
www.ed1000.ca
www.fooed1000.ca`},
		{"ed1000.ca -b", "ed1000.ca", "-b",
			`ed1000.ca
dc-e362ba236494.ed1000.ca
mail.ed1000.ca
www.ed1000.ca`},
	}

	infile := filepath.Join("testdata", "ca_rev.txt")

	for _, tc := range tests {
		cmd := bsearchBin + " -r " + tc.args + " " + tc.search + " " + infile

		output, err := exec.Command("bash", "-c", cmd).CombinedOutput()
		got := strings.TrimSpace(string(output))
		if err != nil {
			t.Fatalf("%s: %s", err.Error(), got)
		}

		if got != tc.expect {
			t.Errorf("test %q arg test failed:\n\ngot:\n%s\n\nexpected:\n%s\n", tc.name, got, tc.expect)
		}
	}
}

func TestRevHeader(t *testing.T) {
	var tests = []struct {
		name   string
		args   string
		search string
		expect string
	}{
		{"0-0.ca", "0-0.ca", "", `0-0.ca
www.0-0.ca
tennis40-0.ca
webdisk.tennis40-0.ca
cpanel.tennis40-0.ca
webmail.tennis40-0.ca
tennis40-0.com.tennis40-0.ca
www.tennis40-0.com.tennis40-0.ca
cpcalendars.tennis40-0.ca
cpcontacts.tennis40-0.ca
tennis40-0.net.tennis40-0.ca
www.tennis40-0.net.tennis40-0.ca`},
		{"3-2-1-0.ca", "3-2-1-0.ca", "", "3-2-1-0.ca"},
		{"3-2-1-0.ca -b", "3-2-1-0.ca", "-b", "3-2-1-0.ca"},
		{"card1000.ca", "card1000.ca", "", "card1000.ca"},
		{"ed1000.ca", "ed1000.ca", "",
			`ed1000.ca
dc-e362ba236494.ed1000.ca
mail.ed1000.ca
www.ed1000.ca
www.fooed1000.ca`},
		{"ed1000.ca -b", "ed1000.ca", "-b",
			`ed1000.ca
dc-e362ba236494.ed1000.ca
mail.ed1000.ca
www.ed1000.ca`},
	}

	infile := filepath.Join("testdata", "ca_rev_hdr.txt")

	for _, tc := range tests {
		cmd := bsearchBin + " -r --hdr " + tc.args + " " + tc.search + " " + infile

		output, err := exec.Command("bash", "-c", cmd).CombinedOutput()
		got := strings.TrimSpace(string(output))
		if err != nil {
			t.Fatalf("%s: %s", err.Error(), got)
		}

		if got != tc.expect {
			t.Errorf("test %q arg test failed:\n\ngot:\n%s\n\nexpected:\n%s\n", tc.name, got, tc.expect)
		}
	}
}
*/
