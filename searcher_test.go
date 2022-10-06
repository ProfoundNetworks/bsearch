package bsearch

import (
	"fmt"
	"strings"
	"testing"

	//"github.com/rs/zerolog"
	//"github.com/rs/zerolog/log"
	"github.com/stretchr/testify/assert"
)

// Test Searcher.Line() using testdata/rdns1.csv, existing keys
func TestSearcherLine1(t *testing.T) {
	var tests = []struct {
		key    string
		expect string
	}{
		{"001.000.128.000", "001.000.128.000,node-0.pool-1-0.dynamic.totinternet.net,202003,totinternet.net"},
		{"001.034.164.000", "001.034.164.000,1-34-164-0.HINET-IP.hinet.net,202003,hinet.net"},
		{"003.122.207.000", "003.122.207.000,ec2-3-122-207-0.eu-central-1.compute.amazonaws.com,202003,amazonaws.com"},
		{"003.126.183.000", "003.126.183.000,ec2-3-126-183-0.eu-central-1.compute.amazonaws.com,202003,amazonaws.com"},
		{"024.066.017.000", "024.066.017.000,S0106905851b9f0e0.rd.shawcable.net,202003,shawcable.net"},
		{"032.176.184.000", "032.176.184.000,mobile000.mycingular.net,202003,mycingular.net"},
		{"223.252.003.000", "223.252.003.000,223-252-3-0.as45671.net,202003,as45671.net"},
	}

	ensureIndex(t, "rdns1.csv")
	s, err := NewSearcher("testdata/rdns1.csv")
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	for _, tc := range tests {
		line, err := s.Line([]byte(tc.key))
		if err != nil {
			t.Fatalf("%s: %s\n", tc.key, err.Error())
		}
		if string(line) != tc.expect {
			t.Errorf("%q => %q\n   expected %q\n", tc.key, line, tc.expect)
		}
	}
}

// Test Searcher.Line() using testdata/domains1.csv (no header)
func TestSearcherLine2(t *testing.T) {
	var tests = []struct {
		key    string
		expect string
	}{
		{"aaa.com", ""},
		{"accuweather.com", "accuweather.com,567"},
		{"adweek.com", "adweek.com,305"},
		{"evernote.com", "evernote.com,739"},
		{"etracker.com", "etracker.com,477"},
		{"matterport.com", "matterport.com,683"},
		{"openfusion.com.au", ""},
		{"zenfolio.com", "zenfolio.com,416"},
		{"zzz.com", ""},
	}

	ensureIndex(t, "domains1.csv")
	s, err := NewSearcher("testdata/domains1.csv")
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	for _, tc := range tests {
		line, err := s.Line([]byte(tc.key))
		if err != nil {
			if err != ErrNotFound || tc.expect != "" {
				t.Fatalf("%s: %s\n", tc.key, err.Error())
			}
		}
		if string(line) != tc.expect {
			t.Errorf("%q => %q\n   expected %q\n", tc.key, line, tc.expect)
		}
	}
}

// Test Searcher.Line() using testdata/domains2.csv (header)
func TestSearcherLine3(t *testing.T) {
	var tests = []struct {
		key    string
		expect string
	}{
		{"aaa.com", ""},
		{"accuweather.com", "accuweather.com,567"},
		{"adweek.com", "adweek.com,305"},
		{"evernote.com", "evernote.com,739"},
		{"etracker.com", "etracker.com,477"},
		{"matterport.com", "matterport.com,683"},
		{"openfusion.com.au", ""},
		{"zenfolio.com", "zenfolio.com,416"},
		{"zzz.com", ""},
	}

	ensureIndex(t, "domains2.csv")
	o := SearcherOptions{Header: true}
	/*
		zerolog.SetGlobalLevel(zerolog.TraceLevel)
		log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stderr})
		o.Logger = &log.Logger
	*/
	s, err := NewSearcherOptions("testdata/domains2.csv", o)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	for _, tc := range tests {
		line, err := s.Line([]byte(tc.key))
		if err != nil {
			if err != ErrNotFound || tc.expect != "" {
				t.Fatalf("%s: %s\n", tc.key, err.Error())
			}
		}
		assert.Equal(t, tc.expect, string(line), tc.key)
	}
}

// Test Searcher.Line() using testdata/foo.csv (header, duplicate keys)
func TestSearcherLineFoo(t *testing.T) {
	var tests = []struct {
		key    string
		expect string
	}{
		{"bar", "bar,1"},
		{"foo", "foo,2"},
	}

	ensureIndex(t, "foo.csv")
	o := SearcherOptions{Header: true}
	/*
		zerolog.SetGlobalLevel(zerolog.TraceLevel)
		log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stderr})
		o.Logger = &log.Logger
	*/
	s, err := NewSearcherOptions("testdata/foo.csv", o)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	for _, tc := range tests {
		line, err := s.Line([]byte(tc.key))
		assert.Nil(t, err)
		assert.Equal(t, tc.expect, string(line), tc.key)
	}
}

// Test Searcher.Lines() using testdata/alstom1.csv (no header)
func TestSearcherLines1(t *testing.T) {
	var tests = []struct {
		key    string
		expect string
	}{
		{"alstom.com", `alstom.com,alstom.com,SOA
alstom.com,alstom.com,ULT
`},
		{"alstom.com.au", "alstom.com.au,alstom.com,RED\n"},
		{"alstom.com.br", "alstom.com.br,alstom.com,RED\n"},
	}

	ensureIndex(t, "alstom1.csv")
	o := SearcherOptions{Header: false}
	/*
		zerolog.SetGlobalLevel(zerolog.TraceLevel)
		log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stderr})
		o.Logger = &log.Logger
	*/
	s, err := NewSearcherOptions("testdata/alstom1.csv", o)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	for _, tc := range tests {
		lines, err := s.Lines([]byte(tc.key))
		if err != nil {
			if err != ErrNotFound || tc.expect != "" {
				t.Fatalf("%s: %s\n", tc.key, err.Error())
			}
		}
		s := []string{}
		for _, line := range lines {
			s = append(s, string(line))
		}
		linesStr := strings.Join(s, "\n") + "\n"
		if linesStr != tc.expect {
			t.Errorf("%q => %q\n   expected %q\n", tc.key, linesStr, tc.expect)
		}
	}
}

// Test Searcher.Lines() using testdata/alstom2.csv (header)
func TestSearcherLines2(t *testing.T) {
	var tests = []struct {
		key    string
		expect string
	}{
		// alstom.com (includes last line of file)
		{"alstom.com", `alstom.com,alstom.com,SOA
alstom.com,alstom.com,ULT
`},
		{"alstom.com.au", "alstom.com.au,alstom.com,RED\n"},
		{"alstom.com.br", "alstom.com.br,alstom.com,RED\n"},
	}

	ensureIndex(t, "alstom2.csv")
	o := SearcherOptions{Header: true}
	s, err := NewSearcherOptions("testdata/alstom2.csv", o)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	for _, tc := range tests {
		lines, err := s.Lines([]byte(tc.key))
		if err != nil {
			if err != ErrNotFound || tc.expect != "" {
				t.Fatalf("%s: %s\n", tc.key, err.Error())
			}
		}
		s := []string{}
		for _, line := range lines {
			s = append(s, string(line))
		}
		linesStr := strings.Join(s, "\n") + "\n"
		if linesStr != tc.expect {
			t.Errorf("%q => %q\n   expected %q\n", tc.key, linesStr, tc.expect)
		}
	}
}

// Test Searcher.Lines() using testdata/alstom3.csv
// (with header, multiple blocks, next block 1)
func TestSearcherLinesMultiBlock1(t *testing.T) {
	var tests = []struct {
		key        string
		first_line string
		last_line  string
		line_count int
	}{
		{"alstom.com", "alstom.com,first", "alstom.com,last", 438},
	}

	ensureIndex(t, "alstom3.csv")
	o := SearcherOptions{Header: true}
	/*
		zerolog.SetGlobalLevel(zerolog.TraceLevel)
		log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stderr})
		o.Logger = &log.Logger
	*/
	s, err := NewSearcherOptions("testdata/alstom3.csv", o)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	for _, tc := range tests {
		lines, err := s.Lines([]byte(tc.key))
		if err != nil {
			t.Fatalf("%s: %s\n", tc.key, err.Error())
		}
		if len(lines) != tc.line_count {
			t.Errorf("%s: expected %d lines, got %d", tc.key, tc.line_count, len(lines))
		}
		if len(lines) > 0 {
			if string(lines[0]) != tc.first_line {
				t.Errorf("%q => first line %q\n   expected %q\n", tc.key, lines[0], tc.first_line)
			}
			if string(lines[len(lines)-1]) != tc.last_line {
				t.Errorf("%q => last line %q\n   expected %q\n", tc.key, lines[len(lines)-1], tc.last_line)
			}
		}
	}
}

// Test Searcher.Lines() using testdata/alstom3.csv
// (with header, multiple blocks, next block 2)
func TestSearcherLinesMultiBlock2(t *testing.T) {
	var tests = []struct {
		key        string
		first_line string
		last_line  string
	}{
		{"alstom.com", "alstom.com,first", "alstom.com,last"},
	}

	ensureIndex(t, "alstom4.csv")
	o := SearcherOptions{Header: true}
	s, err := NewSearcherOptions("testdata/alstom4.csv", o)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	for _, tc := range tests {
		lines, err := s.Lines([]byte(tc.key))
		if err != nil {
			t.Fatalf("%s: %s\n", tc.key, err.Error())
		}
		if len(lines) <= 2 {
			t.Fatalf("%s: expected N>2 lines, got %d\n", tc.key, len(lines))
		}
		if len(lines) > 0 {
			if string(lines[0]) != tc.first_line {
				t.Errorf("%q => first line %q\n   expected %q\n", tc.key, lines[0], tc.first_line)
			}
			if string(lines[len(lines)-1]) != tc.last_line {
				t.Errorf("%q => last line %q\n   expected %q\n", tc.key, lines[len(lines)-1], tc.last_line)
			}
		}
	}
}

// Test Searcher.Lines() (without header, multiple blocks, starting block 1)
func TestSearcherLinesFoo(t *testing.T) {
	var tests = []struct {
		key        string
		count      int
		first_line string
		last_line  string
	}{
		{"bar", 1, "bar,1", "bar,1"},
		{"foo", 9999, "foo,2", "foo,10000"},
	}

	ensureIndex(t, "foo.csv")
	o := SearcherOptions{Header: false}
	s, err := NewSearcherOptions("testdata/foo.csv", o)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	for _, tc := range tests {
		lines, err := s.Lines([]byte(tc.key))
		assert.Nil(t, err)
		assert.Equal(t, tc.count, len(lines), tc.key+" line count")
		assert.Equal(t, tc.first_line, string(lines[0]), tc.key+" first_line")
		assert.Equal(t, tc.last_line, string(lines[len(lines)-1]),
			tc.key+" last_line")
	}
}

// Benchmark Searcher.Lines()
func BenchmarkSearcherLines(b *testing.B) {
	bss, err := NewSearcher("testdata/rdns1.csv")
	if err != nil {
		b.Fatal(err)
	}
	defer bss.Close()
	prefix := []byte("162.")
	for i := 0; i < b.N; i++ {
		lines, err := bss.Lines(prefix)
		if err != nil {
			b.Fatal(err)
		}
		if len(lines) != 12 {
			b.Fatal(fmt.Errorf("Lines returned %d results, expected 12\n", len(lines)))
		}
	}
}
