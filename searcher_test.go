package bsearch

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"
	//"log/slog"
	//"os"
	//"github.com/lmittmann/tint"
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

// Test Searcher.Line() using testdata/single_column.csv, where each line
// is just a key with no delimiter (and the last line has no newline)
func TestSearcherLineSingleColumn(t *testing.T) {
	var tests = []struct {
		key    string
		expect string // "" means ErrNotFound
	}{
		{"ac.0-0", "ac.0-0"},
		{"ac.0-0.www", "ac.0-0.www"},
		{"ac.0001de", "ac.0001de"},
		{"ac.0001de.liam", "ac.0001de.liam"}, // final unterminated line
		{"ac.0-", ""},                        // partial keys don't match
		{"ac.0001", ""},
	}

	ensureIndex(t, "single_column.csv")
	s, err := NewSearcher("testdata/single_column.csv")
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	for _, tc := range tests {
		line, err := s.Line([]byte(tc.key))
		if tc.expect == "" {
			if err != ErrNotFound {
				t.Errorf("%q => %q, %v\n   expected ErrNotFound\n", tc.key, line, err)
			}
			continue
		}
		if err != nil {
			t.Errorf("%q: %s\n", tc.key, err.Error())
			continue
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

	path := fixture(t, "domains2.csv")
	o := SearcherOptions{Header: true}
	/*
		o.Logger = slog.New(tint.NewHandler(os.Stderr, &tint.Options{Level: slog.LevelDebug}))
	*/
	s, err := NewSearcherOptions(path, o)
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
			t.Errorf("%s: got %q, expected %q", tc.key, line, tc.expect)
		}
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

	path := fixture(t, "foo.csv")
	o := SearcherOptions{Header: true}
	/*
		o.Logger = slog.New(tint.NewHandler(os.Stderr, &tint.Options{Level: slog.LevelDebug}))
	*/
	s, err := NewSearcherOptions(path, o)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	for _, tc := range tests {
		line, err := s.Line([]byte(tc.key))
		if err != nil {
			t.Errorf("%s: got error %v, expected nil", tc.key, err)
		}
		if string(line) != tc.expect {
			t.Errorf("%s: got %q, expected %q", tc.key, line, tc.expect)
		}
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
		o.Logger = slog.New(tint.NewHandler(os.Stderr, &tint.Options{Level: slog.LevelDebug}))
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
		o.Logger = slog.New(tint.NewHandler(os.Stderr, &tint.Options{Level: slog.LevelDebug}))
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

	path := fixture(t, "foo.csv")
	o := SearcherOptions{Header: false}
	s, err := NewSearcherOptions(path, o)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	for _, tc := range tests {
		lines, err := s.Lines([]byte(tc.key))
		if err != nil {
			t.Fatalf("%s: got error %v, expected nil", tc.key, err)
		}
		if len(lines) != tc.count {
			t.Fatalf("%s line count: got %d, expected %d", tc.key, len(lines), tc.count)
		}
		if string(lines[0]) != tc.first_line {
			t.Errorf("%s first_line: got %q, expected %q", tc.key, lines[0], tc.first_line)
		}
		if string(lines[len(lines)-1]) != tc.last_line {
			t.Errorf("%s last_line: got %q, expected %q", tc.key, lines[len(lines)-1], tc.last_line)
		}
	}
}

// Benchmark Searcher.Lines()
func BenchmarkSearcherLines(b *testing.B) {
	ensureIndex(b, "rdns1.csv")
	bss, err := NewSearcher("testdata/rdns1.csv")
	if err != nil {
		b.Fatal(err)
	}
	defer bss.Close()
	key := []byte("032.176.184.000")
	for i := 0; i < b.N; i++ {
		lines, err := bss.Lines(key)
		if err != nil {
			b.Fatal(err)
		}
		if len(lines) != 6 {
			b.Fatal(fmt.Errorf("Lines returned %d results, expected 6\n", len(lines)))
		}
	}
}

// Test that NewSearcher rejects compressed datasets
func TestNewSearcherCompressed(t *testing.T) {
	dir := t.TempDir()
	for _, ext := range []string{"gz", "bz2", "br", "xz", "zip", "zst"} {
		path := filepath.Join(dir, "data.csv."+ext)
		if err := os.WriteFile(path, []byte("a,1\n"), 0644); err != nil {
			t.Fatal(err)
		}
		s, err := NewSearcher(path)
		if err != ErrFileCompressed {
			if s != nil {
				s.Close()
			}
			t.Errorf("%s: got %v, expected ErrFileCompressed", ext, err)
		}
	}
}

// isMapped reports whether path is currently mmapped by this process
// (Linux only)
func isMapped(t *testing.T, path string) bool {
	t.Helper()
	maps, err := os.ReadFile("/proc/self/maps")
	if err != nil {
		t.Fatal(err)
	}
	abs, err := filepath.Abs(path)
	if err != nil {
		t.Fatal(err)
	}
	return strings.Contains(string(maps), abs)
}

func skipUnlessLinux(t *testing.T) {
	t.Helper()
	if runtime.GOOS != "linux" {
		t.Skip("needs /proc/self")
	}
}

// Test that Searcher.Close unmaps the dataset
func TestSearcherCloseUnmaps(t *testing.T) {
	skipUnlessLinux(t)
	path := fixture(t, "foo.csv")

	s, err := NewSearcher(path)
	if err != nil {
		t.Fatal(err)
	}
	if !isMapped(t, path) {
		t.Fatalf("%s not mapped after NewSearcher", path)
	}
	s.Close()
	if isMapped(t, path) {
		t.Errorf("%s still mapped after Close", path)
	}
}

// Test that DB.Close unmaps the dataset
func TestDBCloseUnmaps(t *testing.T) {
	skipUnlessLinux(t)
	path := fixture(t, "foo.csv")

	db, err := NewDB(path)
	if err != nil {
		t.Fatal(err)
	}
	db.Close()
	if isMapped(t, path) {
		t.Errorf("%s still mapped after Close", path)
	}
}

// Test that a NewSearcher failure (here, a missing index) leaves no open
// file or mapping behind
func TestNewSearcherErrorReleases(t *testing.T) {
	skipUnlessLinux(t)
	path := filepath.Join(t.TempDir(), "foo.csv")
	copyFile(t, filepath.Join("testdata", "foo.csv"), path)

	before := openFDs(t)
	for i := 0; i < 10; i++ {
		if _, err := NewSearcher(path); err != ErrIndexNotFound {
			t.Fatalf("got %v, expected ErrIndexNotFound", err)
		}
	}
	if after := openFDs(t); after != before {
		t.Errorf("open file descriptors went from %d to %d", before, after)
	}
	if isMapped(t, path) {
		t.Errorf("%s still mapped after failed NewSearcher", path)
	}
}

// Test that lookups on a closed Searcher return ErrClosed
func TestSearcherClosed(t *testing.T) {
	s, err := NewSearcher(fixture(t, "foo.csv"))
	if err != nil {
		t.Fatal(err)
	}
	s.Close()
	s.Close() // second Close is a no-op

	key := []byte("bar")
	if _, err := s.Line(key); err != ErrClosed {
		t.Errorf("Line: got %v, expected ErrClosed", err)
	}
	if _, err := s.Lines(key); err != ErrClosed {
		t.Errorf("Lines: got %v, expected ErrClosed", err)
	}
	if _, err := s.LinesN(key, 2); err != ErrClosed {
		t.Errorf("LinesN: got %v, expected ErrClosed", err)
	}
}

// Test that NewSearcher rejects missing paths, directories, and datasets
// without a current index
func TestNewSearcherErrors(t *testing.T) {
	dir := t.TempDir()

	missing := filepath.Join(dir, "missing.csv")

	subdir := filepath.Join(dir, "subdir.csv")
	if err := os.Mkdir(subdir, 0755); err != nil {
		t.Fatal(err)
	}

	noindex := filepath.Join(dir, "noindex.csv")
	if err := os.WriteFile(noindex, []byte("a,1\nb,2\n"), 0644); err != nil {
		t.Fatal(err)
	}

	expired := indexedData(t, "expired.csv", "a,1\nb,2\n", IndexOptions{})
	idxpath, err := IndexPath(expired)
	if err != nil {
		t.Fatal(err)
	}
	stat, err := os.Stat(idxpath)
	if err != nil {
		t.Fatal(err)
	}
	later := stat.ModTime().Add(2 * time.Second)
	if err := os.Chtimes(expired, later, later); err != nil {
		t.Fatal(err)
	}

	var tests = []struct {
		name   string
		path   string
		expect error
	}{
		{"missing", missing, ErrFileNotFound},
		{"directory", subdir, ErrNotFile},
		{"no index", noindex, ErrIndexNotFound},
		{"expired index", expired, ErrIndexExpired},
	}

	for _, tc := range tests {
		s, err := NewSearcher(tc.path)
		if err != tc.expect {
			if s != nil {
				s.Close()
			}
			t.Errorf("%s: got %v, expected %v", tc.name, err, tc.expect)
		}
	}
}

// Test Searcher.Line() on keys at and beyond the ends of a dataset,
// with and without a trailing newline, in one block or many
func TestSearcherLineEdges(t *testing.T) {
	long := strings.Repeat("x", 100)
	var tests = []struct {
		key    string
		expect string // "" means ErrNotFound
	}{
		{"b", "b,1"},       // first key
		{"f", "f,5"},       // last key
		{"d", "d,3"},       // middle key
		{"a", ""},          // before the first key
		{"", ""},           // empty key
		{"g", ""},          // after the last key
		{"ff", ""},         // after the last key, extending it
		{"b" + long, ""},   // longer than any line, inside the key range
		{"z" + long, ""},   // longer than any line, after the last key
		{"f,5" + long, ""}, // longer than any line, extending the last
	}

	for _, data := range []string{"b,1\nc,2\nd,3\ne,4\nf,5\n", "b,1\nc,2\nd,3\ne,4\nf,5"} {
		for _, blocksize := range []int{0, 4} { // 0 means the default (2048)
			path := indexedData(t, "data.csv", data, IndexOptions{Blocksize: blocksize})
			s, err := NewSearcher(path)
			if err != nil {
				t.Fatal(err)
			}
			if blocksize == 4 && len(s.Index.List) != 5 {
				t.Errorf("blocksize 4: got %d index entries, expected 5", len(s.Index.List))
			}
			trailing := strings.HasSuffix(data, "\n")

			for _, tc := range tests {
				line, err := s.Line([]byte(tc.key))
				if tc.expect == "" {
					if err != ErrNotFound {
						t.Errorf("newline %v, blocksize %d, %q: got %q, %v, expected ErrNotFound",
							trailing, blocksize, tc.key, line, err)
					}
					continue
				}
				if err != nil {
					t.Errorf("newline %v, blocksize %d, %q: got error %v",
						trailing, blocksize, tc.key, err)
					continue
				}
				if string(line) != tc.expect {
					t.Errorf("newline %v, blocksize %d, %q: got %q, expected %q",
						trailing, blocksize, tc.key, line, tc.expect)
				}
			}
			s.Close()
		}
	}
}

// Test that a key-only line doesn't hide a following line whose key
// extends it with a byte that sorts below '\n' (e.g. a tab in a CSV key)
func TestSearcherLineKeyBelowNewline(t *testing.T) {
	t.Skip("BUG: scanLinesWithKey compares len(key) bytes from each line, " +
		"running past the end of a shorter key-only line into the next one; " +
		"'\\n' then compares greater than the key's '\\t' and the scan stops early")

	path := indexedData(t, "data.csv", "a\na\tb,1\n", IndexOptions{})
	s, err := NewSearcher(path)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	key := "a\tb"
	line, err := s.Line([]byte(key))
	if err != nil || string(line) != "a\tb,1" {
		t.Errorf("%q: got %q, %v, expected %q", key, line, err, "a\tb,1")
	}
}

// lineNoPanic calls s.Line(key), converting a panic into an error
func lineNoPanic(s *Searcher, key []byte) (line []byte, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("panic: %v", r)
		}
	}()
	return s.Line(key)
}

// Test that a corrupt index that LoadIndex parses gives an error, either
// from NewSearcher or from the lookup, rather than a panic
func TestSearcherCorruptIndex(t *testing.T) {
	t.Skip("BUG: LoadIndex accepts an index with no entries or with offsets " +
		"beyond the dataset, and Line then panics indexing Index.List or " +
		"slicing the mmap")

	var tests = []struct {
		name   string
		length int
		body   string
	}{
		{"no entries", 0, ""},
		{"offset past end of dataset", 1, "1000\t\"a\"\n"},
	}

	for _, tc := range tests {
		path := indexedData(t, "data.csv", "a,1\nb,2\n", IndexOptions{})
		header, _ := readIndex(t, path)
		header["Length"] = tc.length
		writeIndex(t, path, header, tc.body)

		s, err := NewSearcher(path)
		if err != nil {
			continue
		}
		_, err = lineNoPanic(s, []byte("b"))
		if err == nil || strings.HasPrefix(err.Error(), "panic: ") {
			t.Errorf("%s: got error %v from Line, expected an error and no panic",
				tc.name, err)
		}
		s.Close()
	}
}
