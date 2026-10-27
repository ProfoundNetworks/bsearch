package bsearch

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"testing"
	"time"
)

// ensureNoIndex removes any existing index, when we don't want to load
func ensureNoIndex(t *testing.T, filename string) {
	t.Helper()
	idxpath, err := IndexPath(filepath.Join("testdata", filename))
	if err != nil {
		t.Fatalf("ensureNoIndex %s: %s\n", filename, err.Error())
	}
	_, err = os.Stat(idxpath)
	if err != nil && !os.IsNotExist(err) {
		t.Fatalf("ensureNoIndex %s: %s\n", filename, err.Error())
	}
	if err == nil {
		// idxpath exists!
		err = os.Remove(idxpath)
		if err != nil && !os.IsNotExist(err) {
			t.Fatalf("removing index %s: %s\n", idxpath, err.Error())
		}
	}
}

// copyFile copies src to dst
func copyFile(t *testing.T, src, dst string) {
	t.Helper()
	in, err := os.Open(src)
	if err != nil {
		t.Fatal(err)
	}
	defer in.Close()
	out, err := os.Create(dst)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := io.Copy(out, in); err != nil {
		out.Close()
		t.Fatal(err)
	}
	if err := out.Close(); err != nil {
		t.Fatal(err)
	}
}

// fixture copies testdata/filename and its checked-in index into a temp
// dir, with the index mtime set after the dataset's so it never looks
// expired, and returns the path of the copied dataset. Use this rather
// than ensureIndex for checked-in indexes, which ensureIndex may rewrite.
func fixture(t *testing.T, filename string) string {
	t.Helper()
	src := filepath.Join("testdata", filename)
	srcIdx, err := IndexPath(src)
	if err != nil {
		t.Fatal(err)
	}

	dir := t.TempDir()
	dst := filepath.Join(dir, filename)
	dstIdx := filepath.Join(dir, filepath.Base(srcIdx))
	copyFile(t, src, dst)
	copyFile(t, srcIdx, dstIdx)

	stat, err := os.Stat(dst)
	if err != nil {
		t.Fatal(err)
	}
	later := stat.ModTime().Add(time.Second)
	if err := os.Chtimes(dstIdx, later, later); err != nil {
		t.Fatal(err)
	}
	return dst
}

// ensureIndex checks that an appropriate index exists for filename and
// creates it if not (see fixture for checked-in indexes)
func ensureIndex(t testing.TB, filename string) {
	t.Helper()
	path := filepath.Join("testdata", filename)
	idxpath, err := IndexPath(path)
	if err != nil {
		t.Fatalf("ensureIndex %s: %s\n", filename, err.Error())
	}
	_, err = os.Stat(idxpath)
	if err != nil && !os.IsNotExist(err) {
		t.Fatalf("ensureIndex %s: %s\n", filename, err.Error())
	}
	if err != nil && os.IsNotExist(err) {
		idx, err := NewIndex(path)
		if err != nil {
			t.Fatalf("ensureIndex NewIndex %s: %s\n", filename, err.Error())
		}
		err = idx.Write()
		if err != nil {
			t.Fatalf("ensureIndex index Write %s: %s\n", filename, err.Error())
		}
		return
	}
	// Index exists - recreate it if LoadIndex finds it stale or invalid
	if _, err := LoadIndex(path); err != nil {
		idx, err := NewIndex(path)
		if err != nil {
			t.Fatalf("ensureIndex NewIndex on %q: %s\n", path, err.Error())
		}
		err = idx.Write()
		if err != nil {
			t.Fatalf("ensureIndex index Write for %q: %s\n", path, err.Error())
		}
	}
}

func TestIndexPath(t *testing.T) {
	var tests = []struct {
		filepath  string
		indexpath string
	}{
		{"bar.csv", "bar_csv.bsy"},
		{"/a/b/c/bar.csv", "/a/b/c/bar_csv.bsy"},
	}
	for _, tc := range tests {
		ipath, err := IndexPath(tc.filepath)
		if err != nil {
			t.Fatal(err)
		}
		if !filepath.IsAbs(ipath) {
			t.Errorf("IndexPath for %q is not absolute: %s",
				tc.filepath, ipath)
		}
		if filepath.IsAbs(tc.indexpath) {
			if ipath != tc.indexpath {
				t.Errorf("IndexPath for %q is %q, want %q",
					tc.filepath, ipath, tc.indexpath)
			}
		} else {
			if !strings.HasSuffix(ipath, "/"+filepath.Base(tc.indexpath)) {
				t.Errorf("IndexPath for %q is %q - does not have expected base %q",
					tc.filepath, ipath, filepath.Base(tc.indexpath))
			}
		}
	}
}

// Test LoadIndex on v4 index files
func TestLoadIndexV4(t *testing.T) {
	var tests = []struct {
		filename     string
		delim        string
		header       bool
		listlen      int
		headerFields []string
	}{
		{"foo.csv", ",", true, 2, []string{"label", "lineno"}},
	}

	for _, tc := range tests {
		idx, err := LoadIndex(fixture(t, tc.filename))
		if err != nil {
			t.Fatalf("%s: %s\n", tc.filename, err.Error())
		}

		if idx.Version != 4 {
			t.Errorf("%s version: got %d, expected %d", tc.filename, idx.Version, 4)
		}
		if idx.Filename != tc.filename {
			t.Errorf("%s filename: got %q, expected %q", tc.filename, idx.Filename, tc.filename)
		}
		if string(idx.Delimiter) != tc.delim {
			t.Errorf("%s delimiter: got %q, expected %q", tc.filename, idx.Delimiter, tc.delim)
		}
		if idx.Header != tc.header {
			t.Errorf("%s header: got %v, expected %v", tc.filename, idx.Header, tc.header)
		}
		if idx.Epoch <= 0 {
			t.Errorf("%s epoch: got %d, expected > 0", tc.filename, idx.Epoch)
		}
		if len(idx.List) != tc.listlen {
			t.Errorf("%s listlen: got %d, expected %d", tc.filename, len(idx.List), tc.listlen)
		}
		if !slices.Equal(idx.HeaderFields, tc.headerFields) {
			t.Errorf("%s headerFields: got %q, expected %q", tc.filename, idx.HeaderFields, tc.headerFields)
		}
	}
}

// Test LoadIndex on v3 index files
func TestLoadIndexV3(t *testing.T) {
	var tests = []struct {
		filename string
		delim    string
		header   bool
		listlen  int
	}{
		{"domains2.csv", ",", true, 1},
		{"foo3.csv", ",", true, 2},
	}

	for _, tc := range tests {
		idx, err := LoadIndex(fixture(t, tc.filename))
		if err != nil {
			t.Fatalf("%s: %s\n", tc.filename, err.Error())
		}

		if idx.Version != 3 {
			t.Errorf("%s version: got %d, expected %d", tc.filename, idx.Version, 3)
		}
		if idx.Filename != tc.filename {
			t.Errorf("%s filename: got %q, expected %q", tc.filename, idx.Filename, tc.filename)
		}
		if string(idx.Delimiter) != tc.delim {
			t.Errorf("%s delimiter: got %q, expected %q", tc.filename, idx.Delimiter, tc.delim)
		}
		if idx.Header != tc.header {
			t.Errorf("%s header: got %v, expected %v", tc.filename, idx.Header, tc.header)
		}
		if idx.Epoch <= 0 {
			t.Errorf("%s epoch: got %d, expected > 0", tc.filename, idx.Epoch)
		}
		if len(idx.List) != tc.listlen {
			t.Errorf("%s listlen: got %d, expected %d", tc.filename, len(idx.List), tc.listlen)
		}
	}
}

// Test NewIndex()
func TestNewIndex(t *testing.T) {
	var tests = []struct {
		filename string
		delim    string
		header   bool
		listlen  int
	}{
		{"indexme.csv", ",", false, 1},
	}

	for _, tc := range tests {
		ensureNoIndex(t, tc.filename)

		// NewIndex()
		idx, err := NewIndex(filepath.Join("testdata", tc.filename))
		if err != nil {
			t.Fatalf("%s: %s\n", tc.filename, err.Error())
		}
		if idx.Filename != tc.filename {
			t.Errorf("%s filename: got %q, expected %q", tc.filename, idx.Filename, tc.filename)
		}
		if string(idx.Delimiter) != tc.delim {
			t.Errorf("%s delimiter: got %q, expected %q", tc.filename, idx.Delimiter, tc.delim)
		}
		if idx.Header != tc.header {
			t.Errorf("%s header: got %v, expected %v", tc.filename, idx.Header, tc.header)
		}
		if idx.Epoch <= 0 {
			t.Errorf("%s epoch: got %d, expected > 0", tc.filename, idx.Epoch)
		}
		if len(idx.List) != tc.listlen {
			t.Errorf("%s listlen: got %d, expected %d", tc.filename, len(idx.List), tc.listlen)
		}
	}
}

// Test NewIndexOptions()
func TestNewIndexOptions(t *testing.T) {
	var tests = []struct {
		filename     string
		delim        string
		header       bool
		listlen      int
		headerFields []string
	}{
		{"indexme.csv", ",", false, 1, []string(nil)},
		{"foo2.csv", ",", true, 2, []string{"label1", "label2, with comma", "lineno"}},
	}

	for _, tc := range tests {
		ensureNoIndex(t, tc.filename)

		o := IndexOptions{Delimiter: []byte(tc.delim), Header: tc.header}
		idx, err := NewIndexOptions(filepath.Join("testdata", tc.filename), o)
		if err != nil {
			t.Fatalf("%s: %s\n", tc.filename, err.Error())
		}
		if idx.Filename != tc.filename {
			t.Errorf("%s filename: got %q, expected %q", tc.filename, idx.Filename, tc.filename)
		}
		if string(idx.Delimiter) != tc.delim {
			t.Errorf("%s delimiter: got %q, expected %q", tc.filename, idx.Delimiter, tc.delim)
		}
		if idx.Header != tc.header {
			t.Errorf("%s header: got %v, expected %v", tc.filename, idx.Header, tc.header)
		}
		if idx.Epoch <= 0 {
			t.Errorf("%s epoch: got %d, expected > 0", tc.filename, idx.Epoch)
		}
		if len(idx.List) != tc.listlen {
			t.Errorf("%s listlen: got %d, expected %d", tc.filename, len(idx.List), tc.listlen)
		}
		if !slices.Equal(idx.HeaderFields, tc.headerFields) {
			t.Errorf("%s headerFields: got %q, expected %q", tc.filename, idx.HeaderFields, tc.headerFields)
		}
	}
}

// Test blockEntryLE() on rir_clc_ipv_range.csv
func TestIndexBlockEntryLE(t *testing.T) {
	var tests = []struct {
		key         string
		entryKey    string
		entryOffset int64
	}{
		{"000.001.000.000", "000.001.000.000", 0},
		{"001.001.000.000", "000.001.000.000", 0},
		{"002.055.255.255", "001.024.000.000", 2079},
		{"002.056.000.000", "002.056.000.000", 4113},
		{"002.057.000.000", "002.056.171.000", 6163},
		{"002.057.084.000", "002.057.084.000", 8213},
		{"223.130.000.000", "223.130.000.000", 6504496},
		{"255.255.255.255", "223.223.000.000", 6506532},
		// Error case - should return ErrIndexEntryNotFound
		{"000.000.000.000", "", -1},
	}

	dataset := "rir_clc_ipv_range.csv"
	ensureIndex(t, dataset)
	idx, err := LoadIndex(filepath.Join("testdata", dataset))
	if err != nil {
		t.Fatalf("%s: %s\n", dataset, err.Error())
	}
	if !idx.KeysIndexFirst {
		t.Errorf("%s KeysIndexFirst: got %v, expected %v", dataset, idx.KeysIndexFirst, true)
	}
	if !idx.KeysUnique {
		t.Errorf("%s KeysUnique: got %v, expected %v", dataset, idx.KeysUnique, true)
	}

	for _, tc := range tests {
		_, entry, err := idx.blockEntryLE([]byte(tc.key))
		if tc.entryKey == "" {
			if err != ErrNotFound {
				t.Errorf("%s returns ErrNotFound: got %v, expected %v", tc.key, err, ErrNotFound)
			}
			continue
		}
		if entry.Key != tc.entryKey {
			t.Errorf("%s entryKey: got %q, expected %q", tc.key, entry.Key, tc.entryKey)
		}
		if entry.Offset != tc.entryOffset {
			t.Errorf("%s entryOffset: got %d, expected %d", tc.key, entry.Offset, tc.entryOffset)
		}
	}
}

// Test that NewIndex rejects compressed datasets
func TestNewIndexCompressed(t *testing.T) {
	dir := t.TempDir()
	for _, ext := range []string{"gz", "bz2", "br", "xz", "zip", "zst"} {
		path := filepath.Join(dir, "data.csv."+ext)
		if err := os.WriteFile(path, []byte("a,1\n"), 0644); err != nil {
			t.Fatal(err)
		}
		if _, err := NewIndex(path); err != ErrFileCompressed {
			t.Errorf("%s: got %v, expected ErrFileCompressed", ext, err)
		}
	}
}

// openFDs returns the number of open file descriptors (Linux only)
func openFDs(t *testing.T) int {
	t.Helper()
	entries, err := os.ReadDir("/proc/self/fd")
	if err != nil {
		t.Fatal(err)
	}
	return len(entries)
}

// Test that NewIndex closes the dataset after reading it
func TestNewIndexClosesFile(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("needs /proc/self/fd")
	}
	path := filepath.Join("testdata", "indexme.csv")

	before := openFDs(t)
	for i := 0; i < 10; i++ {
		if _, err := NewIndex(path); err != nil {
			t.Fatal(err)
		}
	}
	if after := openFDs(t); after != before {
		t.Errorf("open file descriptors went from %d to %d", before, after)
	}
}

// Test that NewIndex rejects out-of-order keys anywhere in the dataset,
// including within the first block (where only an out-of-order second
// line is allowed, as the sign of a header)
func TestNewIndexSortViolation(t *testing.T) {
	var tests = []struct {
		name string
		data string
		opt  IndexOptions
	}{
		{"line 3", "a,1\nc,2\nb,3\n", IndexOptions{}},
		{"line 5", "a,1\nb,2\nc,3\nd,4\nb,5\n", IndexOptions{}},
		{"after header", "key,val\na,1\nc,2\nb,3\n", IndexOptions{}},
		{"after explicit header", "key,val\na,1\nc,2\nb,3\n", IndexOptions{Header: true}},
		{"later in first block", "a,1\n" + strings.Repeat("b,2\n", 100) + "a,3\n", IndexOptions{}},
		{"line 2 with NoHeader", "b,1\na,2\nc,3\n", IndexOptions{NoHeader: true}},
	}

	dir := t.TempDir()
	for i, tc := range tests {
		path := filepath.Join(dir, fmt.Sprintf("data%d.csv", i))
		if err := os.WriteFile(path, []byte(tc.data), 0644); err != nil {
			t.Fatal(err)
		}
		idx, err := NewIndexOptions(path, tc.opt)
		if err == nil || !strings.Contains(err.Error(), "key sort violation") {
			var header []string
			if idx != nil {
				header = idx.HeaderFields
			}
			t.Errorf("%s: got err %v (header %q), expected key sort violation",
				tc.name, err, header)
		}
	}
}

// Test that an out-of-order second line is still detected as a header
func TestNewIndexHeaderDetection(t *testing.T) {
	path := filepath.Join(t.TempDir(), "data.csv")
	if err := os.WriteFile(path, []byte("key,val\na,1\nb,2\nb,3\nc,4\n"), 0644); err != nil {
		t.Fatal(err)
	}
	idx, err := NewIndex(path)
	if err != nil {
		t.Fatal(err)
	}
	if !idx.Header {
		t.Errorf("header: got false, expected true")
	}
	if expect := []string{"key", "val"}; !slices.Equal(idx.HeaderFields, expect) {
		t.Errorf("headerFields: got %q, expected %q", idx.HeaderFields, expect)
	}
	if got := idx.List[0]; got.Key != "a" || got.Offset != 8 {
		t.Errorf("first entry: got %+v, expected {Key:a Offset:8}", got)
	}
}

// Test that NoHeader indexes the first line as data, and that it can't be
// combined with Header
func TestNewIndexNoHeader(t *testing.T) {
	path := filepath.Join(t.TempDir(), "data.csv")
	if err := os.WriteFile(path, []byte("a,1\nb,2\nc,3\n"), 0644); err != nil {
		t.Fatal(err)
	}
	idx, err := NewIndexOptions(path, IndexOptions{NoHeader: true})
	if err != nil {
		t.Fatal(err)
	}
	if idx.Header || idx.HeaderFields != nil {
		t.Errorf("header: got %v %q, expected none", idx.Header, idx.HeaderFields)
	}
	if got := idx.List[0]; got.Key != "a" || got.Offset != 0 {
		t.Errorf("first entry: got %+v, expected {Key:a Offset:0}", got)
	}

	_, err = NewIndexOptions(path, IndexOptions{Header: true, NoHeader: true})
	if !errors.Is(err, ErrHeaderConflict) {
		t.Errorf("Header+NoHeader: got err %v, expected ErrHeaderConflict", err)
	}
}

// Test that header auto-detection is logged at info level
func TestNewIndexHeaderDetectionLogged(t *testing.T) {
	path := filepath.Join(t.TempDir(), "data.csv")
	if err := os.WriteFile(path, []byte("key,val\na,1\nb,2\n"), 0644); err != nil {
		t.Fatal(err)
	}
	var buf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelInfo}))
	if _, err := NewIndexOptions(path, IndexOptions{Logger: logger}); err != nil {
		t.Fatal(err)
	}
	if out := buf.String(); !strings.Contains(out, "header detected") ||
		!strings.Contains(out, "key val") {
		t.Errorf("expected header detection log with fields, got %q", out)
	}
}

// Test that lines longer than the blocksize can be indexed and searched
func TestNewIndexLongLines(t *testing.T) {
	long := strings.Repeat("x", 3000)
	data := "a,1\nb," + long + "\nc,3\nd," + long + long + "\ne,5\n"

	for _, blocksize := range []int{0, 64} { // 0 means the default (2048)
		path := filepath.Join(t.TempDir(), "data.csv")
		if err := os.WriteFile(path, []byte(data), 0644); err != nil {
			t.Fatal(err)
		}
		idx, err := NewIndexOptions(path, IndexOptions{Blocksize: blocksize})
		if err != nil {
			t.Fatalf("blocksize %d: %s", blocksize, err)
		}
		if err := idx.Write(); err != nil {
			t.Fatal(err)
		}

		s, err := NewSearcher(path)
		if err != nil {
			t.Fatal(err)
		}
		expect := map[string]string{"a": "a,1", "b": "b," + long, "c": "c,3",
			"d": "d," + long + long, "e": "e,5"}
		for _, key := range []string{"a", "b", "c", "d", "e"} {
			line, err := s.Line([]byte(key))
			if err != nil {
				t.Errorf("blocksize %d, %s: got error %v", blocksize, key, err)
				continue
			}
			if string(line) != expect[key] {
				t.Errorf("blocksize %d, %s: got %d-byte line, expected %d bytes",
					blocksize, key, len(line), len(expect[key]))
			}
		}
		s.Close()
	}
}

// Test that a line over maxLineLength gives an error naming the line
func TestNewIndexLineTooLong(t *testing.T) {
	saved := maxLineLength
	maxLineLength = 100
	defer func() { maxLineLength = saved }()

	path := filepath.Join(t.TempDir(), "data.csv")
	data := "a,1\nb," + strings.Repeat("x", 200) + "\nc,3\n"
	if err := os.WriteFile(path, []byte(data), 0644); err != nil {
		t.Fatal(err)
	}
	_, err := NewIndex(path)
	if err == nil || !strings.Contains(err.Error(), "line 2 exceeds maximum line length") {
		t.Errorf("got %v, expected a line 2 too-long error", err)
	}
}

// indexedData writes data to filename in a temp dir, builds and writes
// its index using opt, and returns the path of the dataset
func indexedData(t *testing.T, filename, data string, opt IndexOptions) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), filename)
	if err := os.WriteFile(path, []byte(data), 0644); err != nil {
		t.Fatal(err)
	}
	idx, err := NewIndexOptions(path, opt)
	if err != nil {
		t.Fatalf("NewIndexOptions %s: %s", filename, err)
	}
	if err := idx.Write(); err != nil {
		t.Fatalf("Write %s: %s", filename, err)
	}
	return path
}

// readIndex returns the decoded header line and the entry lines of the
// index for path
func readIndex(t *testing.T, path string) (map[string]any, string) {
	t.Helper()
	idxpath, err := IndexPath(path)
	if err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(idxpath)
	if err != nil {
		t.Fatal(err)
	}
	first, body, ok := strings.Cut(string(data), "\n")
	if !ok {
		t.Fatalf("index %s has no header line", idxpath)
	}
	var header map[string]any
	if err := json.Unmarshal([]byte(first), &header); err != nil {
		t.Fatal(err)
	}
	return header, body
}

// writeIndex replaces the index for path with the given header and
// entry lines
func writeIndex(t *testing.T, path string, header map[string]any, body string) {
	t.Helper()
	idxpath, err := IndexPath(path)
	if err != nil {
		t.Fatal(err)
	}
	first, err := json.Marshal(header)
	if err != nil {
		t.Fatal(err)
	}
	data := string(first) + "\n" + body
	if err := os.WriteFile(idxpath, []byte(data), 0644); err != nil {
		t.Fatal(err)
	}
}

// Test that LoadIndex returns ErrIndexNotFound for a dataset with no index
func TestLoadIndexNotFound(t *testing.T) {
	path := filepath.Join(t.TempDir(), "data.csv")
	if err := os.WriteFile(path, []byte("a,1\nb,2\n"), 0644); err != nil {
		t.Fatal(err)
	}
	idx, err := LoadIndex(path)
	if err != ErrIndexNotFound {
		t.Errorf("got %v (index %v), expected ErrIndexNotFound", err, idx)
	}
}

// Test that LoadIndex returns ErrIndexExpired once the dataset is newer
// than its index
func TestLoadIndexExpired(t *testing.T) {
	path := indexedData(t, "data.csv", "a,1\nb,2\n", IndexOptions{})
	idxpath, err := IndexPath(path)
	if err != nil {
		t.Fatal(err)
	}
	stat, err := os.Stat(idxpath)
	if err != nil {
		t.Fatal(err)
	}

	// A dataset with the same mtime as its index is current
	if err := os.Chtimes(path, stat.ModTime(), stat.ModTime()); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadIndex(path); err != nil {
		t.Fatalf("same mtime: got %v, expected nil", err)
	}

	later := stat.ModTime().Add(2 * time.Second)
	if err := os.Chtimes(path, later, later); err != nil {
		t.Fatal(err)
	}
	idx, err := LoadIndex(path)
	if err != ErrIndexExpired {
		t.Errorf("dataset newer: got %v (index %v), expected ErrIndexExpired", err, idx)
	}
}

// Test that LoadIndex returns ErrIndexPathMismatch for a v4 index whose
// stored Filename doesn't match the dataset
func TestLoadIndexPathMismatch(t *testing.T) {
	// Index whose header names a different dataset
	path := indexedData(t, "data.csv", "a,1\nb,2\n", IndexOptions{})
	header, body := readIndex(t, path)
	if header["Filename"] != "data.csv" {
		t.Fatalf("written Filename: got %v, expected %q", header["Filename"], "data.csv")
	}
	header["Filename"] = "other.csv"
	writeIndex(t, path, header, body)
	if idx, err := LoadIndex(path); err != ErrIndexPathMismatch {
		t.Errorf("renamed in header: got %v (index %v), expected ErrIndexPathMismatch", err, idx)
	}

	// data.csv and data_csv share an index file name (data_csv.bsy), so
	// data_csv finds the index written for data.csv
	path = indexedData(t, "data.csv", "a,1\nb,2\n", IndexOptions{})
	sibling := filepath.Join(filepath.Dir(path), "data_csv")
	copyFile(t, path, sibling)
	if idx, err := LoadIndex(sibling); err != ErrIndexPathMismatch {
		t.Errorf("sibling data_csv: got %v (index %v), expected ErrIndexPathMismatch", err, idx)
	}
	// The index is still valid for its own dataset
	if _, err := LoadIndex(path); err != nil {
		t.Errorf("data.csv: got %v, expected nil", err)
	}
}

// Test that LoadIndex returns ErrIndexPathMismatch for a v3 index loaded
// for a dataset other than the one it was built for
func TestLoadIndexPathMismatchV3(t *testing.T) {
	path := fixture(t, "domains2.csv")
	sibling := filepath.Join(filepath.Dir(path), "domains2_csv")
	copyFile(t, path, sibling)
	if idx, err := LoadIndex(sibling); err != ErrIndexPathMismatch {
		t.Errorf("got %v (index %v), expected ErrIndexPathMismatch", err, idx)
	}
}

// Test that LoadIndex rejects malformed index entry lines
func TestLoadIndexMalformed(t *testing.T) {
	var tests = []struct {
		name   string
		length int // header Length
		body   string
		expect string // expected error message, after "malformed index: "
	}{
		{"premature EOF", 3, "0\t\"a\"\n4\t\"b\"\n", "premature EOF on line 3"},
		{"malformed pair", 1, "0 \"a\"\n", `line 1 ("0 \"a\"") contains a malformed pair`},
		{"bad offset", 1, "x\t\"a\"\n", "line 1 contains a bad offset"},
		{"bad key", 1, "0\ta\n", "line 1 contains a bad key"},
		{"bad key on line 2", 2, "0\t\"a\"\n4\t\"b\n", "line 2 contains a bad key"},
	}

	for _, tc := range tests {
		path := indexedData(t, "data.csv", "a,1\nb,2\n", IndexOptions{Blocksize: 4})
		header, _ := readIndex(t, path)
		header["Length"] = tc.length
		writeIndex(t, path, header, tc.body)

		idx, err := LoadIndex(path)
		if err == nil || !strings.HasPrefix(err.Error(), "malformed index: "+tc.expect) {
			t.Errorf("%s: got error %v (index %v), expected \"malformed index: %s\"",
				tc.name, err, idx, tc.expect)
		}
	}
}

// Test that LoadIndex rejects an index without a valid JSON header line
func TestLoadIndexBadHeader(t *testing.T) {
	header := `{"Blocksize":2048,"Delimiter":"LA==","Filename":"data.csv",` +
		`"KeysIndexFirst":true,"Length":1,"Version":4}`
	var tests = []struct {
		name   string
		index  string
		expect string // error prefix
		json   bool   // error should also wrap a *json.SyntaxError
	}{
		{"empty index file", "", "malformed index: empty index file", false},
		{"header without newline", header, "malformed index: premature EOF on line 1", false},
		{"header not JSON", "not json\n0\t\"a\"\n", "malformed index: bad header line: ", true},
		{"header truncated", "{\"Blocksize\":2048,\n0\t\"a\"\n", "malformed index: bad header line: ", true},
	}

	for _, tc := range tests {
		path := filepath.Join(t.TempDir(), "data.csv")
		if err := os.WriteFile(path, []byte("a,1\nb,2\n"), 0644); err != nil {
			t.Fatal(err)
		}
		idxpath, err := IndexPath(path)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(idxpath, []byte(tc.index), 0644); err != nil {
			t.Fatal(err)
		}

		idx, err := LoadIndex(path)
		if err == nil || idx != nil {
			t.Errorf("%s: got index %v, error %v, expected an error", tc.name, idx, err)
			continue
		}
		if !strings.HasPrefix(err.Error(), tc.expect) {
			t.Errorf("%s: got error %q, expected prefix %q", tc.name, err, tc.expect)
		}
		var syntaxErr *json.SyntaxError
		if tc.json && !errors.As(err, &syntaxErr) {
			t.Errorf("%s: got error %v, expected a JSON syntax error", tc.name, err)
		}
	}
}

// Test that NewIndex derives the delimiter from the file extension, and
// that IndexOptions.Delimiter overrides it
func TestNewIndexDelimiter(t *testing.T) {
	var tests = []struct {
		filename string
		data     string
		delim    string // IndexOptions.Delimiter
		expect   string // expected delimiter ("" means ErrUnknownDelimiter)
	}{
		{"data.csv", "a,1\nb,2\n", "", ","},
		{"data.psv", "a|1\nb|2\n", "", "|"},
		{"data.tsv", "a\t1\nb\t2\n", "", "\t"},
		{"data.txt", "a,1\nb,2\n", "", ""},
		{"data.csv.txt", "a,1\nb,2\n", "", ""},
		{"data", "a,1\nb,2\n", "", ""},
		// An explicit delimiter overrides the extension (with ","
		// the first key would be "a|x")
		{"data.csv", "a|x,1\nb|y,2\n", "|", "|"},
		{"data.txt", "a;1\nb;2\n", ";", ";"},
	}

	for _, tc := range tests {
		path := filepath.Join(t.TempDir(), tc.filename)
		if err := os.WriteFile(path, []byte(tc.data), 0644); err != nil {
			t.Fatal(err)
		}
		idx, err := NewIndexOptions(path, IndexOptions{Delimiter: []byte(tc.delim)})
		if tc.expect == "" {
			if !errors.Is(err, ErrUnknownDelimiter) {
				t.Errorf("%s: got error %v, expected ErrUnknownDelimiter", tc.filename, err)
			}
			continue
		}
		if err != nil {
			t.Errorf("%s (delimiter %q): got error %v", tc.filename, tc.delim, err)
			continue
		}
		if string(idx.Delimiter) != tc.expect {
			t.Errorf("%s (delimiter %q): got delimiter %q, expected %q",
				tc.filename, tc.delim, idx.Delimiter, tc.expect)
		}
		if idx.List[0].Key != "a" {
			t.Errorf("%s (delimiter %q): got first key %q, expected %q",
				tc.filename, tc.delim, idx.List[0].Key, "a")
		}
	}
}

// Test that NewIndex returns ErrIndexEmpty for a dataset with no records
func TestNewIndexEmpty(t *testing.T) {
	var tests = []struct {
		name string
		data string
		opt  IndexOptions
	}{
		{"empty", "", IndexOptions{}},
		{"header only", "key,val\n", IndexOptions{Header: true}},
		{"header only, no trailing newline", "key,val", IndexOptions{Header: true}},
	}

	for _, tc := range tests {
		path := filepath.Join(t.TempDir(), "data.csv")
		if err := os.WriteFile(path, []byte(tc.data), 0644); err != nil {
			t.Fatal(err)
		}
		idx, err := NewIndexOptions(path, tc.opt)
		if !errors.Is(err, ErrIndexEmpty) {
			t.Errorf("%s: got error %v (index %v), expected ErrIndexEmpty", tc.name, err, idx)
		}
	}
}

// setMtimes sets path's mtime and that of its index, to the given times
func setMtimes(t *testing.T, path string, data, index time.Time) {
	t.Helper()
	idxpath, err := IndexPath(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Chtimes(path, data, data); err != nil {
		t.Fatal(err)
	}
	if err := os.Chtimes(idxpath, index, index); err != nil {
		t.Fatal(err)
	}
}

// Test LoadIndex staleness detection: sub-second mtimes and dataset size
func TestLoadIndexStaleness(t *testing.T) {
	// A fixed whole second, plus a fraction, so both mtimes share a second
	base := time.Date(2026, 1, 2, 3, 4, 5, 100_000_000, time.UTC)

	// New indexes record the dataset size
	path := indexedData(t, "data.csv", "a,1\nb,2\n", IndexOptions{})
	header, _ := readIndex(t, path)
	if size, ok := header["Size"].(float64); !ok || size != 8 {
		t.Errorf("Size: got %v, expected 8", header["Size"])
	}

	// Dataset modified 1ms after its index, within the same second
	setMtimes(t, path, base.Add(time.Millisecond), base)
	if _, err := LoadIndex(path); err != ErrIndexExpired {
		t.Errorf("dataset 1ms newer: got %v, expected ErrIndexExpired", err)
	}

	// Dataset size changed, but its mtime is older than the index's
	if err := os.WriteFile(path, []byte("a,1\nb,22\n"), 0644); err != nil {
		t.Fatal(err)
	}
	setMtimes(t, path, base.Add(-time.Hour), base)
	if _, err := LoadIndex(path); err != ErrIndexExpired {
		t.Errorf("dataset resized, older mtime: got %v, expected ErrIndexExpired", err)
	}

	// An old index without Size skips the size check
	path = indexedData(t, "data.csv", "a,1\nb,2\n", IndexOptions{})
	header, body := readIndex(t, path)
	delete(header, "Size")
	writeIndex(t, path, header, body)
	if err := os.WriteFile(path, []byte("a,1\nb,22\n"), 0644); err != nil {
		t.Fatal(err)
	}
	setMtimes(t, path, base.Add(-time.Hour), base)
	if _, err := LoadIndex(path); err != nil {
		t.Errorf("index without Size: got %v, expected nil", err)
	}

	// Copying dataset then index elsewhere (new mtimes, same size) is fine
	path = indexedData(t, "data.csv", "a,1\nb,2\n", IndexOptions{})
	setMtimes(t, path, base, base.Add(time.Millisecond))
	if _, err := LoadIndex(path); err != nil {
		t.Errorf("relocated: got %v, expected nil", err)
	}
}

// Test that quotes in a header line, or a header the csv package can't
// parse at all, don't stop the dataset being indexed (header fields are
// informational, so they're parsed leniently)
func TestNewIndexHeaderQuotes(t *testing.T) {
	var tests = []struct {
		name   string
		data   string
		opt    IndexOptions
		expect []string // nil means any fields will do
	}{
		{"bare quote, detected", "z \"x\" y,v\na,1\nb,2\n", IndexOptions{},
			[]string{`z "x" y`, "v"}},
		{"bare quote, explicit", "a \"x\",v\nb,1\nc,2\n", IndexOptions{Header: true},
			[]string{`a "x"`, "v"}},
		{"unterminated quote, explicit", "\"z,v\na,1\nb,2\n", IndexOptions{Header: true}, nil},
		// csv.Reader returns io.EOF for an empty line, so this takes the
		// strings.Split fallback
		{"empty, explicit", "\na,1\nb,2\n", IndexOptions{Header: true}, []string{""}},
	}

	for _, tc := range tests {
		path := filepath.Join(t.TempDir(), "data.csv")
		if err := os.WriteFile(path, []byte(tc.data), 0644); err != nil {
			t.Fatal(err)
		}
		idx, err := NewIndexOptions(path, tc.opt)
		if err != nil {
			t.Errorf("%s: got error %v", tc.name, err)
			continue
		}
		if !idx.Header {
			t.Errorf("%s: header not recorded", tc.name)
		}
		if tc.expect != nil && !slices.Equal(idx.HeaderFields, tc.expect) {
			t.Errorf("%s: headerFields: got %q, expected %q", tc.name, idx.HeaderFields, tc.expect)
		}
	}
}

// Test that Index.Write reports a failure to create the index file
func TestIndexWriteFails(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root ignores directory permissions")
	}
	dir := t.TempDir()
	path := filepath.Join(dir, "data.csv")
	if err := os.WriteFile(path, []byte("a,1\nb,2\n"), 0644); err != nil {
		t.Fatal(err)
	}
	idx, err := NewIndex(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(dir, 0555); err != nil {
		t.Fatal(err)
	}
	defer os.Chmod(dir, 0755)

	if err := idx.Write(); !errors.Is(err, os.ErrPermission) {
		t.Errorf("got %v, expected a permission error", err)
	}
	if _, err := LoadIndex(path); err != ErrIndexNotFound {
		t.Errorf("after failed Write, LoadIndex got %v, expected ErrIndexNotFound", err)
	}
}
