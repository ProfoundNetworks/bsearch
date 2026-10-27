package bsearch

import (
	"fmt"
	"io"
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
	stat, err := os.Stat(idxpath)
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
	// Index exists - check it is newer than file or recreate
	fe, err := epoch(path)
	if err != nil {
		t.Fatalf("ensureIndex epoch %s: %s", filename, err.Error())
	}
	ie := stat.ModTime().Unix()
	if fe > ie {
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
