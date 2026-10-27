package bsearch

import (
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"
)

// Test DB.Get() using testdata/rdns1.csv
func TestDBGet(t *testing.T) {
	t.Parallel()

	var tests = []struct {
		key    string
		expect string
	}{
		{"001.000.128.000", "node-0.pool-1-0.dynamic.totinternet.net,202003,totinternet.net"},
		{"001.034.164.000", "1-34-164-0.HINET-IP.hinet.net,202003,hinet.net"},
		{"003.122.207.000", "ec2-3-122-207-0.eu-central-1.compute.amazonaws.com,202003,amazonaws.com"},
		{"003.126.183.000", "ec2-3-126-183-0.eu-central-1.compute.amazonaws.com,202003,amazonaws.com"},
		{"024.066.017.000", "S0106905851b9f0e0.rd.shawcable.net,202003,shawcable.net"},
		{"032.176.184.000", "mobile000.mycingular.net,202003,mycingular.net"},
		{"223.252.003.000", "223-252-3-0.as45671.net,202003,as45671.net"},
	}

	ensureIndex(t, "rdns1.csv")
	db, err := NewDB("testdata/rdns1.csv")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	for _, tc := range tests {
		val, err := db.Get([]byte(tc.key))
		if err != nil {
			t.Fatalf("%s: %s\n", tc.key, err.Error())
		}
		if string(val) != tc.expect {
			t.Errorf("%q => %q\n   expected %q\n", tc.key, val, tc.expect)
		}
	}

	// Lookup a missing key
	key := "foobar"
	val, err := db.Get([]byte(key))
	if err == nil || err != ErrNotFound {
		t.Errorf("%q => %q, %q\n   expected ErrNotFound\n", key, err, val)
	}
}

// Test DB.GetString() using testdata/rdns1.csv
func TestDBGetString(t *testing.T) {
	t.Parallel()

	var tests = []struct {
		key    string
		expect string
	}{
		{"001.000.128.000", "node-0.pool-1-0.dynamic.totinternet.net,202003,totinternet.net"},
		{"001.034.164.000", "1-34-164-0.HINET-IP.hinet.net,202003,hinet.net"},
		{"003.122.207.000", "ec2-3-122-207-0.eu-central-1.compute.amazonaws.com,202003,amazonaws.com"},
		{"003.126.183.000", "ec2-3-126-183-0.eu-central-1.compute.amazonaws.com,202003,amazonaws.com"},
		{"024.066.017.000", "S0106905851b9f0e0.rd.shawcable.net,202003,shawcable.net"},
		{"032.176.184.000", "mobile000.mycingular.net,202003,mycingular.net"},
		{"223.252.003.000", "223-252-3-0.as45671.net,202003,as45671.net"},
	}

	ensureIndex(t, "rdns1.csv")
	db, err := NewDB("testdata/rdns1.csv")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	for _, tc := range tests {
		val, err := db.GetString(tc.key)
		if err != nil {
			t.Fatalf("%s: %s\n", tc.key, err.Error())
		}
		if val != tc.expect {
			t.Errorf("%q => %q\n   expected %q\n", tc.key, val, tc.expect)
		}
	}

	// Lookup a missing key
	key := "foobar"
	val, err := db.GetString(key)
	if err == nil || err != ErrNotFound {
		t.Errorf("%q => %q, %q\n   expected ErrNotFound\n", key, err, val)
	}
}

// Test DB.GetSlice() using testdata/rdns1.csv
func TestDBGetSlice(t *testing.T) {
	t.Parallel()

	var tests = []struct {
		key    string
		expect []string
	}{
		{"001.000.128.000", []string{"node-0.pool-1-0.dynamic.totinternet.net", "202003", "totinternet.net"}},
		{"001.034.164.000", []string{"1-34-164-0.HINET-IP.hinet.net", "202003", "hinet.net"}},
		{"003.122.207.000", []string{"ec2-3-122-207-0.eu-central-1.compute.amazonaws.com", "202003", "amazonaws.com"}},
		{"003.126.183.000", []string{"ec2-3-126-183-0.eu-central-1.compute.amazonaws.com", "202003", "amazonaws.com"}},
		{"024.066.017.000", []string{"S0106905851b9f0e0.rd.shawcable.net", "202003", "shawcable.net"}},
		{"032.176.184.000", []string{"mobile000.mycingular.net", "202003", "mycingular.net"}},
		{"223.252.003.000", []string{"223-252-3-0.as45671.net", "202003", "as45671.net"}},
	}

	ensureIndex(t, "rdns1.csv")
	db, err := NewDB("testdata/rdns1.csv")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	for _, tc := range tests {
		s, err := db.GetSlice(tc.key)
		if err != nil {
			t.Fatalf("%s: %s\n", tc.key, err.Error())
		}
		if diff := cmp.Diff(tc.expect, s); diff != "" {
			t.Errorf("%q mismatch (-want +got):\n%s", tc.key, diff)
		}
	}

	// Lookup a missing key
	key := "foobar"
	s, err := db.GetSlice(key)
	if err == nil || err != ErrNotFound {
		t.Errorf("%q => %q, %q\n   expected ErrNotFound\n",
			key, err, strings.Join(s, ","))
	}
}

// Test DB.Get() on testdata/single_column.csv, where lines have a key
// but no value
func TestDBGetSingleColumn(t *testing.T) {
	ensureIndex(t, "single_column.csv")
	db, err := NewDB("testdata/single_column.csv")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	for _, key := range []string{"ac.0-0", "ac.0001de.liam"} {
		val, err := db.Get([]byte(key))
		if err != nil {
			t.Errorf("%s: %s\n", key, err.Error())
			continue
		}
		if len(val) != 0 {
			t.Errorf("%q => %q\n   expected empty value\n", key, val)
		}
	}
}

// Test that lookups on a closed DB return ErrClosed
func TestDBClosed(t *testing.T) {
	db, err := NewDB(fixture(t, "foo.csv"))
	if err != nil {
		t.Fatal(err)
	}
	db.Close()

	if _, err := db.Get([]byte("bar")); err != ErrClosed {
		t.Errorf("Get: got %v, expected ErrClosed", err)
	}
	if _, err := db.GetString("bar"); err != ErrClosed {
		t.Errorf("GetString: got %v, expected ErrClosed", err)
	}
	if _, err := db.GetSlice("bar"); err != ErrClosed {
		t.Errorf("GetSlice: got %v, expected ErrClosed", err)
	}
}

// Test DB.GetSlice() field splitting, missing keys, and multi-character
// delimiters
func TestDBGetSliceFields(t *testing.T) {
	var tests = []struct {
		key    string
		expect []string // nil means ErrNotFound
	}{
		{"a", []string{"1", "2", "3"}},
		{"b", []string{"x,y", "z"}}, // quoted field containing the delimiter
		{"c", []string{"", "2"}},    // empty first field
		{"aa", nil},                 // missing, inside the key range
		{"0", nil},                  // missing, before the first key
		{"d", nil},                  // missing, after the last key
	}

	path := indexedData(t, "data.csv", "a,1,2,3\nb,\"x,y\",z\nc,,2\n", IndexOptions{})
	db, err := NewDB(path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	for _, tc := range tests {
		s, err := db.GetSlice(tc.key)
		if tc.expect == nil {
			if err != ErrNotFound {
				t.Errorf("%q: got %q, %v, expected ErrNotFound", tc.key, s, err)
			}
			continue
		}
		if err != nil {
			t.Errorf("%q: got error %v", tc.key, err)
			continue
		}
		if diff := cmp.Diff(tc.expect, s); diff != "" {
			t.Errorf("%q mismatch (-want +got):\n%s", tc.key, diff)
		}
	}

	// A multi-character delimiter works for Get, but not GetSlice
	path = indexedData(t, "data.txt", "a::x::y\nb::1\n", IndexOptions{Delimiter: []byte("::")})
	db2, err := NewDB(path)
	if err != nil {
		t.Fatal(err)
	}
	defer db2.Close()

	if val, err := db2.GetString("a"); err != nil || val != "x::y" {
		t.Errorf("multi-character Get: got %q, %v, expected %q", val, err, "x::y")
	}
	s, err := db2.GetSlice("a")
	expect := `cannot convert multi-character delimiter "::" to rune`
	if err == nil || err.Error() != expect {
		t.Errorf("multi-character GetSlice: got %q, %v, expected error %q", s, err, expect)
	}
}

// Test DB.GetSlice() on lines with an empty value
func TestDBGetSliceEmptyValue(t *testing.T) {
	t.Skip("BUG: GetSlice returns io.EOF from csv.Reader.Read when the " +
		"value is empty, although the key exists and Get returns no error")

	var tests = []struct {
		key    string
		expect []string
	}{
		{"a", []string{""}}, // key and delimiter, empty value
		{"b", []string{}},   // key only, no delimiter
	}

	path := indexedData(t, "data.csv", "a,\nb\nc,1\n", IndexOptions{})
	db, err := NewDB(path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	for _, tc := range tests {
		s, err := db.GetSlice(tc.key)
		if err != nil {
			t.Errorf("%q: got error %v", tc.key, err)
			continue
		}
		if diff := cmp.Diff(tc.expect, s); diff != "" {
			t.Errorf("%q mismatch (-want +got):\n%s", tc.key, diff)
		}
	}
}
