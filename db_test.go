package bsearch

import (
	"testing"
)

// Test DB.Get() using testdata/rdns1.csv
func TestDBGet(t *testing.T) {
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
		if string(val) != tc.expect {
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
