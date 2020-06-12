package bsearch

import (
	"os"
	"testing"
)

// Test BlockPosition() using testdata/rdns1.csv
func TestBlockPosition1(t *testing.T) {
	var tests = []struct {
		key    string
		expect int64
	}{
		{"000.000.000.000", 0}, // does not exist
		{"001.000.128.000", 0}, // exists, first line
		{"001.034.164.000", 0}, // exists
		{"003.122.206.000", 0}, // does not exist
		{"003.122.207.000", 0}, // exists, first entry of new block (partial line)
		{"003.126.183.000", 4096},
		{"024.066.017.000", 12288}, // exists, first entry of new block, exact block break
		{"032.176.184.000", 20480}, // exists, entry with dups
		{"100.000.000.000", 126976},
		{"200.000.000.000", 221184},
		{"223.252.003.000", 241664},
		{"255.255.255.255", 241664}, // does not exist
	}

	r, l := open(t, "testdata/rdns1.csv")

	s := NewSearcher(r, l)
	defer s.Close() // noop - test

	for _, tc := range tests {
		pos, err := s.BlockPosition([]byte(tc.key))
		if err != nil {
			t.Fatalf("%s: %s\n", tc.key, err.Error())
		}
		if pos != tc.expect {
			t.Errorf("%q: got %d, expected %d\n", tc.key, pos, tc.expect)
		}
	}

}

// Test BlockPosition() using testdata/rdns2.csv
func TestBlockPosition2(t *testing.T) {
	var tests = []struct {
		key    string
		expect int64
	}{
		{"000.000.000.000", 0},    // does not exist
		{"001.000.128.000", 0},    // exists, first line
		{"001.034.164.000", 8192}, // exists
		{"003.122.206.000", 8192}, // does not exist
		{"003.122.207.000", 8192}, // exists
		{"003.126.183.000", 8192},
		{"100.000.000.000", 131072},
		{"200.000.000.000", 229376},
		{"223.252.003.000", 245760},
		{"255.255.255.255", 245760}, // does not exist
	}

	r, l := open(t, "testdata/rdns2.csv")

	o := Options{Blocksize: 8192, Compare: PrefixCompareString}
	s := NewSearcherOptions(r, l, o)
	defer s.Close() // noop - test

	for _, tc := range tests {
		pos, err := s.BlockPosition([]byte(tc.key))
		if err != nil {
			t.Fatalf("%s: %s\n", tc.key, err.Error())
		}
		if pos != tc.expect {
			t.Errorf("%q: got %d, expected %d\n", tc.key, pos, tc.expect)
		}
	}
}

// Test LinePosition() using testdata/rdns1.csv
func TestLinePosition1(t *testing.T) {
	var tests = []struct {
		key    string
		expect int64
	}{
		{"001.000.128.000", 0},      // exists, first line
		{"001.034.164.000", 79},     // exists
		{"003.122.207.000", 4123},   // exists, first entry of new block (partial line)
		{"003.126.183.000", 4211},   // exists
		{"024.066.017.000", 16384},  // exists, first entry of new block, exact block break
		{"032.176.184.000", 20703},  // exists, entry with dups
		{"223.252.003.000", 243896}, // exists
		{"000.000.000.000", -1},     // does not exist
		{"003.122.206.000", -1},     // does not exist
		{"100.000.000.000", -1},     // does not exist
		{"200.000.000.000", -1},     // does not exist
		{"255.255.255.255", -1},     // does not exist
	}

	r, l := open(t, "testdata/rdns1.csv")

	s := NewSearcher(r, l)

	for _, tc := range tests {
		pos, err := s.LinePosition([]byte(tc.key))
		if err != nil && err != ErrNotFound {
			t.Fatalf("%s: %s\n", tc.key, err.Error())
		}
		if pos != tc.expect {
			t.Errorf("%q: got %d, expected %d\n", tc.key, pos, tc.expect)
		}
	}
}

// Test Line() using testdata/rdns1.csv, existing keys
func TestLine1(t *testing.T) {
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

	s, err := NewSearcherFile("testdata/rdns1.csv")
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close() // required for NewSearcherFile

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

// Test with Options.MatchLE set
func TestMatchLE(t *testing.T) {
	var tests = []struct {
		key    string
		expect int64
	}{
		{"000.000.000.000", -1},     // does not exist, before first line
		{"001.000.127.000", -1},     // does not exist, before first line
		{"001.000.127.255", -1},     // does not exist, before first line
		{"001.000.128.000", 0},      // exists, first line
		{"001.034.164.000", 79},     // exists
		{"003.122.206.000", 4033},   // does not exist
		{"003.122.207.000", 4123},   // exists
		{"003.126.183.000", 4211},   // exists
		{"100.000.000.000", 128327}, // does not exist
		{"192.184.097.255", 217624}, // does not exist
		{"192.184.098.000", 217695}, // exists
		{"192.184.098.100", 217695}, // does not exist
		{"200.000.000.000", 221414}, // does not exist
		{"223.252.003.000", 243896}, // exists
		{"223.252.003.001", 243896}, // does not exist
		{"255.255.255.255", 243896}, // does not exist
	}

	o := Options{MatchLE: true}
	s, err := NewSearcherFileOptions("testdata/rdns1.csv", o)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close() // required for NewSearcherFile

	for _, tc := range tests {
		pos, err := s.LinePosition([]byte(tc.key))
		if err != nil && err != ErrNotFound {
			t.Fatalf("%s: %s\n", tc.key, err.Error())
		}
		if pos != tc.expect {
			t.Errorf("%q: got %d, expected %d\n", tc.key, pos, tc.expect)
		}
	}
}

// Helper if not using NewSearcherFile
func open(t *testing.T, filename string) (fh *os.File, length int64) {
	fh, err := os.Open(filename)
	if err != nil {
		t.Fatal(err)
	}

	fileinfo, err := fh.Stat()
	if err != nil {
		t.Fatal(err)
	}

	return fh, fileinfo.Size()
}
