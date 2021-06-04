package bsearch

import (
	"fmt"
	"strings"
	"testing"
)

// Test BlockPosition() using testdata/rdns1.csv
func TestBlockPosition1(t *testing.T) {
	var tests = []struct {
		key    string
		expect int64
	}{
		{"000.000.000.000", 0},     // does not exist
		{"000.000.127.000", 0},     // does not exist
		{"001.000.128.000", 0},     // exists, first line
		{"001.034.164.000", 0},     // exists
		{"003.122.206.000", 0},     // does not exist
		{"003.114.231.000", 0},     // exists, last entry of first block
		{"003.122.206.000", 0},     // does not exist
		{"003.122.207.000", 0},     // exists, first entry of new block (partial line)
		{"003.126.183.000", 4096},  // exists, second entry of new block
		{"024.066.017.000", 12288}, // exists, first entry of new block, exact block break
		{"032.176.184.000", 20480}, // exists, entry with dups
		{"100.000.000.000", 126976},
		{"200.000.000.000", 221184},
		{"221.094.141.000", 237568}, // exists, first entry of last block
		{"223.252.003.000", 241664}, // exists, last block
		{"255.255.255.255", 241664}, // does not exist
	}

	s, err := NewSearcher("testdata/rdns1.csv")
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

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

	o := Options{Blocksize: 8192, Compare: PrefixCompareString}
	s, err := NewSearcherOptions("testdata/rdns2.csv", o)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

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

// Test BlockPosition() using testdata/rdns1i.csv (with an index)
// (the offsets here are blockposition first-line positions, due to the index)
func TestBlockPosition3(t *testing.T) {
	var tests = []struct {
		key    string
		expect int64
	}{
		{"000.000.000.000", 0},     // does not exist
		{"000.000.127.000", 0},     // does not exist
		{"001.000.128.000", 0},     // exists, first line
		{"001.034.164.000", 0},     // exists
		{"003.122.206.000", 0},     // does not exist
		{"003.114.231.000", 0},     // exists, last entry of first block
		{"003.122.206.000", 0},     // does not exist
		{"003.122.207.000", 0},     // exists, first entry of new block (partial line)
		{"003.126.183.000", 4123},  // exists, second entry of new block
		{"024.066.017.000", 12317}, // exists, first entry of new block, exact block break
		{"032.176.184.000", 20505}, // exists, entry with dups
		{"100.000.000.000", 127050},
		{"200.000.000.000", 221225},
		{"221.094.141.000", 237593}, // exists, first entry of last block
		{"223.252.003.000", 241678}, // exists, last block
		{"255.255.255.255", 241678}, // does not exist
	}

	o := Options{Index: IndexCreate}
	s, err := NewSearcherOptions("testdata/rdns1i.csv", o)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

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

	s, err := NewSearcher("testdata/rdns1.csv")
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

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

// Test Line() using testdata/domain.csv
func TestLine2(t *testing.T) {
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

// Test Line() using testdata/domain.csv
func TestLine3(t *testing.T) {
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

	o := Options{Header: true}
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
		if string(line) != tc.expect {
			t.Errorf("%q => %q\n   expected %q\n", tc.key, line, tc.expect)
		}
	}
}

// Test Line() using testdata/rdns1i.csv, existing keys, with an index
func TestLine4(t *testing.T) {
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

	s, err := NewSearcher("testdata/rdns1i.csv")
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

// Test Lines() using testdata/alstom.csv
func TestLines(t *testing.T) {
	var tests = []struct {
		key    string
		expect string
	}{
		// alstom.com
		{"alstom.com", `alstom.com,alstom.com,SOA
alstom.com,alstom.com,ULT
alstom.com.au,alstom.com,RED
alstom.com.br,alstom.com,RED
`},
		// alstom.com, with delimiter
		{"alstom.com,", `alstom.com,alstom.com,SOA
alstom.com,alstom.com,ULT
`},
		// alstom.co
		{"alstom.co", `alstom.co.th,alstom.com,RED
alstom.com,alstom.com,SOA
alstom.com,alstom.com,ULT
alstom.com.au,alstom.com,RED
alstom.com.br,alstom.com,RED
`},
		// alstom.c (includes first line)
		{"alstom.c", `alstom.ca,alstom.com,RED
alstom.co.th,alstom.com,RED
alstom.com,alstom.com,SOA
alstom.com,alstom.com,ULT
alstom.com.au,alstom.com,RED
alstom.com.br,alstom.com,RED
`},
	}

	o := Options{Header: false}
	s, err := NewSearcherOptions("testdata/alstom.csv", o)
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

// Test Lines() using testdata/alstom2.csv (with header)
func TestLines2(t *testing.T) {
	var tests = []struct {
		key    string
		expect string
	}{
		// alstom.com (includes last line of file)
		{"alstom.com", `alstom.com,alstom.com,SOA
alstom.com,alstom.com,ULT
alstom.com.au,alstom.com,RED
alstom.com.br,alstom.com,RED
`},
		// alstom.com, with delimiter
		{"alstom.com,", `alstom.com,alstom.com,SOA
alstom.com,alstom.com,ULT
`},
		// alstom.co (includes last line of file)
		{"alstom.co", `alstom.co.th,alstom.com,RED
alstom.com,alstom.com,SOA
alstom.com,alstom.com,ULT
alstom.com.au,alstom.com,RED
alstom.com.br,alstom.com,RED
`},
		// alstom.c (includes first line after header, and last line of file)
		{"alstom.c", `alstom.ca,alstom.com,RED
alstom.co.th,alstom.com,RED
alstom.com,alstom.com,SOA
alstom.com,alstom.com,ULT
alstom.com.au,alstom.com,RED
alstom.com.br,alstom.com,RED
`},
	}

	o := Options{Header: true}
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

// Test Lines() using testdata/alstom3.csv (with header, multiple blocks, next block 1)
func TestLinesMultiBlock1(t *testing.T) {
	var tests = []struct {
		key        string
		first_line string
		last_line  string
	}{
		{"alstom.com,", "alstom.com,first", "alstom.com,last"},
	}

	o := Options{Header: true}
	s, err := NewSearcherOptions("testdata/alstom3.csv", o)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	for _, tc := range tests {
		lines, err := s.Lines([]byte(tc.key))
		if err != nil {
			if err != ErrNotFound {
				t.Fatalf("%s: %s\n", tc.key, err.Error())
			}
		}
		//fmt.Println("+ lines:")
		//for _, line := range lines {
		//	fmt.Printf("  %s\n", string(line))
		//}
		if string(lines[0]) != tc.first_line {
			t.Errorf("%q => first line %q\n   expected %q\n", tc.key, lines[0], tc.first_line)
		}
		if string(lines[len(lines)-1]) != tc.last_line {
			t.Errorf("%q => last line %q\n   expected %q\n", tc.key, lines[len(lines)-1], tc.last_line)
		}
	}
}

// Test Lines() using testdata/alstom3.csv (with header, multiple blocks, next block 2)
func TestLinesMultiBlock2(t *testing.T) {
	var tests = []struct {
		key        string
		first_line string
		last_line  string
	}{
		{"alstom.com,", "alstom.com,first", "alstom.com,last"},
	}

	o := Options{Header: true}
	s, err := NewSearcherOptions("testdata/alstom4.csv", o)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	for _, tc := range tests {
		lines, err := s.Lines([]byte(tc.key))
		if err != nil {
			if err != ErrNotFound {
				t.Fatalf("%s: %s\n", tc.key, err.Error())
			}
		}
		if string(lines[0]) != tc.first_line {
			t.Errorf("%q => first line %q\n   expected %q\n", tc.key, lines[0], tc.first_line)
		}
		if string(lines[len(lines)-1]) != tc.last_line {
			t.Errorf("%q => last line %q\n   expected %q\n", tc.key, lines[len(lines)-1], tc.last_line)
		}
	}
}

// Test Lines() (without header, multiple blocks, starting block 1)
func TestLinesMultiBlock3(t *testing.T) {
	var tests = []struct {
		key        string
		first_line string
		last_line  string
	}{
		{"foo,", "foo,1", "foo,10000"},
	}

	o := Options{Header: false}
	s, err := NewSearcherOptions("testdata/foo.csv", o)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	for _, tc := range tests {
		lines, err := s.Lines([]byte(tc.key))
		if err != nil {
			if err != ErrNotFound {
				t.Fatalf("%s: %s\n", tc.key, err.Error())
			}
		}
		if len(lines) == 0 {
			t.Fatalf("%s: no lines returned\n", tc.key)
		}
		if string(lines[0]) != tc.first_line {
			t.Errorf("%q => first line %q\n   expected %q\n", tc.key, lines[0], tc.first_line)
		}
		if string(lines[len(lines)-1]) != tc.last_line {
			t.Errorf("%q => last line %q\n   expected %q\n", tc.key, lines[len(lines)-1], tc.last_line)
		}
	}
}

// Test Lines() with Options.Boundary set (on alstom2.csv)
func TestLinesBoundary(t *testing.T) {
	var tests = []struct {
		key    string
		expect string
	}{
		// alstom.com (includes last line of file)
		{"alstom.com", `alstom.com,alstom.com,SOA
alstom.com,alstom.com,ULT
alstom.com.au,alstom.com,RED
alstom.com.br,alstom.com,RED
`},
		// alstom.co with boundary returns only one line
		{"alstom.co", `alstom.co.th,alstom.com,RED
`},
		// alstom.c with boundary returns nothing
		{"alstom.c", ""},
	}

	o := Options{Header: true, Boundary: true}
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
		var linesStr string
		if len(lines) > 0 {
			s := []string{}
			for _, line := range lines {
				s = append(s, string(line))
			}
			linesStr = strings.Join(s, "\n") + "\n"
		}
		if linesStr != tc.expect {
			t.Errorf("%q => %q\n   expected %q\n", tc.key, linesStr, tc.expect)
		}
	}
}

// Test Lines() with Options.Boundary set (on ca_rev.txt)
func TestLinesBoundary2(t *testing.T) {
	var tests = []struct {
		key    string
		expect string
	}{
		{"ac.101gnitekrametailiffa", `ac.101gnitekrametailiffa
ac.101gnitekrametailiffa.ksidbew
ac.101gnitekrametailiffa.lenapc
ac.101gnitekrametailiffa.liambew
ac.101gnitekrametailiffa.revocsidotua
ac.101gnitekrametailiffa.sradnelacpc
ac.101gnitekrametailiffa.stcatnocpc
`},
	}

	o := Options{Header: true, Boundary: true}
	s, err := NewSearcherOptions("testdata/ca_rev.txt", o)
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
		var linesStr string
		if len(lines) > 0 {
			s := []string{}
			for _, line := range lines {
				s = append(s, string(line))
			}
			linesStr = strings.Join(s, "\n") + "\n"
		}
		if linesStr != tc.expect {
			t.Errorf("%q => %q\n   expected %q\n", tc.key, linesStr, tc.expect)
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
	s, err := NewSearcherOptions("testdata/rdns1.csv", o)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

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

// Benchmark Lines()
func BenchmarkLines(b *testing.B) {
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

// Benchmark LinesViaScanner()
func BenchmarkLinesViaScanner(b *testing.B) {
	bss, err := NewSearcher("testdata/rdns1.csv")
	if err != nil {
		b.Fatal(err)
	}
	defer bss.Close()
	prefix := []byte("162.")
	for i := 0; i < b.N; i++ {
		lines, err := bss.linesViaScanner(prefix)
		if err != nil {
			b.Fatal(err)
		}
		if len(lines) != 12 {
			b.Fatal(fmt.Errorf("Lines returned %d results, expected 12\n", len(lines)))
		}
	}
}
