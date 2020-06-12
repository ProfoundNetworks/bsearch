/*
bsearch_verify is a test utility that does a bsearch line lookup on
every (unique) key in a sorted csv file, and checks that the bsearch
line returned matches the input row.
*/

package main

import (
	"bytes"
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"os"
	"strings"

	"github.com/ProfoundNetworks/bsearch"
	flags "github.com/jessevdk/go-flags"
)

// Options
type Options struct {
	Verbose bool   `short:"v" long:"verbose" description:"display verbose debug output"`
	Sep     string `short:"t" long:"sep"     description:"CSV separator character" default:","`
	Header  bool   `          long:"hdr"     description:"flag indicating CSV file has a header"`
	KeyPosn int    `short:"k" long:"key"     description:"1-based key field position" default:"1"`
	Stdin   bool   `short:"i" long:"stdin"   description:"read csv input from stdin (e.g. to use pv)"`
	MatchLE bool   `short:"l" long:"le"      description:"enable bsearch MatchLE option"`
	Args    struct {
		CSVFile string `description:"CSV input file to be checked"`
	} `positional-args:"yes" required:"yes"`
}

// Disable flags.PrintErrors for more control
var parser = flags.NewParser(&opts, flags.Default&^flags.PrintErrors)
var opts Options

func usage() {
	parser.WriteHelp(os.Stderr)
	os.Exit(2)
}

func vprintf(format string, args ...interface{}) {
	if opts.Verbose {
		fmt.Fprintf(os.Stderr, format, args...)
	}
}

func main() {
	// Parse options
	_, err := parser.Parse()
	if err != nil {
		if flagsErr, ok := err.(*flags.Error); ok && flagsErr.Type != flags.ErrHelp {
			fmt.Fprintf(os.Stderr, "%s\n\n", err)
		}
		usage()
	}

	// Setup
	log.SetFlags(0)
	keyIdx := opts.KeyPosn - 1
	// Setup CSV reader/writer
	var csvfh *os.File
	if opts.Stdin {
		csvfh = os.Stdin
	} else {
		csvfh, err = os.Open(opts.Args.CSVFile)
		if err != nil {
			log.Fatal(err)
		}
		defer csvfh.Close()
	}
	reader := csv.NewReader(csvfh)
	reader.LazyQuotes = true
	// Turn on reader ReuseRecord option to minimise memory allocations
	reader.ReuseRecord = true
	if opts.Sep != "," {
		if opts.Sep == "\\t" {
			opts.Sep = "\t"
		}
		if len([]byte(opts.Sep)) > 1 {
			log.Fatalf("Separator %q is longer than one byte\n", opts.Sep)
		}
		sep_char := []rune(opts.Sep)
		reader.Comma = sep_char[0]
	}
	vprintf("+ opts.MatchLE: %t\n", opts.MatchLE)
	bso := bsearch.Options{MatchLE: opts.MatchLE}
	bss, err := bsearch.NewSearcherFileOptions(opts.Args.CSVFile, bso)
	if err != nil {
		log.Fatal(err)
	}

	// Process
	stats := map[string]int{}
	lc := 1
	for {
		fields, err := reader.Read()
		if err == io.EOF {
			break
		} else if err != nil {
			log.Fatal("reader.Read(): ", err)
		}

		// Skip header
		if opts.Header {
			opts.Header = false
			lc++
			continue
		}

		key := fields[keyIdx]
		if key == "" {
			log.Fatalf("missing key in line %d: %v\n", lc, fields)
		}
		keyPlus := key + opts.Sep
		rest := strings.Join(splice(fields, keyIdx, 1, nil), ",")
		//vprintf("+ %q rest: %s\n", key, rest)

		// Lookup key via bsearch
		row, err := bss.Line([]byte(keyPlus))
		if err != nil && err != bsearch.ErrNotFound {
			log.Fatal(err)
		}
		if err != nil && err == bsearch.ErrNotFound {
			stats["notfound"]++
			continue
		}

		offset := len(keyPlus)
		if bytes.Compare(row[offset:], []byte(rest)) == 0 {
			stats["found"]++
			//vprintf("+ [%8d] ✔ input: %s,%s\n              lookup: %s\n", lc, key, rest, row)
		} else {
			stats["row_mismatch"]++
			vprintf("+ [%8d] ✘ input: %s,%s\n              lookup: %s\n", lc, key, rest, row)
		}

		lc++
	}

	stats["total"] = lc - 1
	fmt.Printf("stats: %v\n", stats)
}

// splice remove n elements from s, starting at i, replacing them with new
func splice(s []string, i, n int, new []string) []string {
	if new != nil {
		log.Fatal("splice `new` not implemented yet")
	}
	copy(s[i:], s[i+n:])
	for j := 1; j <= n; j++ {
		s[len(s)-j] = ""
	}
	s = s[:len(s)-n]
	return s
}
