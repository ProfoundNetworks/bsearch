// Binary search ordered Filename for lines beginning with SearchString

package main

import (
	"fmt"
	"log"
	"os"
	"regexp"

	"github.com/ProfoundNetworks/bsearch"
	flags "github.com/jessevdk/go-flags"
)

// Options
var opts struct {
	Verbose  bool `short:"v" long:"verbose" description:"display verbose debug output"`
	Header   bool `short:"H" long:"hdr" description:"ignore first line (header) in Filename when doing lookups"`
	Rev      bool `short:"r" long:"rev" description:"reverse SearchString for search, and reverse output lines when printing"`
	Boundary bool `short:"b" long:"boundary" description:"require SearchString to be followed by a word boundary (a word-nonword transition)"`
	//Utf8  bool   `short:"u" long:"utf8" description:"use utf8 string comparisons instead of (default) bytewise-compare"`
	Args struct {
		SearchString string
		Filename     string
	} `positional-args:"yes" required:"yes"`
}

// Disable flags.PrintErrors for more control
var parser = flags.NewParser(&opts, flags.Default&^flags.PrintErrors)

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

	// Die if Filename looks compressed
	re := regexp.MustCompile(`\.(gz|bz2|br)$`)
	if re.MatchString(opts.Args.Filename) {
		fmt.Fprintf(os.Stderr, "Filename %q appears to be compressed - cannot binary search\n", opts.Args.Filename)
		os.Exit(2)
	}

	// Instantiate searcher
	o := bsearch.Options{Header: opts.Header, Boundary: opts.Boundary}
	bss, err := bsearch.NewSearcherOptions(opts.Args.Filename, o)
	if err != nil {
		log.Fatal(err)
	}
	if bss.Index != nil {
		vprintf("+ using index %s\n", bsearch.IndexPath(opts.Args.Filename))
	}

	searchStr := opts.Args.SearchString
	if opts.Rev {
		searchStr = reverse(searchStr)
	}

	// Search
	results, err := bss.Lines([]byte(searchStr))
	if err != nil {
		if err == bsearch.ErrCompressedNoIndex {
			log.Fatal("Error: compressed dataset without index - recompress using bsearch_compress.\n")
		}
		log.Fatalf("Error: %s\n", err)
	}
	for _, l := range results {
		var line string
		if opts.Rev {
			line = reverse(string(l))
		} else {
			line = string(l)
		}
		fmt.Println(line)
	}
}

// reverse returns its argument string reversed rune-wise left to right.
func reverse(s string) string {
	r := []rune(s)
	for i, j := 0, len(r)-1; i < len(r)/2; i, j = i+1, j-1 {
		r[i], r[j] = r[j], r[i]
	}
	return string(r)
}
