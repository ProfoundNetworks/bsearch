// Binary search ordered Filename for lines beginning with SearchString

package main

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"os"
	"regexp"
	"strings"

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
	re := regexp.MustCompile(`\.(gz|bz2|zst|br)$`)
	if re.MatchString(opts.Args.Filename) {
		fmt.Fprintf(os.Stderr, "Filename %q appears to be compressed - cannot binary search\n", opts.Args.Filename)
		os.Exit(2)
	}

	// Instantiate searcher
	o := bsearch.Options{Header: opts.Header, Boundary: opts.Boundary}
	bss, err := bsearch.NewSearcherFileOptions(opts.Args.Filename, o)
	if err != nil {
		log.Fatal(err)
	}

	// Search
	searchStr := opts.Args.SearchString
	if opts.Rev {
		searchStr = reverse(searchStr)
	}
	//vprintf("+ searchStr: %q\n", searchStr)

	/*
		posn, err := bss.LinePosition([]byte(searchStr))
		if err != nil && err == bsearch.ErrNotFound {
			// Not found
			if opts.Verbose {
				log.Println("Not found")
			}
			os.Exit(1)
		} else if err != nil {
			// General error
			log.Fatal(err)
		}
		//vprintf("+ bsearch.LinePosition: %d\n", posn)

		reader := bss.Reader()
		results := getLinesViaScanner(reader, posn, searchStr)
	*/

	results, err := bss.Lines([]byte(searchStr))
	if err != nil {
		log.Fatal(err)
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

func getLinesViaScanner(reader io.ReaderAt, posn int64, searchStr string) []string {
	p, err := reader.(io.ReadSeeker).Seek(posn, io.SeekStart)
	if err != nil {
		log.Fatal(err)
	}
	if p != posn {
		log.Fatalf("seek returned unexpected position: %d != expected %d\n", p, posn)
	}

	// Scan all lines that match
	scanner := bufio.NewScanner(reader.(io.Reader))
	//searchStringLen := len(searchStr)
	var results []string
	for scanner.Scan() {
		line := scanner.Text()
		//vprintf("+ line: %s\n", line)
		if strings.HasPrefix(line, searchStr) {
			// If --delim is set, the next character in line after searchStr must be in opts.Delim
			// (or the end of the string)
			/*
				if opts.Delim != "" {
					if len(line) > searchStringLen {
						next := line[searchStringLen : searchStringLen+1]
						if !strings.ContainsAny(next, opts.Delim) {
							continue
						}
					}
				}
			*/
			if opts.Rev {
				line = reverse(line)
			}
			results = append(results, line)
		} else if line > searchStr {
			// If line > searchStr we're done
			break
		}
	}

	return results
}

// reverse returns its argument string reversed rune-wise left to right.
func reverse(s string) string {
	r := []rune(s)
	for i, j := 0, len(r)-1; i < len(r)/2; i, j = i+1, j-1 {
		r[i], r[j] = r[j], r[i]
	}
	return string(r)
}
