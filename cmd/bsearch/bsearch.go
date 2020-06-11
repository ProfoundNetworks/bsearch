// Binary search ordered Filename for lines beginning with SearchString

package main

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"os"
	"strings"

	"github.com/ProfoundNetworks/bsearch"
	"github.com/golang/example/stringutil"
	flags "github.com/jessevdk/go-flags"
)

// Options
var opts struct {
	Verbose []bool `short:"v" long:"verbose" description:"display verbose debug output (repeatable)"`
	//BlockSize int64  `short:"b" long:"bs" description:"blocksize to use for searching (bytes)" default:"4096"`
	Delim string `short:"d" long:"delim" description:"require SearchString to be followed by a delimiter (any char in this string)"`
	Rev   bool   `short:"r" long:"rev" description:"reverse output lines when printing"`
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
	if len(opts.Verbose) >= 1 {
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
	// Mmap input file
	/*
		reader, err := mmap.Open(opts.Args.Filename)
		if err != nil {
			log.Fatal(err)
		}
		defer reader.Close()
	*/

	// Instantiate searcher
	//bso := bsearch.Options{}
	bss, err := bsearch.NewSearcherFile(opts.Args.Filename)
	if err != nil {
		log.Fatal(err)
	}
	reader := bss.Reader()

	// Search
	posn, err := bss.LinePosition([]byte(opts.Args.SearchString))
	if err != nil && err == bsearch.ErrNotFound {
		// Not found
		os.Exit(1)
	} else if err != nil {
		// General error
		log.Fatal(err)
	}
	vprintf("+ bsearch.LinePosition: %d\n", posn)

	p, err := reader.(io.ReadSeeker).Seek(posn, io.SeekStart)
	if err != nil {
		log.Fatal(err)
	}
	if p != posn {
		log.Fatalf("seek returned unexpected position: %d != expected %d\n", p, posn)
	}

	// Scan all lines that match
	scanner := bufio.NewScanner(reader.(io.Reader))
	searchStringLen := len(opts.Args.SearchString)
	for scanner.Scan() {
		line := scanner.Text()
		vprintf("+ line: %s\n", line)
		if strings.HasPrefix(line, opts.Args.SearchString) {
			// If --delim is set, the next character in line after SearchString must be in opts.Delim
			// (or the end of the string)
			if opts.Delim != "" {
				if len(line) > searchStringLen {
					next := line[searchStringLen : searchStringLen+1]
					if !strings.ContainsAny(next, opts.Delim) {
						continue
					}
				}
			}
			if opts.Rev {
				line = stringutil.Reverse(line)
			}
			fmt.Println(line)
		} else if line > opts.Args.SearchString {
			// If line > SearchString, we're done
			break
		}
	}
}
