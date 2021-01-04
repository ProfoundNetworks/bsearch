/*
bsearch selftest utility to load a bsearch dataset and then do
opts.Count random lookups on keys, checking each result.
*/

package main

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"regexp"
	"strings"

	"github.com/ProfoundNetworks/bsearch"
	flags "github.com/jessevdk/go-flags"
)

// Options
var opts struct {
	Verbose bool   `short:"v" long:"verbose" description:"display verbose debug output"`
	Sep     string `short:"t" long:"sep" description:"separator" default:","`
	Count   int    `short:"c" long:"count" description:"number of checks to run" default:"100"`
	Header  bool   `short:"H" long:"hdr" description:"ignore first line (header) in Filename when doing lookups"`
	Args    struct {
		Filename string
	} `positional-args:"yes" required:"yes"`
}

type Entry struct {
	Key string
	Val string
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

	// Instantiate a bsearch.Searcher
	bso := bsearch.Options{Header: opts.Header}
	bss, err := bsearch.NewSearcherFileOptions(opts.Args.Filename, bso)
	if err != nil {
		log.Fatal(err)
	}

	// Load opts.Args.Filename as a CSV map
	cmap := loadCSVMap(opts.Args.Filename, opts.Sep, opts.Header)

	// Run checks, using the fact that `range` returns map entries in a semi-random order
	ok := 0
	fail := 0
	i := 0
	for key, val := range cmap {
		line, err := bss.Line([]byte(key + opts.Sep))
		val2 := ""
		if err == nil {
			val2 = strings.TrimPrefix(string(line), key+opts.Sep)
		}
		vprintf("+ [%d] %q => got %q / exp %q\n", i, key, val2, val)
		if val != val2 {
			fmt.Printf("error: lookup on %q: got %q, expected %q\n", key, val2, val)
			fail++
		} else {
			ok++
		}
		i++
		if opts.Count > 0 && i >= opts.Count {
			break
		}
	}
	if fail > 0 {
		fmt.Printf("%d / %d checks failed, %d / %d check ok\n", fail, fail+ok, ok, fail+ok)
	} else {
		fmt.Printf("%d / %d checks ok\n", ok, ok)
	}
}

// loadCSVMap returns a key=>value map for filename, treating the first CSV field as the key
func loadCSVMap(filename, sep string, header bool) map[string]string {
	cmap := make(map[string]string)
	fh, err := os.Open(filename)
	if err != nil {
		log.Fatal(err)
	}
	scanner := bufio.NewScanner(fh)
	i := 0
	for scanner.Scan() {
		line := scanner.Text()
		if header {
			header = false
			continue
		}
		tokens := strings.Split(line, sep)
		if len(tokens) <= 1 {
			log.Fatalf("Error: too few tokens on line %d: %s\n", i, line)
		}
		cmap[tokens[0]] = strings.Join(tokens[1:], sep)
		i++
	}
	if err := scanner.Err(); err != nil {
		log.Fatal(err)
	}

	return cmap
}