/*
bsearch utility to generate a bsearch index file for a dataset.

The index file is a zstd-compressed yaml file. It has the same name and
location as the dataset, but with all '.' characters changed to '_', and
a '.bsx' suffix e.g. the index for `test_foobar.csv` is `test_foobar_csv.bsx`.
*/

package main

import (
	"fmt"
	"log"
	"os"
	"regexp"

	"github.com/ProfoundNetworks/bsearch"
	flags "github.com/jessevdk/go-flags"
	yaml "gopkg.in/yaml.v3"
)

// Options
var opts struct {
	Verbose bool   `short:"v" long:"verbose" description:"display verbose debug output"`
	Delim   string `short:"t" long:"sep" description:"separator/delimiter character"`
	Header  bool   `long:"hdr" description:"Filename includes a header, which should be skipped (usually optional)"`
	Force   bool   `short:"f" long:"force" description:"force index generation even if up-to-date"`
	Cat     bool   `short:"c" long:"cat" description:"write generated index to stdout instead of to file"`
	Args    struct {
		Filename string
	} `positional-args:"yes" required:"yes"`
}

func vprintf(format string, args ...interface{}) {
	if opts.Verbose {
		fmt.Fprintf(os.Stderr, format, args...)
	}
}

func main() {
	// Parse default options are HelpFlag | PrintErrors | PassDoubleDash
	parser := flags.NewParser(&opts, flags.Default)
	_, err := parser.Parse()
	if err != nil {
		if flags.WroteHelp(err) {
			os.Exit(0)
		}

		// Does PrintErrors work? Is it not set?
		fmt.Fprintln(os.Stderr, "")
		parser.WriteHelp(os.Stderr)
		os.Exit(2)
	}

	// Setup
	log.SetFlags(0)

	// Die if Filename looks compressed
	reCompression := regexp.MustCompile(`\.(gz|bz2|br)$`)
	if reCompression.MatchString(opts.Args.Filename) {
		fmt.Fprintf(os.Stderr, "Filename %q appears to be compressed - cannot binary search\n",
			opts.Args.Filename)
		os.Exit(2)
	}
	reZstd := regexp.MustCompile(`\.zst$`)
	if reZstd.MatchString(opts.Args.Filename) {
		fmt.Fprintf(os.Stderr, "Cannot create index on zstd dataset %q - recompress with bsearch_compress instead\n",
			opts.Args.Filename)
		os.Exit(2)
	}

	// Noop if a valid index already exists (unless --force is specified)
	if !opts.Force && !opts.Cat {
		_, err = bsearch.LoadIndex(opts.Args.Filename)
		if err == nil {
			vprintf("+ index file found and up to date\n")
			os.Exit(0)
		}
	}

	// Generate and write index
	idxopt := bsearch.IndexOptions{Delimiter: []byte(opts.Delim)}
	if opts.Header {
		idxopt.Header = true
	}
	index, err := bsearch.NewIndexOptions(opts.Args.Filename, idxopt)
	if err != nil {
		log.Fatal(err)
	}

	// Output to stdout if --cat specified
	if opts.Cat {
		data, err := yaml.Marshal(index)
		if err != nil {
			log.Fatal(err)
		}
		fmt.Print(string(data))
		os.Exit(0)
	}

	// Write index to file
	err = index.Write()
	if err != nil {
		log.Fatal(err)
	}
}
