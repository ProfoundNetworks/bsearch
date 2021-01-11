/*
bsearch utility to generate a bsearch index file for a dataset.
The index file has the same name and location as the dataset,
but with a '.bsx' suffix (although it is just a yaml text file).
*/

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
	Verbose bool `short:"v" long:"verbose" description:"display verbose debug output"`
	Force   bool `short:"f" long:"force" description:"force index generation even if up-to-date"`
	Args    struct {
		Filename string
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
	reCompression := regexp.MustCompile(`\.(gz|bz2|zst|br)$`)
	if reCompression.MatchString(opts.Args.Filename) {
		fmt.Fprintf(os.Stderr, "Filename %q appears to be compressed - cannot binary search\n", opts.Args.Filename)
		os.Exit(2)
	}

	// Noop if a valid index already exists (unless --force is specified)
	if !opts.Force {
		_, err = bsearch.NewIndexLoad(opts.Args.Filename)
		if err == nil {
			vprintf("+ index file found and up to date\n")
			os.Exit(0)
		}
	}

	// Generate and write index
	index, err := bsearch.NewIndex(opts.Args.Filename)
	if err != nil {
		log.Fatal(err)
	}
	err = index.Write()
	if err != nil {
		log.Fatal(err)
	}
}
