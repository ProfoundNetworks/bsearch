/*
bsearch utility to generate a bsearch index file for a dataset.

The index file is a zstd-compressed yaml file. It has the same name and
location as the dataset, but with all '.' characters changed to '_', and
a '.bsx' suffix e.g. the index for `test_foobar.csv` is `test_foobar_csv.bsx`.
*/

package main

import (
	"fmt"
	"os"
	"regexp"

	"log/slog"

	"github.com/ProfoundNetworks/bsearch"
	flags "github.com/jessevdk/go-flags"
	"github.com/lmittmann/tint"
	yaml "gopkg.in/yaml.v3"
)

// Options
var opts struct {
	Verbose   []bool `short:"v" long:"verbose" description:"display verbose debug output"`
	Delim     string `short:"t" long:"sep" description:"separator/delimiter character"`
	Header    bool   `long:"hdr" description:"Filename includes a header, which should be skipped (usually optional)"`
	Force     bool   `short:"f" long:"force" description:"force index generation even if up-to-date"`
	Cat       bool   `short:"c" long:"cat" description:"write generated index to stdout instead of to file"`
	Blocksize int    `short:"b" long:"bs" description:"index blocksize (kB, default 2kB)"`
	Args      struct {
		Filename string
	} `positional-args:"yes" required:"yes"`
}

func die(msg string) {
	fmt.Fprintln(os.Stderr, msg)
	os.Exit(1)
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
	setupLogging(len(opts.Verbose))

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
			slog.Info("index file found and up to date")
			os.Exit(0)
		}
	}

	// Generate and write index
	idxopt := bsearch.IndexOptions{Delimiter: []byte(opts.Delim)}
	if opts.Header {
		idxopt.Header = true
	}
	if len(opts.Verbose) > 0 {
		idxopt.Logger = slog.Default()
	}
	if opts.Blocksize > 0 {
		idxopt.Blocksize = opts.Blocksize * 1024
	}
	index, err := bsearch.NewIndexOptions(opts.Args.Filename, idxopt)
	if err != nil {
		die(err.Error())
	}

	// Output to stdout if --cat specified
	if opts.Cat {
		data, err := yaml.Marshal(index)
		if err != nil {
			die(err.Error())
		}
		fmt.Print(string(data))
		os.Exit(0)
	}

	// Write index to file
	err = index.Write()
	if err != nil {
		die(err.Error())
	}
}

// setupLogging configures the default slog logger to write human-readable
// output to stderr, with the level set from the number of -v flags given.
func setupLogging(verbosity int) {
	level := slog.LevelWarn
	switch verbosity {
	case 0:
		level = slog.LevelWarn
	case 1:
		level = slog.LevelInfo
	default:
		level = slog.LevelDebug
	}
	slog.SetDefault(slog.New(tint.NewHandler(os.Stderr, &tint.Options{
		Level:      level,
		TimeFormat: "15:04:05.000",
	})))
}
