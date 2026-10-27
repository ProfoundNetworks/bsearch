/*
bsearch utility to generate a bsearch index file for a dataset.

The index file is an uncompressed json+tsv file (a JSON metadata line,
then one offset<TAB>key line per block). It has the same name and location
as the dataset, but with all '.' characters changed to '_', and a '.bsy'
suffix e.g. the index for `test_foobar.csv` is `test_foobar_csv.bsy`.
With --cat, the index is written to stdout as YAML instead.
*/

package main

import (
	"errors"
	"fmt"
	"os"

	"log/slog"

	"github.com/ProfoundNetworks/bsearch"
	"github.com/alecthomas/kong"
	helpcolours "github.com/gavincarr/kong-help-colours"
	"github.com/lmittmann/tint"
	"golang.org/x/term"
	yaml "gopkg.in/yaml.v3"
)

// Options. Every field carries an explicit name: tag, so kong never derives
// a flag name from the field name. Verbose is a counter rather than go-flags'
// []bool idiom, which makes kong hang.
type Options struct {
	Verbose   int    `short:"v" name:"verbose" type:"counter" help:"display verbose debug output"`
	Delim     string `short:"t" name:"sep" help:"separator/delimiter character"`
	Header    bool   `name:"hdr" xor:"hdr" help:"Filename includes a header, which should be skipped (usually optional)"`
	NoHeader  bool   `name:"no-hdr" xor:"hdr" help:"Filename has no header, so an out-of-order second line is an error"`
	Force     bool   `short:"f" name:"force" help:"force index generation even if up-to-date"`
	Cat       bool   `short:"c" name:"cat" help:"write generated index to stdout instead of to file"`
	Blocksize int    `short:"b" name:"bs" help:"index blocksize (kB, default 2kB)"`
	Args      struct {
		Filename string `arg:"" name:"Filename"`
	} `embed:""`
}

var opts Options

// newParser builds the kong parser for bsearch_index over o.
func newParser(o *Options) (*kong.Kong, error) {
	return kong.New(o,
		kong.Name("bsearch_index"),
		kong.Description("Generate a bsearch index file for a dataset."),
		kong.UsageOnError(),
		kong.Help(helpcolours.Help),
		kong.Exit(exitOnUsageError),
	)
}

// exitOnUsageError maps kong's usage-error status (80) onto 2. The code != 0
// guard is load-bearing: kong routes --help through here with 0.
func exitOnUsageError(code int) {
	if code != 0 {
		code = 2
	}
	os.Exit(code)
}

func die(msg string) {
	fmt.Fprintln(os.Stderr, msg)
	os.Exit(1)
}

func main() {
	// Parse options
	parser, err := newParser(&opts)
	if err != nil {
		fmt.Fprintln(os.Stderr, "Error: "+err.Error())
		os.Exit(2)
	}
	if _, err := parser.Parse(os.Args[1:]); err != nil {
		// kong's UsageOnError writes usage to parser.Stdout; keep it on
		// stderr. --help exits inside Parse, so it still goes to stdout.
		parser.Stdout = os.Stderr
		parser.FatalIfErrorf(err)
	}

	// Setup
	setupLogging(opts.Verbose)

	// Noop if a valid index already exists (unless --force is specified,
	// or explicit options disagree with it)
	if !opts.Force && !opts.Cat {
		index, err := bsearch.LoadIndex(opts.Args.Filename)
		if err == nil {
			diffs := optionsDiffer(index, opts)
			if len(diffs) == 0 {
				slog.Info("index file found and up to date")
				os.Exit(0)
			}
			slog.Info("index options differ, regenerating", "options", diffs)
		}
	}

	// Generate and write index
	idxopt := bsearch.IndexOptions{
		Delimiter: []byte(opts.Delim),
		Header:    opts.Header,
		NoHeader:  opts.NoHeader,
	}
	if opts.Verbose > 0 {
		idxopt.Logger = slog.Default()
	}
	if opts.Blocksize > 0 {
		idxopt.Blocksize = opts.Blocksize * 1024
	}
	index, err := bsearch.NewIndexOptions(opts.Args.Filename, idxopt)
	if errors.Is(err, bsearch.ErrFileCompressed) {
		fmt.Fprintf(os.Stderr, "Filename %q appears to be compressed - cannot binary search\n",
			opts.Args.Filename)
		os.Exit(2)
	} else if err != nil {
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

// optionsDiffer returns the names of the options explicitly set in o that
// disagree with index. Options left unset never count as differing.
func optionsDiffer(index *bsearch.Index, o Options) []string {
	var diffs []string
	if o.Header && !index.Header {
		diffs = append(diffs, "hdr")
	}
	if o.NoHeader && index.Header {
		diffs = append(diffs, "no-hdr")
	}
	if o.Delim != "" && o.Delim != string(index.Delimiter) {
		diffs = append(diffs, "sep")
	}
	if o.Blocksize > 0 && o.Blocksize*1024 != index.Blocksize {
		diffs = append(diffs, "bs")
	}
	return diffs
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
	// Colour only for a terminal, so redirected logs stay plain text
	noColour := os.Getenv("NO_COLOR") != "" || !term.IsTerminal(int(os.Stderr.Fd()))
	slog.SetDefault(slog.New(tint.NewTextHandler(os.Stderr, &tint.Options{
		Level:      level,
		TimeFormat: "15:04:05.000",
		NoColor:    noColour,
	})))
}
