/*
bsearch_selftest checks that every line of a dataset (or of test data on
stdin) can be found in it by key. It reports each row that can't be found,
then a summary, and exits 1 if any row failed.
*/

package main

import (
	"bufio"
	"fmt"
	"os"
	"strings"

	"log/slog"

	"github.com/ProfoundNetworks/bsearch"
	"github.com/alecthomas/kong"
	helpcolours "github.com/gavincarr/kong-help-colours"
	"github.com/lmittmann/tint"
	"golang.org/x/term"
)

// Options. Every field carries an explicit name: tag, so kong never derives
// a flag name from the field name. Verbose is a counter rather than go-flags'
// []bool idiom, which makes kong hang.
type Options struct {
	Verbose int    `short:"v" name:"verbose" type:"counter" help:"display verbose debug output"`
	Sep     string `short:"t" name:"sep" help:"separator"`
	Header  bool   `short:"H" name:"hdr" help:"CSV file includes a header (don't test)"`
	Stdin   bool   `short:"i" name:"stdin" help:"read test data from stdin instead of from CSVFile"`
	Args    struct {
		CSVFile string `arg:"" name:"CSVFile" help:"CSV file to be processed"`
	} `embed:""`
}

var opts Options

// maxLineLength is the longest test-data line we accept; it matches the
// limit bsearch.NewIndex applies when indexing
const maxLineLength = 64 * 1024 * 1024

// newParser builds the kong parser for bsearch_selftest over o.
func newParser(o *Options) (*kong.Kong, error) {
	return kong.New(o,
		kong.Name("bsearch_selftest"),
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

// results counts the rows checked and reports each failure on stdout
type results struct {
	checked int
	failed  int
}

func (r *results) pass() {
	r.checked++
}

func (r *results) fail(rownum int, name, reason string) {
	r.checked++
	r.failed++
	fmt.Printf("row %d: %s: %s\n", rownum, name, reason)
}

func processLine(bss *bsearch.Searcher, res *results, key, line string, rownum int) {
	// Lookup the line for key
	got, err := bss.Line([]byte(key))
	if err != nil {
		res.fail(rownum, key, err.Error())
	} else if string(got) != line {
		res.fail(rownum, key, fmt.Sprintf("got %q, expected %q", got, line))
	} else {
		res.pass()
	}
}

func processBatch(bss *bsearch.Searcher, res *results, key string, batch []string, rownum int) {
	// Lookup all lines for key
	lines, err := bss.Lines([]byte(key))
	if err != nil {
		for i, line := range batch {
			res.fail(rownum+i, line, err.Error())
		}
		return
	}

	// Check that every input line in batch is among the lines found
	found := make(map[string]bool)
	for _, line := range lines {
		found[string(line)] = true
	}
	for i, line := range batch {
		if !found[line] {
			res.fail(rownum+i, line, fmt.Sprintf("line not found for key %q", key))
		} else {
			res.pass()
		}
	}
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
	var fh *os.File
	if opts.Stdin {
		fh = os.Stdin
	} else {
		fh, err = os.Open(opts.Args.CSVFile)
		if err != nil {
			die(err.Error())
		}
	}
	bss, err := bsearch.NewSearcher(opts.Args.CSVFile)
	if err != nil {
		die(err.Error())
	}
	defer bss.Close()

	// Default to the index's delimiter, which is what searches use
	if opts.Sep == "" {
		opts.Sep = string(bss.Index.Delimiter)
	}
	slog.Info("", "sep", opts.Sep)

	// Skip the header if the index records one, when reading the dataset
	// itself (stdin input may be a sample, so there it needs --hdr)
	skipHeader := opts.Header || (!opts.Stdin && bss.Index.Header)
	keysUnique := bss.Index.KeysUnique
	scanner := bufio.NewScanner(fh)
	scanner.Buffer(make([]byte, 64*1024), maxLineLength)

	// Process
	var res results
	rownum := 1
	prevKey := ""
	batch := []string{}
	for scanner.Scan() {
		line := scanner.Text()

		if skipHeader {
			skipHeader = false
			continue
		}

		// NB: this split *requires* that there are no escaped delimiters
		// in the key. But that should always be the case on these kinds
		// of datasets, since `sort(1)` won't sort them correctly otherwise.
		// A line without a separator is all key (a single-column dataset).
		key, _, _ := strings.Cut(line, opts.Sep)

		// keysUnique processing - individual lines
		if keysUnique {
			processLine(bss, &res, key, line, rownum)
			rownum += 1
			continue
		}

		// Duplicate keys processing - accumulate batch with the same key
		if key < prevKey {
			// Keys must be in sorted order for dup keys processing
			die(fmt.Sprintf("Unsorted input? Line %d key %q < %q",
				rownum+len(batch), key, prevKey))
		} else if key == prevKey {
			batch = append(batch, line)
		} else {
			// Break - process batch and reset
			if len(batch) > 0 {
				processBatch(bss, &res, prevKey, batch, rownum)
			}
			prevKey = key
			rownum += len(batch)
			batch = []string{line}
		}
	}
	if err := scanner.Err(); err != nil {
		die(err.Error())
	}
	if len(batch) > 0 {
		processBatch(bss, &res, prevKey, batch, rownum)
	}

	// Summarise, exiting 1 if any row failed
	if res.failed > 0 {
		fmt.Printf("rows checked: %d, failed: %d\n", res.checked, res.failed)
		os.Exit(1)
	}
	fmt.Printf("rows checked: %d, all ok\n", res.checked)
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
