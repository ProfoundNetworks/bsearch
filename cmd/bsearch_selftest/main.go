/*
bsearch_selftest is a utility
*/

package main

import (
	"bufio"
	"fmt"
	"os"
	"regexp"
	"strings"

	"log/slog"

	"github.com/ProfoundNetworks/bsearch"
	"github.com/alecthomas/kong"
	helpcolours "github.com/gavincarr/kong-help-colours"
	"github.com/lmittmann/tint"
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

func defaultSep() {
	if opts.Sep != "" {
		return
	}
	reSuffix := regexp.MustCompile(`\.([cpt]sv)$`)
	matches := reSuffix.FindStringSubmatch(opts.Args.CSVFile)
	if matches != nil {
		switch matches[1] {
		case "psv":
			opts.Sep = "|"
		case "tsv":
			opts.Sep = "\t"
		default:
			opts.Sep = ","
		}
	}
}

func tap(rownum int, status, key, diag string) {
	fmt.Printf("%s %d %s\n", status, rownum, key)
	if diag != "" {
		fmt.Println(diag)
	}
}

func processLine(bss *bsearch.Searcher, key, line string, rownum int) {
	status := "ok"
	diag := ""

	// Lookup the line for key
	got, err := bss.Line([]byte(key))
	if err != nil {
		status = "not ok"
		diag = "# " + err.Error()
	} else if string(got) != line {
		status = "not ok"
		diag = fmt.Sprintf("# got %q, expected %q", got, line)
	}

	tap(rownum, status, key, diag)
}

func processBatch(bss *bsearch.Searcher, key string, batch []string, rownum int) {
	// Lookup all lines for key
	lines, err := bss.Lines([]byte(key))
	if err != nil {
		diag := "# " + err.Error()
		for i, line := range lines {
			tap(rownum+i, "not ok", string(line), diag)
		}
		return
	}

	// Check that every line exists in batch
	bmap := make(map[string]bool)
	for _, line := range batch {
		bmap[line] = true
	}
	for i, line := range lines {
		status := "ok"
		diag := ""
		if !bmap[string(line)] {
			status = "not ok"
			diag = fmt.Sprintf("# entry not found for key %q: %s", key, line)
		}
		tap(rownum+i, status, string(line), diag)
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
	defaultSep()
	slog.Info("", "sep", opts.Sep)
	bss, err := bsearch.NewSearcher(opts.Args.CSVFile)
	if err != nil {
		die(err.Error())
	}
	defer bss.Close()
	keysUnique := bss.Index.KeysUnique
	scanner := bufio.NewScanner(fh)

	// Process
	rownum := 1
	prevKey := ""
	batch := []string{}
	for scanner.Scan() {
		line := scanner.Text()

		if opts.Header {
			opts.Header = false
			continue
		}

		// NB: this split *requires* that there are no escaped delimiters
		// in the key. But that should always be the case on these kinds
		// of datasets, since `sort(1)` won't sort them correctly otherwise.
		splits := strings.SplitN(line, opts.Sep, 2)
		if len(splits) != 2 {
			die(fmt.Sprintf("Cannot split key+value on %q in line %d: %s\n",
				opts.Sep, rownum, line))
		}
		key := splits[0]

		// keysUnique processing - individual lines
		if keysUnique {
			processLine(bss, key, line, rownum)
			rownum += 1
			continue
		}

		// Duplicate keys processing - accumulate batch with the same key
		if key < prevKey {
			// Keys must be in sorted order for dup keys processing
			die(fmt.Sprintf("Unsorted input? Line %d key %q < %q",
				rownum, key, prevKey))
		} else if key == prevKey {
			batch = append(batch, line)
		} else {
			// Break - process batch and reset
			if len(batch) > 0 {
				processBatch(bss, prevKey, batch, rownum)
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
		processBatch(bss, prevKey, batch, rownum)
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
	slog.SetDefault(slog.New(tint.NewTextHandler(os.Stderr, &tint.Options{
		Level:      level,
		TimeFormat: "15:04:05.000",
	})))
}
