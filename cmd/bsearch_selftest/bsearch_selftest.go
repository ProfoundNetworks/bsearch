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

	"github.com/ProfoundNetworks/bsearch"
	flags "github.com/jessevdk/go-flags"
	"github.com/rs/zerolog"
	log "github.com/rs/zerolog/log"
)

// Options
var opts struct {
	Verbose []bool `short:"v" long:"verbose" description:"display verbose debug output"`
	Sep     string `short:"t" long:"sep" description:"separator"`
	Header  bool   `short:"H" long:"hdr" description:"CSV file includes a header (don't test)"`
	Stdin   bool   `short:"i" long:"stdin" description:"read test data from stdin instead of from CSVFile"`
	Args    struct {
		CSVFile string `description:"CSV file to be processed"`
	} `positional-args:"yes" required:"yes"`
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
	log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stderr})
	switch len(opts.Verbose) {
	case 0:
		zerolog.SetGlobalLevel(zerolog.WarnLevel)
	case 1:
		zerolog.SetGlobalLevel(zerolog.InfoLevel)
	case 2:
		zerolog.SetGlobalLevel(zerolog.DebugLevel)
	default:
		zerolog.SetGlobalLevel(zerolog.TraceLevel)
	}
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
	log.Info().
		Str("sep", opts.Sep).
		Msg("")
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
