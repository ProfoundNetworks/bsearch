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
	bsdb, err := bsearch.NewDB(opts.Args.CSVFile)
	if err != nil {
		die(err.Error())
	}
	scanner := bufio.NewScanner(fh)

	// Process
	row := 1
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
				opts.Sep, row, line))
		}
		key := splits[0]
		val := splits[1]

		// Do lookup
		status := "ok"
		diag := ""
		got, err := bsdb.GetString(key)
		if err != nil {
			status = "not ok"
			diag = "# " + err.Error()
		} else if got != val {
			status = "not ok"
			diag = fmt.Sprintf("# got %q, expected %q", got, val)
		}

		// TAP output
		fmt.Printf("%s %d %s\n", status, row, key)
		if diag != "" {
			fmt.Println(diag)
		}
		row += 1
	}
	if err := scanner.Err(); err != nil {
		die(err.Error())
	}

	bsdb.Close()
}
