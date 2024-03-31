// Binary search ordered Filename for lines beginning with SearchString

package main

import (
	"bufio"
	"fmt"
	"io"
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
	Header  bool   `short:"H" long:"hdr" description:"ignore first line (header) in Filename when doing lookups"`
	Rev     bool   `short:"r" long:"rev" description:"reverse SearchString for search, and reverse output lines when printing"`
	Stdin   bool   `short:"c" long:"stdin" description:"read SearchStrings from standard input instead of command line"`
	Args    struct {
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
	if len(opts.Verbose) > 1 {
		fmt.Fprintf(os.Stderr, format, args...)
	}
}

func die(msg string) {
	fmt.Fprintln(os.Stderr, msg)
	os.Exit(1)
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

	// Die if Filename looks compressed
	re := regexp.MustCompile(`\.(gz|bz2|br)$`)
	if re.MatchString(opts.Args.Filename) {
		fmt.Fprintf(os.Stderr, "Filename %q appears to be compressed - cannot binary search\n", opts.Args.Filename)
		os.Exit(2)
	}

	// Instantiate searcher
	o := bsearch.SearcherOptions{Header: opts.Header}
	if len(opts.Verbose) > 0 {
		log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stderr})
		o.Logger = &log.Logger
	}
	bss, err := bsearch.NewSearcherOptions(opts.Args.Filename, o)
	if err != nil {
		die(err.Error())
	}
	if len(opts.Verbose) > 0 {
		idxpath, err := bsearch.IndexPath(opts.Args.Filename)
		if err != nil {
			die(err.Error())
		}
		log.Info().
			Str("path", idxpath).
			Msg("using index")
	}

	if opts.Stdin {
		reader := bufio.NewReader(os.Stdin)

		for {
			lineBytes, err := reader.ReadBytes('\n')
			if err == io.EOF {
				break
			} else if err != nil {
				die(err.Error())
			}

			searchStr := strings.Trim(string(lineBytes), "\n")
			if opts.Rev {
				searchStr = reverse(searchStr)
			}

			results, err := bss.Lines([]byte(searchStr))
			if err == bsearch.ErrNotFound {
				fmt.Println()
				continue
			} else if err != nil {
				die(err.Error())
			}

			for _, r := range results {
				rs := string(r)
				if opts.Rev {
					fmt.Println(reverse(rs))
				} else {
					fmt.Println(rs)
				}
			}

			//
			// Nb. blank line means no more results
			//
			fmt.Println()
		}

		os.Exit(0)
	}

	searchStr := opts.Args.SearchString
	if opts.Rev {
		searchStr = reverse(searchStr)
	}

	// Search
	results, err := bss.Lines([]byte(searchStr))
	if err != nil {
		if err == bsearch.ErrIndexNotFound {
			die("Error: compressed dataset without index - recompress using bsearch_compress.")
		}
		die("Error: " + err.Error())
	}
	for _, l := range results {
		var line string
		if opts.Rev {
			line = reverse(string(l))
		} else {
			line = string(l)
		}
		fmt.Println(line)
	}
}

// reverse returns its argument string reversed rune-wise left to right.
func reverse(s string) string {
	r := []rune(s)
	for i, j := 0, len(r)-1; i < len(r)/2; i, j = i+1, j-1 {
		r[i], r[j] = r[j], r[i]
	}
	return string(r)
}
