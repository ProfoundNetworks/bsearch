// Binary search ordered Filename for lines whose key is SearchString

package main

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"os"
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
	Verbose int  `short:"v" name:"verbose" type:"counter" help:"display verbose debug output"`
	Header  bool `short:"H" name:"hdr" help:"ignore first line (header) in Filename when doing lookups"`
	Rev     bool `short:"r" name:"rev" help:"reverse SearchString for search, and reverse output lines when printing"`
	Stdin   bool `short:"c" name:"stdin" help:"read SearchStrings from standard input instead of command line"`
	Args    struct {
		SearchString string `arg:"" name:"SearchString"`
		Filename     string `arg:"" name:"Filename"`
	} `embed:""`
}

var opts Options

// newParser builds the kong parser for bsearch over o.
func newParser(o *Options) (*kong.Kong, error) {
	return kong.New(o,
		kong.Name("bsearch"),
		kong.Description("Binary search ordered Filename for lines whose key is SearchString."),
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

	// Instantiate searcher
	o := bsearch.SearcherOptions{Header: opts.Header}
	if opts.Verbose > 0 {
		o.Logger = slog.Default()
	}
	bss, err := bsearch.NewSearcherOptions(opts.Args.Filename, o)
	switch {
	case errors.Is(err, bsearch.ErrFileCompressed):
		fmt.Fprintf(os.Stderr, "Filename %q appears to be compressed - cannot binary search\n", opts.Args.Filename)
		os.Exit(2)
	case errors.Is(err, bsearch.ErrIndexNotFound):
		die(fmt.Sprintf("Error: no index found for %q - create one with bsearch_index",
			opts.Args.Filename))
	case err != nil:
		die(err.Error())
	}
	if opts.Verbose > 0 {
		idxpath, err := bsearch.IndexPath(opts.Args.Filename)
		if err != nil {
			die(err.Error())
		}
		slog.Info("using index", "path", idxpath)
	}

	if opts.Stdin {
		reader := bufio.NewReader(os.Stdin)

		for {
			// A final key without a trailing newline comes back with io.EOF
			lineBytes, err := reader.ReadBytes('\n')
			if err != nil && err != io.EOF {
				die(err.Error())
			}
			if len(lineBytes) == 0 {
				break
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
