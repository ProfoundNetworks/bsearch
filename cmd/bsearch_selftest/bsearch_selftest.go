/*
bsearch selftest utility to load a bsearch CSV dataset and then do
opts.Count random lookups on keys, checking each result.
Assumes keys are unique i.e. one line exists per key.
*/

package main

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"os"
	"regexp"
	"strings"

	"github.com/DataDog/zstd"
	"github.com/ProfoundNetworks/bsearch"
	flags "github.com/jessevdk/go-flags"
)

// Options
var opts struct {
	Verbose  bool   `short:"v" long:"verbose" description:"display verbose debug output"`
	Sep      string `short:"t" long:"sep" description:"separator" default:","`
	Count    int    `short:"c" long:"count" description:"number of checks to run" default:"100"`
	Header   bool   `short:"H" long:"hdr" description:"ignore first line (header) in Filename when doing lookups"`
	BufferSz int    `short:"s" long:"bs" description:"buffer size to allocate (max line size), in MB" default:"1"`
	Fatal    bool   `short:"f" long:"fatal" description:"die on any errors"`
	Index    string `short:"i" long:"index" description:"index enum: [required|create|none]"`
	Args     struct {
		Filename string
	} `positional-args:"yes" required:"yes"`
}

type Entry struct {
	Key string
	Val string
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
	var idxopt bsearch.IndexSemantics = 0
	if opts.Index != "" {
		switch opts.Index {
		case "required":
			idxopt = bsearch.IndexRequired
		case "create":
			idxopt = bsearch.IndexCreate
		case "none":
			idxopt = bsearch.IndexNone
		default:
			fmt.Fprintf(os.Stderr, "Error: invalid --index argument %q\n\n", opts.Index)
			usage()
		}
	}

	// Setup
	log.SetFlags(0)

	// Die if Filename looks compressed
	re := regexp.MustCompile(`\.(gz|bz2|br)$`)
	if re.MatchString(opts.Args.Filename) {
		fmt.Fprintf(os.Stderr, "Filename %q appears to be compressed - cannot binary search\n", opts.Args.Filename)
		os.Exit(2)
	}

	// Instantiate a bsearch.Searcher
	bso := bsearch.Options{Header: opts.Header, Index: idxopt}
	bss, err := bsearch.NewSearcherFileOptions(opts.Args.Filename, bso)
	if err != nil {
		log.Fatal(err)
	}
	if bss.Index != nil {
		if bss.Index.Header {
			opts.Header = true
		}
		vprintf("+ using index %s\n", bsearch.IndexPath(opts.Args.Filename))
	}

	// Load opts.Args.Filename as a CSV map
	cmap := loadCSVMap(opts.Args.Filename, opts.Sep, opts.Header)
	vprintf("+ loadCSVMap complete, %d entries loaded\n", len(cmap))

	// Run checks, using the fact that `range` returns map entries in a semi-random order
	ok := 0
	fail := 0
	eleb := 0
	i := 0
	for key, val := range cmap {
		if opts.Count > 0 && i >= opts.Count {
			break
		}
		vprintf("+ lookup %d: %s\n", i, key)
		lines, err := bss.Lines([]byte(key + opts.Sep))
		if err == bsearch.ErrKeyExceedsBlocksize {
			if opts.Fatal {
				fmt.Printf("Error: lookup on %q got ErrKeyExceedsBlocksize\n", key)
				os.Exit(2)
			}
			eleb++
			i++
			continue
		}
		val2 := ""
		if err == nil {
			if len(lines) > 0 {
				line := lines[0]
				val2 = strings.TrimPrefix(string(line), key+opts.Sep)
			}
		}
		if val != val2 {
			fmt.Printf("Error: [%d] lookup on %q: got %q, expected %q\n", i, key, val2, val)
			if opts.Fatal {
				os.Exit(2)
			}
			fail++
		} else {
			//vprintf("+ [%d] %q => got %q / exp %q\n", i, key, val2, val)
			ok++
		}
		i++
	}
	total := ok + fail + eleb
	if fail > 0 || eleb > 0 {
		fmt.Printf("%d / %d checks failed, %d / %d eleb errors, %d / %d checks ok\n", fail, total, eleb, total, ok, total)
	} else {
		fmt.Printf("%d / %d checks ok\n", ok, total)
	}
}

// loadCSVMap returns a key=>value map for filename, treating the first CSV field as the key
func loadCSVMap(filename, sep string, header bool) map[string]string {
	cmap := make(map[string]string)
	fh, err := os.Open(filename)
	if err != nil {
		log.Fatal(err)
	}
	defer fh.Close()
	reCompressed := regexp.MustCompile(`\.zst$`)
	var reader io.ReadCloser
	if reCompressed.MatchString(filename) {
		reader = zstd.NewReader(fh)
		defer reader.Close()
	} else {
		reader = fh
	}
	scanner := bufio.NewScanner(reader)
	// Allocate scanner buffer manually to allow for lines > 64kB
	maxlen := opts.BufferSz * 1024 * 1024 // BufferSz MB
	buf := make([]byte, maxlen)
	scanner.Buffer(buf, maxlen)
	i := 0
	for scanner.Scan() {
		line := scanner.Text()
		if header {
			header = false
			continue
		}
		tokens := strings.Split(line, sep)
		if len(tokens) <= 1 {
			log.Fatalf("Error: too few tokens on line %d: %s\n", i, line)
		}
		cmap[tokens[0]] = strings.Join(tokens[1:], sep)
		i++
	}
	if err := scanner.Err(); err != nil {
		log.Fatal(err)
	}

	return cmap
}
