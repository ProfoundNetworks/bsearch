/*
bsearch dev utility to run lint checks on the index for a given dataset.
*/

package main

import (
	"fmt"
	"io"
	"log"
	"os"
	"regexp"

	"github.com/DataDog/zstd"
	"github.com/ProfoundNetworks/bsearch"
	flags "github.com/jessevdk/go-flags"
)

// Options
var opts struct {
	Verbose bool `short:"v" long:"verbose" description:"display verbose debug output"`
	Fatal   bool `short:"f" long:"fatal" description:"die on any errors"`
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

func loadDataBlock(reader io.ReadCloser, entry *bsearch.IndexEntry, compressed bool) ([]byte, error) {
	buf := make([]byte, entry.Length)

	// Read data from reader
	bytesread, err := reader.Read(buf)
	if err != nil && err != io.EOF {
		return buf, err
	}
	if bytesread < entry.Length {
		return buf, fmt.Errorf("error reading block - read %d bytes, expected %d\n", bytesread, entry.Length)
	}

	if !compressed {
		return buf, nil
	}

	// If the data is compressed, we need to decompress it
	//vprintf("+ decompressing %d bytes, md5 %x\n%v\n", len(buf), md5.Sum(buf), buf)
	dbuf, err := zstd.Decompress(nil, clone(buf))
	if err != nil {
		return dbuf, err
	}

	return dbuf, nil
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

	// Instantiate a bsearch.Searcher
	bso := bsearch.Options{Index: bsearch.IndexRequired}
	bss, err := bsearch.NewSearcherFileOptions(opts.Args.Filename, bso)
	if err != nil {
		log.Fatal(err)
	}

	fh, err := os.Open(opts.Args.Filename)
	if err != nil {
		log.Fatal(err)
	}
	defer fh.Close()
	reCompressed := regexp.MustCompile(`\.zst$`)
	compressed := false
	if reCompressed.MatchString(opts.Args.Filename) {
		compressed = true
	}
	//vprintf("+ compressed: %v\n", compressed)

	// Check all index blocks
	index := bss.Index
	ok := 0
	fail := 0
	var offset int64 = 0
	var lastKey string
	for i, entry := range index.List {
		vprintf("+ entry: %v\n", entry)

		// Skip header, if present
		if i == 0 && entry.Offset > 0 {
			buf := make([]byte, entry.Offset)
			bytesread, err := fh.Read(buf)
			if err != nil && err != io.EOF {
				log.Fatal(err)
			}
			if bytesread != int(entry.Offset) {
				log.Fatalf("bad initial read, read %d, expected offset %d\n", bytesread, entry.Offset)
			}
			offset += entry.Offset
		}

		// Load the data block
		buf, err := loadDataBlock(fh, &entry, compressed)
		if err != nil {
			log.Fatal(err)
		}

		// Lint checks
		errors := 0
		// entry.Offset should match our calculated offset
		if offset != entry.Offset {
			fmt.Printf("[%d] block offset does not match calculated: got %d, exp %d\n", i, entry.Offset, offset)
			errors++
		}
		// buf should begin with entry.Key
		if string(buf[:len(entry.Key)]) != entry.Key {
			fmt.Printf("[%d] block does not begin with entry.Key: got %q, exp %q\n", i, string(buf[:len(entry.Key)]), entry.Key)
			errors++
		}
		// buf should end with a newline
		if buf[len(buf)-1] != '\n' {
			begin := len(buf) - 11
			if begin < 0 {
				begin = 0
			}
			fmt.Printf("[%d] block does not end with a newline: ends %q\n", i, string(buf[begin:]))
			errors++
		}
		// entry.Key should always come after lastKey (no multi-block keys)
		if lastKey != "" && entry.Key <= lastKey {
			fmt.Printf("[%d] block key is <= lastKey: lastKey %q, entry.Key %q\n", i, lastKey, entry.Key)
			errors++
		}

		offset += int64(entry.Length)
		lastKey = entry.Key

		if errors > 0 {
			fail++
		} else {
			ok++
		}
	}

	total := fail + ok
	if fail > 0 {
		fmt.Printf("%d / %d blocks failed, %d / %d blocks ok\n", fail, total, ok, total)
	} else {
		fmt.Printf("%d / %d blocks ok\n", ok, total)
	}

}

// clone returns a copy of the given byte slice
func clone(b []byte) []byte {
	c := make([]byte, len(b))
	copy(c, b)
	return c
}
