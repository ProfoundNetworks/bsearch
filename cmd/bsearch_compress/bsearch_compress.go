/*
bsearch utility to compress a plaintext bsearch dataset using
a bsearch index. Each bsearch index block is compressed separately,
and then the compressed blocks are concatenated together to form
the compressed output file. This style of multistream compression
is supported by both gzip and zstd. zstd compression is the default,
as it produces smaller and faster compressed files.

If no index file exists for the given dataset one will be created.
*/

package main

import (
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"regexp"

	"github.com/DataDog/zstd"
	"github.com/ProfoundNetworks/bsearch"
	flags "github.com/jessevdk/go-flags"
	"github.com/jinzhu/copier"
)

// Options
var opts struct {
	Verbose     bool   `short:"v" long:"verbose"  description:"display verbose debug output"`
	Compression string `short:"c" long:"compress" description:"compression format - 'zstd|gzip'" default:"zstd"`
	Force       bool   `short:"f" long:"force"    description:"force compression even if a compressed file exists"`
	Args        struct {
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

// epoch returns the modtime for filename in epoch/unix format
func epoch(filename string) (int64, error) {
	stat, err := os.Stat(filename)
	if err != nil {
		return 0, err
	}
	return stat.ModTime().Unix(), nil
}

func loadIndex(indexPath string) *bsearch.Index {
	_, err := os.Stat(indexPath)
	if err == nil {
		index, err := bsearch.NewIndexLoad(opts.Args.Filename)
		if err != nil {
			log.Fatal(err)
		}
		return index
	}
	vprintf("+ generating missing index path %q\n", indexPath)
	if !os.IsNotExist(err) {
		log.Fatal(err)
	}
	// No index - generate+write
	index, err := bsearch.NewIndex(opts.Args.Filename)
	if err != nil {
		log.Fatal(err)
	}
	err = index.Write()
	if err != nil {
		log.Fatal(err)
	}
	return index
}

func compress(src []byte) (dst []byte, err error) {
	switch {
	case opts.Compression == "zstd":
		dst, err = zstd.Compress(nil, src)
	case opts.Compression == "gzip":
		//dst, err = gzip.Compress(nil, src)
	}
	return dst, err
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

	// Die if Filename looks compressed
	reCompression := regexp.MustCompile(`\.(gz|bz2|zst|br)$`)
	if reCompression.MatchString(opts.Args.Filename) {
		log.Fatalf("Filename %q appears to be already compressed?\n", opts.Args.Filename)
	}

	var zfile string
	switch {
	case opts.Compression == "zstd":
		zfile = opts.Args.Filename + ".zst"
	case opts.Compression == "gzip":
		zfile = opts.Args.Filename + ".gz"
	default:
		log.Fatalf("Invalid --compress option %q - not 'zstd|gzip'\n", opts.Compression)
	}
	vprintf("+ compression: %s\n", opts.Compression)

	// Noop if a compressed file already exists (unless --force is specified)
	if !opts.Force {
		_, err := os.Stat(zfile)
		if err != nil && !os.IsNotExist(err) {
			log.Fatal(err)
		}
		if err == nil {
			log.Fatalf("Compressed file %q already exists - use --force to overwrite\n", zfile)
		}
	}

	// Generate (uncompressed) index if not found
	uip := bsearch.IndexPath(opts.Args.Filename)
	vprintf("+ uip: %s\n", uip)
	uidx := loadIndex(uip)

	// Generate a new index for the compressed dataset
	zip := bsearch.IndexPath(zfile)
	vprintf("+ zip: %s\n", zip)
	zidx := &bsearch.Index{}
	copier.Copy(zidx, uidx)
	zidx.Filename = filepath.Base(zfile)
	vprintf("+ zidx: %v\n", zidx)

	// Open reader/writer
	reader, err := os.Open(opts.Args.Filename)
	if err != nil {
		log.Fatal(err)
	}
	// Use a standard os.File instead of a (zstd|gzip).Writer, because we need to track compressed bytes
	writer, err := os.OpenFile(zfile, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		log.Fatal(err)
	}

	// Read/write dataset blocks
	var c int64 = 0
	if uidx.Header {
		entry := uidx.List[0]
		src := make([]byte, entry.Offset)
		bytesread, err := reader.ReadAt(src, 0)
		if err != nil && err != io.EOF {
			log.Fatal(err)
		}
		if bytesread != int(entry.Offset) {
			log.Fatalf("Error: short read for header before %v - only %d bytes read\n", entry, bytesread)
		}
		dst, err := compress(src)
		if err != nil && err != io.EOF {
			log.Fatal(err)
		}
		_, err = writer.Write(dst)
		if err != nil && err != io.EOF {
			log.Fatal(err)
		}
		c += int64(len(dst))
	}
	for i, entry := range uidx.List {
		fmt.Printf("+ [%d] %v\n", i, entry)
		src := make([]byte, entry.Length)
		bytesread, err := reader.ReadAt(src, entry.Offset)
		if err != nil && err != io.EOF {
			log.Fatal(err)
		}
		if bytesread != entry.Length {
			log.Fatalf("Error: short read for entry %v - only %d bytes read\n", entry, bytesread)
		}
		dst, err := compress(src)
		if err != nil && err != io.EOF {
			log.Fatal(err)
		}
		_, err = writer.Write(dst)
		if err != nil && err != io.EOF {
			log.Fatal(err)
		}
		// Update compressed index entry
		zidx.List[i].Offset = c
		zidx.List[i].Length = len(dst)
		c += int64(len(dst))
	}
	err = writer.Close()
	if err != nil && err != io.EOF {
		log.Fatal(err)
	}

	// Write new compressed index
	epoch, err := epoch(zfile)
	if err != nil && err != io.EOF {
		log.Fatal(err)
	}
	zidx.Epoch = epoch
	err = zidx.Write()
	if err != nil && err != io.EOF {
		log.Fatal(err)
	}
}
