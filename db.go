/*
DB provides a sipmle key-value-store-like interface using bsearch.Searcher,
returning the first first entry for a given key.
*/

package bsearch

import (
	"bytes"
	"fmt"
	"io"
)

// DB provides a simple key-value-store-like interface using bsearch.Searcher,
// returning the first value from path for a given key (if you need more
// control you're encouraged to use bsearch.Searcher directly).
type DB struct {
	bss *Searcher // searcher
}

// NewDB returns a new DB for the file at path. The caller is responsible
// for calling DB.Close() when finished (e.g. via defer).
func NewDB(path string) (*DB, error) {
	bss, err := NewSearcher(path)
	if err != nil {
		return nil, err
	}

	return &DB{bss: bss}, nil
}

// Get returns the (first) value associated with key in db
// (or ErrNotFound if missing)
func (db *DB) Get(key []byte) ([]byte, error) {
	line, err := db.bss.Line(key)
	if err != nil {
		return nil, err
	}

	// Remove leading key+delimiter from line
	prefix := append(key, db.bss.Index.Delimiter...)
	// Sanity check
	if !bytes.HasPrefix(line, prefix) {
		panic(
			fmt.Sprintf("line returned for %q does not begin with key+delim: %s\n",
				key, line))
	}
	line = bytes.TrimPrefix(line, prefix)

	return line, nil
}

// GetString returns the (first) value associated with key in db, as a string
// (or ErrNotFound if missing)
func (db *DB) GetString(key string) (string, error) {
	val, err := db.Get([]byte(key))
	if err != nil {
		return "", err
	}
	return string(val), nil
}

// Close closes our Searcher's underlying reader (if applicable)
func (db *DB) Close() {
	if closer, ok := db.bss.r.(io.Closer); ok {
		closer.Close()
	}
}
