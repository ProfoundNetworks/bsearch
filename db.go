/*
DB provides a key-value-store-like interface using bsearch.Searcher.
Returns the first entry containing the given key.
*/

package bsearch

import (
	"bytes"
	"errors"
	"fmt"
	"io"
)

// DB provides a key-value-store-like interface using bsearch.Searcher
type DB struct {
	bss   *Searcher // searcher
	delim []byte    // delimiter
}

// NewDB returns a new DB for filename, using delimiter to terminate keys.
// The caller is responsible for calling DB.Close() when finished (e.g. via defer).
func NewDB(filename, delim string) (*DB, error) {
	if delim == "" {
		return nil, errors.New("NewDB delimiter cannot be an empty string")
	}

	bss, err := NewSearcher(filename)
	if err != nil {
		return nil, err
	}

	// FIXME: If the searcher has an index with a different delimiter,
	// we have a problem
	//if bss.Index != nil && string(bss.Index.Delimiter) != delim {
	//    return nil, errors.New("NewDB delimiter does not not match index delimiter!")
	//}

	return &DB{bss: bss, delim: []byte(delim)}, nil
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
		panic(fmt.Sprintf("line returned for %q does not begin with key+delim: %s\n",
			key, line))
	}
	line = bytes.TrimPrefix(line, prefix)

	return line, nil
}

// GetString returns the (first) value associated with key in db, as a string (or ErrNotFound if missing)
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
