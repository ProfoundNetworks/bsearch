/*
DB provides a key-value-store-like interface using bsearch.Searcher.
Returns the first entry prefixed with the given key.
*/

package bsearch

import (
	"errors"
	"io"
)

// DB provides a key-value-store-like interface using bsearch.Searcher
type DB struct {
	bss *Searcher // searcher
	sep []byte    // separator bytes
}

// NewDB returns a new DB for filename, using separator to terminate keys.
// The caller is responsible for calling DB.Close() when finished (e.g. via defer).
func NewDB(filename, separator string) (*DB, error) {
	if separator == "" {
		return nil, errors.New("NewDB separator cannot be an empty string")
	}

	bss, err := NewSearcher(filename)
	if err != nil {
		return nil, err
	}

	return &DB{bss: bss, sep: []byte(separator)}, nil
}

// Get returns the (first) value associated with key in db (or ErrNotFound if missing)
func (db *DB) Get(key []byte) ([]byte, error) {
	lookup := append(key, db.sep...)

	line, err := db.bss.Line(lookup)
	if err != nil {
		return nil, err
	}

	return line[len(lookup):], nil
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
