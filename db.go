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
	//if bss.Index != nil && bss.Index.Delimiter != delim {
	//    return nil, errors.New("NewDB delimiter does not not match index delimiter!")
	//}

	return &DB{bss: bss, delim: []byte(delim)}, nil
}

// Get returns the (first) value associated with key in db (or ErrNotFound if missing)
func (db *DB) Get(key []byte) ([]byte, error) {
	lookup := key
	trim := len(db.delim)

	// If the underlying index is not using a delimiter, add ours
	if db.bss.Index != nil && db.bss.Index.Delimiter == 0 {
		lookup = append(key, db.delim...)
		trim = 0
	}

	line, err := db.bss.Line(lookup)
	if err != nil {
		return nil, err
	}

	return line[len(lookup)+trim:], nil
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
