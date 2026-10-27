/*
DB provides a simple key-value-store-like interface using bsearch.Searcher,
returning the first first entry for a given key.
*/

package bsearch

import (
	"bytes"
	"encoding/csv"
	"fmt"
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

// value returns the (first) value associated with key in db, and whether
// the line holds only the key (no delimiter, so no value fields at all)
func (db *DB) value(key []byte) ([]byte, bool, error) {
	line, err := db.bss.Line(key)
	if err != nil {
		return nil, false, err
	}

	// A line consisting only of key has an empty value
	if bytes.Equal(line, key) {
		return []byte{}, true, nil
	}

	// Remove leading key+delimiter from line
	prefix := append(key, db.bss.Index.Delimiter...)
	// Sanity check
	if !bytes.HasPrefix(line, prefix) {
		panic(
			fmt.Sprintf("line returned for %q does not begin with key+delim: %s\n",
				key, line))
	}
	return bytes.TrimPrefix(line, prefix), false, nil
}

// Get returns the (first) value associated with key in db
// (or ErrNotFound if missing)
func (db *DB) Get(key []byte) ([]byte, error) {
	val, _, err := db.value(key)
	return val, err
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

// GetSlice returns the (first) value associated with key in db as a string
// slice (read using a csv.Reader with the appropriate Delimiter)
// (or returns ErrNotFound if missing)
func (db *DB) GetSlice(key string) ([]string, error) {
	val, keyOnly, err := db.value([]byte(key))
	if err != nil {
		return []string{}, err
	}
	delim := string(db.bss.Index.Delimiter)
	if len(delim) > 1 {
		return []string{},
			fmt.Errorf("cannot convert multi-character delimiter %q to rune", delim)
	}
	// csv.Reader returns io.EOF for empty input, so handle that here: a
	// key-only line has no fields, and "key<delim>" one empty field
	if keyOnly {
		return []string{}, nil
	}
	if len(val) == 0 {
		return []string{""}, nil
	}
	reader := csv.NewReader(bytes.NewReader(val))
	reader.Comma = rune(delim[0])
	s, err := reader.Read()
	if err != nil {
		return []string{}, err
	}
	return s, nil
}

// Close closes our underlying Searcher
func (db *DB) Close() {
	db.bss.Close()
}
