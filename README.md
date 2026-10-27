
bsearch
=======

bsearch is a go library providing binary search functionality for line-ordered
byte streams (such as `LC_ALL=C` sorted text files). It allows very fast
lookups by key, in the spirit of the venerable `look(1)` unix utility. A
line's key is its first field, and keys match as a whole: searching for
`foo` finds `foo,1` but not `foobar,1`.

bsearch can also be used via a `DB` object with a simple db-like interface,
allowing sorted CSV files to be used as a key-value store, with excellent
performance.

bsearch currently only supports bytewise key comparisons (not UTF-8 collations).
This restriction may be removed in the future.

Usage
-----

```
    import "github.com/ProfoundNetworks/bsearch"

    // Create the index (also see cmd/bsearch_index for a CLI tool)
    idx, err := bsearch.NewIndex(filepath)

    // Alternatively, if you need to tweak the options
    idx, err := bsearch.NewIndexOptions(filepath, bsearch.IndexOptions{Delimiter: []byte("|")})

    // Persist the created index to disk, so that the searcher may use it
    err = idx.Write()

    // Instantiate searcher from a file
    bss, err := bsearch.NewSearcher(filepath)
    defer bss.Close()

    // Find first line whose key is searchStr
    line, err := bss.Line([]byte(searchStr))

    // Distinguish not found from other errors
    if err != nil && err == bsearch.ErrNotFound {
        // do something
    } else if err != nil {
        log.Fatal(err)
    }


    // Or via the `DB` interface
    db, err := bsearch.NewDB(filepath)
    defer db.Close()

    // Lookup values via byteslices or strings
    valbytes, err := db.Get(keybytes)
    valstr, err := db.GetString(keystring)

```

Status
------

bsearch is alpha software. Interfaces may break and change until an official
version 1.0.0 is released.


Copyright and Licence
---------------------

Copyright 2020 Profound Networks LLC.

This project is licensed under the terms of the MIT license.

