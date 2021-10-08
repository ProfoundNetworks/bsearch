
bsearch
=======

bsearch is a go library providing binary search functionality for line-ordered
byte streams (such as `LC_ALL=C` sorted text files). It allows very fast
lookups based on line prefixes, like the venerable `look(1)` unix utility.

bsearch can also be used via a `DB` object with a simple db-like interface,
allowing sorted CSV files to be used as a key-value store, with excellent
performance.

bsearch currently only supports bytewise key comparisons (not UTF-8 collations).
This restriction may be removed in the future.

Usage
-----

```
    import "github.com/ProfoundNetworks/bsearch"

    // Instantiate searcher from a file
    bss, err := bsearch.NewSearcherFile(filepath)
    defer bss.Close()

    // Or instantiate searcher from an io.ReaderAt
    bss := bsearch.NewSearcher(reader, datalen)

    // Find first line beginning with searchStr
    line, err := bss.Line([]byte(searchStr))

    // Find position of first line beginning with searchStr
    pos, err := bss.LinePosition([]byte(searchStr))

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
    valbytes := db.Get(keybytes)
    valstr := db.GetString(keystring)

```

Status
------

bsearch is alpha software. Interfaces may break and change until an official
version 1.0.0 is released.


Copyright and Licence
---------------------

Copyright 2020 Profound Networks LLC.

This project is licensed under the terms of the MIT license.

