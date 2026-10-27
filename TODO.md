# TODO

- [ ] Make `bsearch_index` regenerate an up-to-date index when explicit
      options (`--hdr`, `--no-hdr`, `-t`, `-b`) disagree with it. Today
      the `LoadIndex` early exit reports "up to date" and keeps the old
      index unless `--force` is given.
- [ ] Remove or implement `SearcherOptions.Header`. `searcher.go` never
      reads it, so `bsearch -H` has no effect; header handling comes
      entirely from the index.
- [ ] Revisit whether to add back support for key prefix matching
      (dropped in favour of whole-key matching in 95a7fe8).
- [ ] Do some performance tuning.
- [ ] Explore the tradeoffs around using mmap or not.
