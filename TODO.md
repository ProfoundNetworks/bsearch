# TODO

- [ ] Remove or implement `SearcherOptions.Header`. `searcher.go` never
      reads it, so `bsearch -H` has no effect; header handling comes
      entirely from the index.
- [ ] Revisit whether to add back support for key prefix matching
      (dropped in favour of whole-key matching in 95a7fe8).
- [ ] Do some performance tuning.
- [ ] Explore the tradeoffs around using mmap or not.
