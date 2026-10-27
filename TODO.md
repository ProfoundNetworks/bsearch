# TODO

- [ ] Remove or implement `SearcherOptions.Header`. `searcher.go` never
      reads it, so `bsearch -H` has no effect; header handling comes
      entirely from the index.
- [ ] Revisit whether to add back support for key prefix matching
      (dropped in favour of whole-key matching in 95a7fe8).
- [ ] Revisit `SearcherOptions.MatchLE`, and whether to support exact
      matching again. `setOptions` sets `s.matchLE` but nothing reads it
      (its comment refers to a `LinePosition` that no longer exists).
- [ ] Do some performance tuning.
- [ ] Explore the tradeoffs around using mmap or not.
