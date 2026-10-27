# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

`github.com/ProfoundNetworks/bsearch` is a Go library (plus CLIs) for binary
searching line-ordered byte streams by line prefix, like `look(1)`. Datasets
must be bytewise sorted (`LC_ALL=C sort`); only bytewise key comparison is
supported, not UTF-8 collation. Alpha software: no API stability promise.

## Commands

```bash
go test ./...                              # all packages
go test -run TestSearcherLine1 .           # single test in the root package
go test -v ./cmd/bsearch_index             # one CLI package
```

The `cmd/bsearch` tests run the real binary: `TestMain` builds it into a
temporary directory. Use `indexedCopy` to give a test a dataset; it copies the
file into that directory and writes a fresh index next to it, so these tests
never depend on the repo's `testdata/*.bsy` files.

Other scripts: `rake test` uses `richgo`. `rake build`
(`scripts/build_all`) builds the binaries listed in `scripts/push.list`
(`bsearch`, `bsearch_index`) into `bin/`. `rake push` (`scripts/push`)
rsyncs them to `/opt/bsearch/bin` on the hosts given as
`rake push[host1,host2]` or `PUSH_HOST`; there is no default host.
`rake selftest` (`scripts/selftest`) builds `bin/bsearch_selftest`, indexes
each dataset, and runs the selftest against the large `data/` datasets,
which are not in the repo. `benchmark_dgin.sh` pulls a dataset from S3.
The GitHub workflow (`.github/workflows/test.yaml`) runs `go test -v ./...`
and takes its Go version from `go.mod`.

## Architecture

The root package has three layers:

- **`index.go`: `Index`.** A sparse block index over the dataset. For each
  `Blocksize` chunk (default 2048 bytes), `generateLineIndex` records the key
  (the line text up to the first `Delimiter`) and byte offset of the first
  line starting in that block. The scan also does the following:
  - Validates sort order and fails on a key sort violation.
  - Detects duplicate keys (`KeysUnique`). When a key spans blocks, the entry
    points back to the key's first occurrence (`KeysIndexFirst`).
  - Auto-detects a header: if only the second line is out of order, it treats
    the first line as a header and restarts indexing. `IndexOptions.Header`
    forces a header; `IndexOptions.NoHeader` disables detection, so an
    out-of-order second line is a sort violation. Setting both returns
    `ErrHeaderConflict`.
- **`searcher.go`: `Searcher`.** Mmaps the whole data file and loads the
  on-disk index. A lookup does a binary search over `Index.List`
  (`blockEntryLE` if `KeysIndexFirst`, else the more conservative
  `blockEntryLT`), then scans forward linearly from that block offset in the
  mmap (`scanLinesWithKey`). A match means the line starts with
  `key + Delimiter`, or consists only of the key (for single-column files),
  so keys match as whole fields, not raw prefixes. `LinesN`
  caps results to 1 when keys are unique.
- **`db.go`: `DB`.** A thin key-value wrapper over `Searcher.Line` that strips
  the `key+delimiter` prefix (`Get`, `GetString`, and `GetSlice` via
  `encoding/csv`).

### Index file format and lifecycle

- **Location:** next to the dataset. The name is the dataset filename with
  every `.` replaced by `_`, plus `.bsy`; for example, `foo.csv` becomes
  `foo_csv.bsy` (see `IndexPath`).
- **Format** (`indexVersion = 4`): line 1 is the JSON-encoded `Index` struct
  (with `List` excluded). Each later line is `offset<TAB>strconv.Quote(key)`.
- **`NewSearcher` requires an existing index.** `LoadIndex` returns one of
  these errors:
  - `ErrIndexNotFound`
  - `ErrIndexExpired`: the dataset's mtime is newer than the index file's
    (compared at full resolution, not whole seconds), or the dataset's size
    differs from the `Size` recorded at indexing. Indexes written before
    `Size` was added skip the size check. Don't require exact mtime
    equality: datasets moved with plain `cp` get new mtimes.
  - `ErrIndexPathMismatch` (the stored `Filename` doesn't match)

  `Write()` blanks `Filepath` and stores only `Filename`, so an index stays
  valid if the dataset and index move together. `LoadIndex` still accepts
  older indexes that stored `Filepath`.
- **Delimiter:** inferred from the file extension (`.csv`, `.psv`, `.tsv`,
  optionally with `.zst`) unless set in `IndexOptions.Delimiter`.
- **Compressed datasets** (by extension: `.gz`, `.bz2`, `.br`, `.xz`, `.zip`,
  `.zst`) are rejected by both `NewSearcherOptions` and `NewIndexOptions`
  with `ErrFileCompressed`. The CLIs turn that error into their
  "appears to be compressed" message.
- **Changing the on-disk format** means bumping `indexVersion` and keeping
  `LoadIndex` backward compatible.

### CLIs (`cmd/`)

Each lives in `cmd/<tool>/main.go`. They use `slog` + `tint` for logging
(`-v` for info, `-vv` for debug) and `kong` + `kong-help-colours` for
options. Each defines a `newParser` that both `main` and its grammar tests
use. Keep these kong conventions:
- an explicit `name:` tag on every field;
- `Verbose` as `type:"counter"`, never `[]bool`, which hangs kong;
- usage errors exit 2, with usage on stderr;
- `--help` exits 0, with help on stdout.

The `main_test.go` in each tool checks both streams separately.

- `bsearch_index` builds and writes the index. `--cat` dumps it as YAML
  instead; `-b` sets the blocksize in kB; `--hdr`/`--no-hdr` (mutually
  exclusive) override header auto-detection. Without `-f`, it leaves an
  up-to-date index alone unless an explicitly given option disagrees with
  it (`optionsDiffer`).
- `bsearch` looks up a key in a dataset:
  - `-c/--stdin` does batch lookups and prints a blank line after each key's
    results.
  - `-r/--rev` reverses both the key and the output, for reverse-domain
    datasets.
- `bsearch_selftest` verifies every line of a dataset can be found. It
  prints a `row N: ...` line for each failure, then a summary line, and
  exits 1 if any row failed.

## Testdata quirks

`.gitignore` ignores `*.csv` and `*.zst` everywhere, and `testdata/*.bsy`
(plus `.bsx`/`.bsz`) in the root `testdata/`. As a result:

- New CSV fixtures must be added with `git add -f`.
- Most `testdata/*.bsy` files are generated by the tests (`ensureIndex` in
  `index_test.go`) and are untracked.
- A few `.bsy` files are tracked (`foo_csv.bsy`, plus the version-3
  `foo3_csv.bsy` and `domains2_csv.bsy` that test backward compatibility).
  Tests must load these with `fixture()`, which uses a temp copy. Don't call
  `ensureIndex` on them: it rewrites an index whose CSV is newer, which would
  silently upgrade the v3 fixtures to v4.
