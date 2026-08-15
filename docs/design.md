# metainjester design

Implementation detail lives in `CLAUDE.md` and the code; this file keeps the
intent, the invariants worth not breaking, and what is still open.

## Product

A local, single-user CLI that catalogues a directory tree into SQLite and reports
what changed on later runs. It observes metadata only — it never copies, moves,
edits, serves, or takes ownership of file contents.

**Out of scope:** GUI, in-app search, CSV export, network or service mode,
content management, multi-user or concurrent writers, continuous filesystem
watching, image similarity. Querying is SQLiteBrowser's job. Freshness comes from
rerunning `ingest`.

## Invariants

Each of these has a reason that is easy to forget and expensive to rediscover.

- **`files` is the last successfully completed scan.** A cancelled, failed, or
  partial scan must never leave a mixed view, create deletion records, or replace
  the baseline. Deletion is only ever established by a complete scan.
- **Inclusion policy must match the baseline or the rescan refuses.** If a prior
  scan included `.cache` and the current config skips hidden files, every hidden
  path would look deleted. This is the whole reason the three policy fields are
  stored per base.
- **Never use a parent directory's mtime to skip a subtree.** Editing a file does
  not change its parent's mtime, so an ancestor timestamp cannot prove a subtree
  unchanged. Directory timestamps may prioritise work; they may never skip it.
- **Hash reuse requires exact equality of path, size, and nanosecond mtime.**
  Accepted edge case: a preserved or restored timestamp can hide a content
  change. `always_hash` would trade time for the stronger guarantee.
- **Renames are `deleted` + `added`.** A content-hash match is not proof of a
  move — duplicate files and inode reuse produce false claims.
- **SHA-256 is a 32-byte BLOB, not hex text.** Hex doubles the bytes in `files`,
  staging, WAL, indexes, and backups. `hex(content_hash)` renders it for humans.
- **"base", never "root".** Avoids ambiguity with `/` and filesystem roots.
- **Single containment.** One SQLite file holds everything; the database is
  self-describing via `sqlite_master`. No sidecar state — no external cache, log,
  or auxiliary index. Any future feature needing a second durable file must be
  redesigned. To copy a live database use
  `sqlite3 <db> "VACUUM INTO '<dest>'"`, never `cp`.
- **Durability is not negotiable.** Never `journal_mode=OFF|MEMORY` or
  `synchronous=OFF`.
- **No policy flags on the command line.** `ingest <base-path>` takes a path and
  nothing else; policy lives in configuration.

## Privacy

Out of scope at the application level — no password, encryption, or key
management. The database is plain SQLite so SQLiteBrowser works; put it in a LUKS
container if it needs protection.

## Capacity

Planning range is 300k files (lower bound) to 5M (upper). The preflight gate
reserves a conservative 4 KiB per expected file.

Measured on the real implementation: **~667 bytes per file row**. The 4 KiB
constant predates that and should be recalibrated before any system requirement
is published.

## Open

Two implementation choices that may be wrong:

- **Any file-level error blocks promotion.** A vanished temporary file is
  currently as fatal as a permission failure. Likely too strict on live
  directories — needs a distinction between trust-breaking and transient errors.
- **Free-space preflight for a new base uses only the configured minimum.**
  `initial_average_file_kib` is parsed and reported but does not feed the gate,
  because expected file count does not exist before the first walk. A rescan
  correctly uses its prior present-file count × 4 KiB.

## Future

Roughly in the order they are likely to matter.

- **History retention.** `scan_changes` grows with change *events* and is
  unbounded; a directory rename high in the tree is the worst case. `files`
  deleted rows grow with *distinct paths ever seen* and are self-limiting, but
  leave a permanently dead tail after such a rename. Two prunes, preview-first,
  window counted per base over completed scans only, run after promotion in a
  separate transaction, keeping `scans` rows (they feed ETA estimates).
  Deleting rows does not shrink the file — that needs `VACUUM INTO`.
- **Do not add a foreign key from `scan_changes` to `files`.** `scan_changes`
  stores `relative_path` deliberately, so dead `files` rows can be pruned without
  touching history. An FK would couple the two prunes together.
- **Convenience views** for SQLiteBrowser — a `current_files` view that filters
  `presence = 'present'` and joins `bases` for absolute paths, so the default
  click is correct and forgetting the filter cannot silently include deleted rows.
- **`status` and `doctor`.** Read-only diagnostics: configured path, known bases,
  latest and resumable scans; then schema checks, `quick_check`, file modes, free
  space, stale staging. Add when a real need appears.
- **Rename detection** — only on an unambiguous one-to-one match, reported as an
  inference, never as fact. Capturing device/inode would strengthen a
  same-filesystem detector; cross-filesystem moves stay unprovable.
- **`always_hash` policy** and **content sniffing** for MIME (currently extension
  or `unknown`).
