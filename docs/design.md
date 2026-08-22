# metainjester design

Intent and the rules worth not breaking. Implementation detail lives in
`CLAUDE.md` and the code. This file records decisions, not plans.

## Product

A local, single-user CLI that catalogues a directory tree into SQLite and reports
what changed on later runs. It observes metadata only — it never copies, moves,
edits, serves, or takes ownership of file contents.

`ingest <base-path>` is the whole write surface, and is also the resume command.
`status` and `history prune` (without `--apply`) are read-only, so they stay
answerable while a scan runs.

**Out of scope:** GUI, in-app search, CSV export, network or service mode,
content management, multi-user or concurrent writers, continuous filesystem
watching, image similarity, content sniffing for MIME. Querying is
SQLiteBrowser's job. Freshness comes from rerunning `ingest`.

Sniffing is out because `mime_type` feeds change detection: switching sources
would report a false `updated` for every file whose sniffed type disagrees with
its extension. Use `file(1)` over `current_files` instead.

## Invariants

Each has a reason that is easy to forget and expensive to rediscover.

- **`files` is the last successfully completed scan.** A cancelled, failed, or
  partial scan never replaces the baseline or creates deletion records.
- **Absence proves deletion only when the path could be read.** An unreadable
  directory leaves its contents unknown, so its rows become
  `presence = 'unreadable'` and get no change row in either direction. Fixing the
  permission and rerunning must land exactly where a readable first scan would
  have. Only an unreadable *base* refuses to promote, because it prefixes every
  path and puts the whole baseline in doubt at once.
- **Inclusion policy must match the baseline or the rescan refuses.** If a prior
  scan included `.cache` and the config now skips hidden files, every hidden path
  would look deleted. This is why the three policy fields are stored per base.
- **Never use a directory's mtime to skip a subtree.** Editing a file does not
  change its parent's mtime, so an ancestor timestamp cannot prove a subtree
  unchanged. Timestamps may prioritise work; they may never skip it.
- **Traversal holds one directory handle, not one per level.** Descending while
  the parent's `ReadDir` is still alive costs a descriptor per level and hits
  `EMFILE` on a deep tree, silently abandoning everything below it while the scan
  still reports `complete`. Each level collects its subdirectories, drops the
  handle, then descends. Keeping a `DirEntry` instead would defeat this — it
  holds an `Arc` to the open directory.
- **Hash reuse requires exact equality of path, size, and nanosecond mtime.**
  Accepted edge case: a restored timestamp can hide a content change.
- **Renames are `deleted` + `added`.** A content-hash match is not proof of a
  move — duplicate files and inode reuse produce false claims.
- **No foreign key from `scan_changes` to `files`.** History stores
  `(base_id, relative_path)` so dead `files` rows can be pruned without touching
  it. The cost: readers must `LEFT JOIN` on *both* columns, since joining on path
  alone silently matches across bases.
- **SHA-256 is a 32-byte BLOB, not hex text.** Hex doubles the bytes everywhere.
  `hex(content_hash)` renders it for humans.
- **Single containment.** One SQLite file holds everything — no external cache,
  log, or auxiliary index. Any feature needing a second durable file must be
  redesigned. Copy a live database with `VACUUM INTO`, never `cp`.
- **Free space is a floor to stay above, never a size to predict.** Predicting a
  scan's appetite needs a file count that does not exist before the first walk.
  `storage.minimum_free_space_mib` is checked before the scan and after every
  batch; crossing it stops the scan and keeps the staging for a later resume.
- **Durability is not negotiable.** Never `journal_mode=OFF|MEMORY` or
  `synchronous=OFF`.
- **No policy flags on the command line.** Policy lives in configuration.
- **"base", never "root".** Avoids ambiguity with `/`.

## Capacity

Planning range is 300k files to 5M, measured at **~667 bytes per file row** — so
roughly 3.3 GB at the top end. Nothing reserves against this figure; it exists to
size a machine before buying one.

## History retention

`scan_changes` grows with change *events*, so a directory rename high in the tree
is the worst case. `history.keep_scans` bounds it to the newest N **complete**
scans per base, so a run of failures cannot age out real history. The same cutoff
prunes `files` rows already `deleted`; `unreadable` is live state and is never
pruned.

Default is 0 — keep everything. Deleting a user's history because they upgraded
is not a default worth choosing.

Deleting rows does not shrink the file; freed pages are reused, so it stops
growing. Reclaiming bytes needs `VACUUM INTO`, which `prune` prints rather than
attempts.

## Views

`current_files` filters to `presence = 'present'` and joins an absolute path,
because the obvious `SELECT * FROM files` silently includes deleted and
unreadable rows. Views hold no data, so they are dropped and rebuilt on every
*write* open — a changed definition cannot go stale, and adding one needs no
`SCHEMA_VERSION` bump. Rebuilding is itself a write, so the read-only commands
open read-only and skip it rather than contend for the write lock.

Privacy is out of scope at the application level: the database is plain SQLite so
SQLiteBrowser works. Put it in a LUKS container if it needs protection.
