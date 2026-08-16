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
- **Absence is only evidence of deletion when the path could be read.** A file
  that vanished mid-scan is simply gone and must not hold the scan hostage. A
  directory that could not be read leaves its contents unknown, so its baseline
  rows become `presence = 'unreadable'` rather than `deleted`, and get no change
  row in either direction — fixing the permission and rerunning must land exactly
  where a readable first scan would have, with unchanged files showing no change.
  Only the unreadable base itself still refuses to promote: it prefixes every
  path, so nothing can be shielded and the whole baseline is in doubt at once.
  Which paths failed lives in `scan_errors`; the summary reports only a count.
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
- **Free space is a floor to stay above, never a size to predict.** Estimating a
  scan's appetite needs a file count that does not exist before the first walk,
  and the old 4 KiB-per-file guess was six times the measured row cost. So
  `storage.minimum_free_space_mib` is checked before the scan and again after
  every committed batch; crossing it stops the scan with `low_space`, keeps the
  staging, and exits 5 exactly as the up-front refusal does. This is a floor, not
  a reservation — it stops this program filling the disk, not other programs.
- **Durability is not negotiable.** Never `journal_mode=OFF|MEMORY` or
  `synchronous=OFF`.
- **No policy flags on the command line.** `ingest <base-path>` takes a path and
  nothing else; policy lives in configuration.

## Privacy

Out of scope at the application level — no password, encryption, or key
management. The database is plain SQLite so SQLiteBrowser works; put it in a LUKS
container if it needs protection.

## Capacity

Planning range is 300k files (lower bound) to 5M (upper). Measured on the real
implementation: **~667 bytes per file row**, so the upper bound is roughly 3.3 GB
of database. Nothing in the code reserves against that figure — see the
free-space invariant — and it exists only to size a machine before buying one.

## History retention

`scan_changes` grows with change *events* and was the only unbounded table; a
directory rename high in the tree is the worst case. `files` deleted rows grow
with *distinct paths ever seen* and are self-limiting, but leave a permanently
dead tail after such a rename. `history.keep_scans` bounds both to the newest N
**complete** scans per base — incomplete scans do not count toward the window, so
a run of failures cannot age out real history.

Two prunes share one cutoff: change rows strictly before it, and `files` rows
that are `deleted` before it. `presence = 'unreadable'` is live state, never
absence, and is never pruned. `scans`, `scan_errors`, and `scan_stage_entries`
are kept; keeping `scans` is what makes this FK-safe, since an unchanged file's
`metadata_from_scan_id` still points at whatever ancient scan supplied it.

It runs after promotion and **outside its transaction** — promotion is the one
atomic step that moves the baseline, and folding a large delete into it would
stretch the window where a crash discards the scan. A prune failure is reported
and swallowed rather than undoing a scan that already succeeded. Deletes are
batched at `writer_batch_rows` per statement, because one giant transaction
builds a WAL the size of the thing being shrunk.

Default is 0 — keep everything. Deleting a user's history because they upgraded
is not a default worth choosing. Measured cost is ~180 bytes per change row
against ~667 per file row, so at 1% churn each retained scan costs roughly 0.4%
of the baseline; the window only matters after a big rename.

Deleting rows does not shrink the file — freed pages are reused, so it stops
growing. Reclaiming bytes needs `VACUUM INTO`, which `prune` prints rather than
attempts. Creating the database with `auto_vacuum = INCREMENTAL` would let prune
return space directly, but that cannot be switched on afterwards without a full
`VACUUM`; worth considering at the next schema break.

## Views

`current_files` filters `presence = 'present'` and joins `bases` for an absolute
path, because the default click in a browser is `SELECT * FROM files` — which
silently includes `deleted` and `unreadable` rows. `content_hash` is exposed as
hex so it compares directly against `sha256sum`; the text paths are lossy in
exactly the way the `parent_path` / `name` / `extension` helper columns already
are, and `relative_path` is carried through unchanged as the authority.

Views hold no data, so they are **dropped and rebuilt on every open** rather than
guarded with `IF NOT EXISTS`. A changed definition therefore cannot go stale in
an existing database, and adding a view needs neither a `SCHEMA_VERSION` bump nor
a recreation — the one kind of schema change that costs the user nothing.

## Open

Nothing. All prior entries are settled: file-level errors shield rather than
block, the free-space gate is a floor rather than a prediction, and history is
bounded.

## Future

Roughly in the order they are likely to matter.

- **Do not add a foreign key from `scan_changes` to `files`.** `scan_changes`
  stores `relative_path` deliberately, so dead `files` rows can be pruned without
  touching history. An FK would couple the two prunes together.
- **`status` and `doctor`.** Read-only diagnostics: configured path, known bases,
  latest and resumable scans; then schema checks, `quick_check`, file modes, free
  space, stale staging. Add when a real need appears.
- **Rename detection** — only on an unambiguous one-to-one match, reported as an
  inference, never as fact. Capturing device/inode would strengthen a
  same-filesystem detector; cross-filesystem moves stay unprovable.
- **`always_hash` policy** and **content sniffing** for MIME (currently extension
  or `unknown`).
