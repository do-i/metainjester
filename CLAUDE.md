# metainjester

Rust CLI that catalogues a directory tree into SQLite and reports what changed
on later runs. Design doc: `../cms/docs/2026-07-26-007.md` plus dated deltas in
that directory. Those docs are **immutable** — record changes as a new
`docs/YYYY-MM-DD-NNN.md` delta, a few lines, never an edit.

## State

Step 1 POC done (`3de78b6`). Step 2 rescan diff done: each ingest opens a
`scans` row, diffs the tree against `files`, writes one `scan_changes` row per
added/updated/deleted path, and prints
`added N  updated N  deleted N  unchanged N`. The stored hash is reused when
size and mtime both match, so an unchanged rerun hashes nothing and rewrites no
rows. Deletion flips `presence` rather than removing the row, so a reappearance
is recognized as `added` with the old values kept in the change row.

Verified: fresh scan, no-op rerun, edit + delete + add in one pass, resurrection
of a deleted path, mtime-only touch (`field_mask = 2`, rehash, `updated`),
hashes still match `sha256sum`, spaced paths survive.

```sql
bases(base_id, base_path)
scans(scan_id, base_id, status, started_at_ns, finished_at_ns,
      added_count, updated_count, deleted_count, unchanged_count, error_count)
files(file_id, base_id, relative_path, presence, size_bytes, mtime_ns,
      content_hash, metadata_from_scan_id, deleted_in_scan_id)
  UNIQUE(base_id, relative_path)
scan_changes(change_id, scan_id, base_id, relative_path, change_kind,
             field_mask, old_/new_ size_bytes, mtime_ns, content_hash)
```

`field_mask` bits: size=1, mtime=2, MIME=4 (unused), hash=8, presence=16.

## Next: step 3 — not yet chosen

Deferred: staging table (the diff is in memory, so a scan is not resumable and
promotion is not atomic per the design's §7), resume, config file, workers,
MIME, free-space preflight, `status`/`doctor`, `scan_errors` (errors are counted
on `scans` and printed, not itemized).

## Working rules

- POC first: lean, small, just works. **No README, no tests yet** — added after
  the user verifies behavior.
- Keep replies and files short. Reading is the user's bottleneck.
- One-line git commit messages, no trailers.
- Schema changes just recreate the POC database; no migrations yet.
- Known POC shortcuts: `relative_path` is lossy TEXT (design says BINARY), and
  the database path is hardcoded to `./metainjester.sqlite3`.
- `lazymenu-cli` is installed; `menu.toml` drives build/ingest/test/lint.
