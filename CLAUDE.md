# metainjester

Rust CLI that catalogues a directory tree into SQLite and reports what changed
on later runs. Design doc: `../cms/docs/2026-07-26-007.md` plus dated deltas in
that directory. Those docs are **immutable** — record changes as a new
`docs/YYYY-MM-DD-NNN.md` delta, a few lines, never an edit.

## State

Step 1 POC done (`3de78b6`), step 2 rescan diff done (`264cc6d`). Step 3 done:
staging, atomic promotion, and resume. The walk no longer touches `files`. It
stages every observed file into `scan_stage_entries` in 512-row transactions;
only a complete, uncancelled, error-free walk promotes, and promotion is one
transaction that writes `scan_changes`, upserts `files`, marks absences
`deleted`, moves `bases.last_complete_scan_id`, and drains the staging.

`ingest` is also the resume command. A scan left `running` (crash), `cancelled`
(Ctrl-C), `partial` (error), or `failed` is picked up by the next ingest on the
same base rather than restarted. Every attempt re-walks, so staged rows for
paths that have since disappeared are pruned before promotion. A staged row is
reused only when size and mtime still match exactly; otherwise the path is
rehashed. Baseline hash reuse from step 2 still applies on top.

Reuse order per file: durable staged row from this scan, then the `files`
baseline, then hash. So an unchanged rerun hashes nothing and rewrites no rows,
and a resumed scan rehashes only what it had not already finished.

```sql
bases(base_id, base_path, last_complete_scan_id)
scans(scan_id, base_id, baseline_scan_id, status, started_at_ns,
      finished_at_ns, added_count, updated_count, deleted_count,
      unchanged_count, error_count)
  status: running | complete | partial | cancelled | failed
files(file_id, base_id, relative_path, presence, size_bytes, mtime_ns,
      content_hash, metadata_from_scan_id, deleted_in_scan_id)
  UNIQUE(base_id, relative_path)
scan_stage_entries(scan_id, base_id, relative_path, size_bytes, mtime_ns,
                   content_hash, complete)  UNIQUE(scan_id, relative_path)
scan_changes(change_id, scan_id, base_id, relative_path, change_kind,
             field_mask, old_/new_ size_bytes, mtime_ns, content_hash)
```

`field_mask` bits: size=1, mtime=2, MIME=4 (unused), hash=8, presence=16.

Verified on a 2000-file tree: SIGKILL mid-scan leaves `status = running` with
`files` untouched and one batch staged; the next ingest resumes the same
`scan_id`, reuses exactly the surviving staged rows, and promotes once. Ctrl-C
exits 130 as `cancelled` and promotes nothing. An unreadable *new* file makes
the scan `partial`, exits 1, and holds the baseline; fixing the permission and
rerunning completes it. A file deleted between attempts never reaches `files`.
Rename yields one `deleted` + one `added`. Hashes match `sha256sum` across all
2000 files. Also still: no-op rerun, resurrection, mtime-only touch, spaced
paths.

**Any file-level error blocks promotion** (design §7 / acceptance test 3), so one
permanently unreadable file will stall a base until it is fixed or excluded.
Faithful to the design, but the first thing to revisit if it gets annoying.

## Next: step 4 — not yet chosen

Deferred: config file + inclusion policy (`policy_mismatch`), MIME, hashing
workers, free-space preflight, `status`/`doctor`, `application_metadata`,
`scan_errors` (errors are counted and printed, not itemized).

## Working rules

- POC first: lean, small, just works. **No README, no tests yet** — added after
  the user verifies behavior.
- Keep replies and files short. Reading is the user's bottleneck.
- One-line git commit messages, no trailers.
- Schema changes just recreate the POC database; no migrations yet.
- Known POC shortcuts: `relative_path` is lossy TEXT (design says BINARY), the
  database path is hardcoded to `./metainjester.sqlite3`, the walker collects
  every path into a `Vec` before staging, and hashing is single-threaded.
- `lazymenu-cli` is installed; `menu.toml` drives build/ingest/test/lint.
