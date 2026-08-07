# metainjester

Rust CLI that catalogues a directory tree into SQLite and reports what changed
on later runs. Design doc: `../cms/docs/2026-07-26-007.md` plus dated deltas in
that directory. Those docs are **immutable** — record changes as a new
`docs/YYYY-MM-DD-NNN.md` delta, a few lines, never an edit.

## State

MVP complete and unverified by the user. Steps 1-3 (`3de78b6`, `264cc6d`,
`fab5dac`) built ingest, the rescan diff, and staged/atomic/resumable scans.
The current commit adds the rest of the design: configuration, database
discovery, inclusion policy, MIME, worker pipeline, free-space preflight,
`scan_errors`, and progress. Implementation deviations are recorded in
`../cms/docs/2026-08-06-002.md`.

```text
metainjester ingest <base-path>      # the entire command surface
```

`ingest` is also the resume command. Policy and storage come from configuration,
never from flags.

### Modules

- `config.rs` — TOML load and validation. `/etc/xdg/...` then `~/.config/...`
  on top. Unknown key, bad type, bad enum, or malformed file is a startup error.
- `db.rs` — discovery per §2, schema v1, pragmas (WAL, `synchronous = FULL`,
  `foreign_keys`), and the `writer_lock` single-writer guard.
- `walk.rs` — traversal, the three inclusion policies, MIME, path helpers.
- `scan.rs` — the §7 lifecycle: preflight, stage, validate, promote.

### Pipeline

`walker -> bounded queue -> N hash workers -> bounded queue -> staging writer`.
Only the writer holds the write connection; each worker opens its own read-only
connection, which WAL permits during a scan. Memory is bounded by queue and
batch size, not tree size.

Reuse order per file: a durable staged row from this scan, then the `files`
baseline, then hash. An unchanged rerun hashes nothing and rewrites no rows.

### Lifecycle

Only a fully traversed, uncancelled, error-free scan promotes. Promotion is one
transaction: write `scan_changes`, upsert `files`, mark absences `deleted`, move
`bases.last_complete_scan_id`, drain staging. Anything else keeps its staging and
leaves the baseline untouched, and the next `ingest` on that base resumes it.

Exit codes: `0` ok, `1` error/partial, `2` usage, `3` config, `4` busy,
`5` no space, `6` policy mismatch, `130` cancelled.

### Schema v1

```sql
application_metadata(key, value)          -- application_id, schema_version
writer_lock(id, pid, acquired_at_ns)
bases(base_id, base_path, created_at_ns, last_complete_scan_id,
      skip_hidden, skip_mount_boundaries, follow_symlinks, last_error)
scans(scan_id, base_id, baseline_scan_id, status, started/finished_at_ns,
      <policy + workers/batch/throttle/hash_policy>, <work totals>,
      <outcome counts>, <exclusion counts>, <capacity>, failure_code/message)
  status: running | complete | partial | cancelled | failed
files(file_id, base_id, relative_path, parent_path, name, extension, presence,
      size_bytes, mtime_ns, created_ns, mime_type, mime_source,
      hash_algorithm, content_hash, hash_status,
      metadata_from_scan_id, deleted_in_scan_id)
scan_stage_entries(...)  UNIQUE(scan_id, relative_path)
scan_changes(change_id, scan_id, base_id, relative_path, change_kind,
             field_mask, old_/new_ size_bytes, mtime_ns, mime_type, content_hash)
scan_errors(error_id, scan_id, relative_path, stage, error_code, message)
```

`relative_path` is BINARY; the three helper columns are lossy TEXT for query
convenience. `field_mask` bits: size=1, mtime=2, MIME=4, hash=8, presence=16.

## Verified

34,741 files (`/usr/include`, 341 MiB) in 7.6s; rerun 0.45s hashing nothing;
`pragma quick_check` ok; ~667 bytes per file row. Random hashes confirmed with
`sha256sum -c`.

Also: SIGKILL and SIGINT mid-scan leave the baseline untouched and resume
correctly (exit 130 on SIGINT); a file deleted between attempts never reaches
`files`; unreadable file and change-during-hash both yield `partial` with the
baseline intact, then complete on retry; policy change is refused with
`policy_mismatch`; foreign and other-application databases in the working
directory are refused; a stale writer lock from a dead pid is reclaimed; mount
boundaries skipped and crossed per policy (verified under `unshare -rm`);
symlink loops terminate; rename yields `deleted` + `added`; hidden entries and
symlinks excluded by count only.

## Next

Nothing is planned — waiting on the user's testing, then delta updates.

Post-MVP in the design: `history prune`, `status`, `doctor`, rename detection,
content sniffing, always-hash policy.

## Working rules

- POC first: lean, small, just works. **No README, no tests yet** — added after
  the user verifies behavior.
- Keep replies and files short. Reading is the user's bottleneck.
- One-line git commit messages, no trailers.
- Schema changes just recreate the database; no migrations yet. A version
  mismatch refuses to open rather than guessing.
- `lazymenu-cli` is installed; `menu.toml` drives build/ingest/test/lint.
