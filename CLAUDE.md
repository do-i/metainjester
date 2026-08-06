# metainjester

Rust CLI that catalogues a directory tree into SQLite and reports what changed
on later runs. Design doc: `../cms/docs/2026-07-26-007.md` plus dated deltas in
that directory. Those docs are **immutable** — record changes as a new
`docs/YYYY-MM-DD-NNN.md` delta, a few lines, never an edit.

## State

Step 1 POC done (`3de78b6`): `metainjester ingest <path>` walks a tree, skips
hidden entries and symlinks, hashes each file with SHA-256, and upserts rows.
Verified: hashes match `sha256sum`, spaced paths survive, rerun is idempotent,
`~` expanded in-app. ~150 lines in `src/main.rs`, two tables.

```sql
bases(base_id, base_path)
files(file_id, base_id, relative_path, size_bytes, mtime_ns, content_hash)
UNIQUE(base_id, relative_path)
```

## Next: step 2 — rescan diff

- Add `presence` to `files`; add `scans` and `scan_changes` tables
- Detect added / updated / deleted against existing rows
- Reuse the stored hash when size + mtime both match
- Print `added N  updated N  deleted N  unchanged N`

Deferred past step 2: staging table, resume, config file, workers, MIME,
free-space preflight, `status`/`doctor`.

## Working rules

- POC first: lean, small, just works. **No README, no tests yet** — added after
  the user verifies behavior.
- Keep replies and files short. Reading is the user's bottleneck.
- One-line git commit messages, no trailers.
- Schema changes just recreate the POC database; no migrations yet.
- Known POC shortcuts: `relative_path` is lossy TEXT (design says BINARY), and
  the database path is hardcoded to `./metainjester.sqlite3`.
- `lazymenu-cli` is installed; `menu.toml` drives build/ingest/test/lint.
