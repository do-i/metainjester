# metainjester

Rust CLI that catalogues a directory tree into SQLite and reports what changed
on later runs. Design doc: `docs/design.md` — intent, invariants, and what is
still open. It is version-controlled, so git history replaces the old
dated-delta scheme; edit it in place and keep the diff small.

## State

MVP complete and unverified by the user. Steps 1-3 (`3de78b6`, `264cc6d`,
`fab5dac`) built ingest, the rescan diff, and staged/atomic/resumable scans.
The current commit adds the rest of the design: configuration, database
discovery, inclusion policy, MIME, worker pipeline, free-space preflight,
`scan_errors`, and progress. Where the build departs from the design, the
reason is noted in `docs/design.md`.

```text
metainjester ingest <base-path>          # scan; also the resume command
metainjester history prune [--apply]     # bound history; preview unless --apply
```

Policy and storage come from configuration, never from flags.

### Modules

- `config.rs` — TOML load and validation. `/etc/xdg/...` then `~/.config/...`
  on top. Unknown key, bad type, bad enum, or malformed file is a startup error.
- `db.rs` — discovery per §2, schema v1, pragmas (WAL, `synchronous = FULL`,
  `foreign_keys`), and the `writer_lock` single-writer guard.
- `walk.rs` — traversal, the three inclusion policies, MIME, path helpers.
- `scan.rs` — the §7 lifecycle: preflight, stage, validate, promote.
- `history.rs` — retention: the cutoff, and the two batched prunes.

### Pipeline

`walker -> bounded queue -> N hash workers -> bounded queue -> staging writer`.
Only the writer holds the write connection; each worker opens its own read-only
connection, which WAL permits during a scan. Memory is bounded by queue and
batch size, not tree size.

Reuse order per file: a durable staged row from this scan, then the `files`
baseline, then hash. An unchanged rerun hashes nothing and rewrites no rows.

### Lifecycle

Only a fully traversed, uncancelled scan promotes. Promotion is one transaction:
write `scan_changes`, upsert `files`, mark absences under an unreadable path
`unreadable` and every other absence `deleted`, move
`bases.last_complete_scan_id`, drain staging. Anything else keeps its staging and
leaves the baseline untouched, and the next `ingest` on that base resumes it.

File-level errors do not block promotion. `permission_denied`, `io_error`, and
`invalid_data` (`walk::UNREADABLE_CODES`) shield the failing path and everything
beneath it from deletion — the byte-wise prefix test in `scan::shielded`, never
string concatenation, since `relative_path` is a BLOB. An unreadable base is the
exception and yields `partial` / `base_unreadable`: it prefixes every path, so
nothing can be shielded. `unreadable` rows return to `present` on a scan that can
read them again, with no change row unless the file itself changed.

Free space is a floor, not an estimate: `storage.minimum_free_space_mib`
(default 500) is checked in preflight and again after every committed batch in
`writer`. Crossing it sets `low_space`, which cancels the pipeline, yields
`partial` / `low_space`, and exits 5 like the up-front refusal. Nothing predicts
a scan's size — there is no expected-file-count math and no per-file reservation.

`history.keep_scans` (default 0 = keep everything) bounds `scan_changes` and the
dead tail of `deleted` `files` rows to the newest N **complete** scans per base.
It runs after promotion, outside that transaction, and a failure is reported not
fatal. `unreadable` rows are never pruned; `scans` rows are always kept, which is
what keeps it FK-safe. Deletes are batched at `writer_batch_rows`. Pruning frees
pages for reuse but does not shrink the file — that needs `VACUUM INTO`.

Exit codes: `0` ok, `1` error/partial, `2` usage, `3` config, `4` busy,
`5` no space, `6` policy mismatch, `130` cancelled.

### Schema v1

```sql
application_metadata(key, value)          -- application_id, schema_version = 2
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
  presence: present | deleted | unreadable
scan_stage_entries(...)  UNIQUE(scan_id, relative_path)
scan_changes(change_id, scan_id, base_id, relative_path, change_kind,
             field_mask, old_/new_ size_bytes, mtime_ns, mime_type, content_hash)
scan_errors(error_id, scan_id, relative_path, stage, error_code, message)
```

`relative_path` is BINARY; the three helper columns are lossy TEXT for query
convenience. `field_mask` bits: size=1, mtime=2, MIME=4, hash=8, presence=16.

View `current_files` — `presence = 'present'` joined to `bases` for
`absolute_path`, plus `content_hash_hex`. Views are dropped and rebuilt in
`ensure_schema` on every open, so changing one needs no `SCHEMA_VERSION` bump and
no recreation.

## Verified

34,741 files (`/usr/include`, 341 MiB) in 7.6s; rerun 0.45s hashing nothing;
`pragma quick_check` ok; ~667 bytes per file row. Random hashes confirmed with
`sha256sum -c`.

Also: SIGKILL and SIGINT mid-scan leave the baseline untouched and resume
correctly (exit 130 on SIGINT); a file deleted between attempts never reaches
`files`; a locked directory promotes with its rows held `unreadable` and no
deletion recorded, returns to `present` with zero change rows once the permission
is restored, and records `deleted` if the file really went away meanwhile, while
an unreadable base still yields `partial` with the baseline intact; the
free-space floor refuses up front and also stops mid-scan (both exit 5), with the
interrupted attempt resuming off its staged rows without rehashing them; policy
change is refused with `policy_mismatch`; foreign and other-application databases in the working
directory are refused; a stale writer lock from a dead pid is reclaimed; mount
boundaries skipped and crossed per policy (verified under `unshare -rm`);
symlink loops terminate; rename yields `deleted` + `added`; hidden entries and
symlinks excluded by count only.

## Next

Nothing is planned — waiting on the user's testing, then delta updates.

Post-MVP in the design: `status`, `doctor`, rename detection, content sniffing,
always-hash policy.

## Packaging

Hosted on **GitHub** (`do-i/metainjester`), following thumbgrid. Actions supply
`GITHUB_TOKEN` to every run, so **the release path has no secrets to manage** —
nothing to create, store, or rotate.

- `.github/workflows/checks.yml` — build, clippy, `cargo test` on develop/main
  and PRs. This is the gate `release.sh` requires before it will tag.
- `.github/workflows/arch-package.yml` — on a `v*` tag: build the package in
  `archlinux:base-devel`, attach it to the GitHub release, and refresh the
  pacman repo on `gh-pages` under `arch/x86_64/`. `workflow_dispatch` with a
  `pkgrel` input republishes an ABI-only rebuild (metainjester links the system
  SQLite) without a new version.
- `scripts/release.sh` — `status` and `cut`; also in `menu.toml` under Release.

Versioning is calendar-based `year.month.sequence`, tagged `v2026.8.1`. The tag
must equal `version` in Cargo.toml or the build refuses. Licence is MIT, text
shipped to `/usr/share/licenses/metainjester/`.

### Branching

`develop` is the integration branch and the only branch `cut` runs from. `main`
never takes a direct commit — it only fast-forwards from develop, then gets
tagged. `cut` refuses if main is not an ancestor of develop.

Because the package is built from Cargo.toml (thumbgrid derives its version at
build time instead), `cut` bumps Cargo.toml **and Cargo.lock** as its own commit
on develop, waits for a green `checks.yml` on it, then fast-forwards and tags in
one atomic push. The lock matters: makepkg builds `--frozen`.

Consumers add to `/etc/pacman.conf`:

```ini
[metainjester]
SigLevel = Optional TrustAll
Server = https://do-i.github.io/metainjester/arch/$arch
```

A pacman repo lists one version per package name — `repo-add` replaces the
previous entry rather than accumulating. Older `.pkg.tar.zst` files stay in
`arch/x86_64/` for a manual `pacman -U`.

`repo-add` leaves `<repo>.db` as a symlink, which is the exact name pacman
fetches. GitHub Pages runs Jekyll in safe mode and **ignores symlinks**, so the
workflow replaces them with resolved copies and writes `.nojekyll`. Without
that, every `pacman -Sy` 404s.

Verified locally on the gh-pages layout: a real `pacman -Sy` / `-Sl` / `-Si` /
`-S` against a `file://` copy (with `$arch` expanding) installs into an isolated
root and runs; a second release is offered as `2026.8.1 -> 2026.8.2` with both
package files retained.

Verified on GitHub with the `v2026.8.1` release: `checks.yml` green on main and
develop, `arch-package.yml` green, the package attached to the release, and
`gh-pages` published with `metainjester.db` as a **regular file** (mode 100644,
not a symlink) plus `.nojekyll`.

Verified against the live Pages URL, which is the whole chain end to end: a real
`pacman -Sy` / `-Sl` / `-Si` / `-S` over
`https://do-i.github.io/metainjester/arch/$arch` installs 2026.8.1-1 into an
isolated root, and the downloaded binary runs. The fetched package is byte-identical
to the release asset (486,253 bytes). Nothing about the release path is
unverified now.

**One-time setup, both done:** repo public, Pages serving `gh-pages` at
`/ (root)`. Packages are unsigned (`SigLevel = Optional TrustAll`).

## Working rules

- POC first: lean, small, just works. **No README, no tests yet** — added after
  the user verifies behavior.
- Keep replies and files short. Reading is the user's bottleneck.
- One-line git commit messages, no trailers.
- Schema changes just recreate the database; no migrations yet. **Bump
  `SCHEMA_VERSION` in `db.rs` on every schema change** — `CREATE TABLE IF NOT
  EXISTS` cannot add a column to an existing table, so without the bump an older
  database fails later on a missing column instead of refusing up front.
- `lazymenu-cli` is installed; `menu.toml` drives build/ingest/test/lint.
