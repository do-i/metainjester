use std::fs;
use std::io::Read;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use rusqlite::{params, Connection, OptionalExtension};
use sha2::{Digest, Sha256};

const DB_FILE: &str = "metainjester.sqlite3";

/// Staged rows committed per transaction. Bounds the work an interrupt can lose.
const BATCH: usize = 512;

/// `field_mask` bits in `scan_changes`. MIME (4) is unused until MIME lands.
const F_SIZE: i64 = 1;
const F_MTIME: i64 = 2;
const F_HASH: i64 = 8;
const F_PRESENCE: i64 = 16;
const F_ADDED: i64 = F_SIZE | F_MTIME | F_HASH | F_PRESENCE;

const SCHEMA: &str = "
CREATE TABLE IF NOT EXISTS bases (
    base_id   INTEGER PRIMARY KEY,
    base_path TEXT NOT NULL UNIQUE,
    last_complete_scan_id INTEGER
);
CREATE TABLE IF NOT EXISTS scans (
    scan_id        INTEGER PRIMARY KEY,
    base_id        INTEGER NOT NULL REFERENCES bases(base_id),
    baseline_scan_id INTEGER,
    status         TEXT NOT NULL,
    started_at_ns  INTEGER NOT NULL,
    finished_at_ns INTEGER,
    added_count     INTEGER,
    updated_count   INTEGER,
    deleted_count   INTEGER,
    unchanged_count INTEGER,
    error_count     INTEGER
);
CREATE TABLE IF NOT EXISTS files (
    file_id       INTEGER PRIMARY KEY,
    base_id       INTEGER NOT NULL REFERENCES bases(base_id),
    relative_path TEXT NOT NULL,
    presence      TEXT NOT NULL,
    size_bytes    INTEGER NOT NULL,
    mtime_ns      INTEGER NOT NULL,
    content_hash  BLOB NOT NULL,
    metadata_from_scan_id INTEGER NOT NULL REFERENCES scans(scan_id),
    deleted_in_scan_id    INTEGER REFERENCES scans(scan_id),
    UNIQUE(base_id, relative_path)
);
CREATE INDEX IF NOT EXISTS files_by_base_presence ON files(base_id, presence);
CREATE TABLE IF NOT EXISTS scan_stage_entries (
    scan_id       INTEGER NOT NULL REFERENCES scans(scan_id),
    base_id       INTEGER NOT NULL REFERENCES bases(base_id),
    relative_path TEXT NOT NULL,
    size_bytes    INTEGER NOT NULL,
    mtime_ns      INTEGER NOT NULL,
    content_hash  BLOB NOT NULL,
    complete      INTEGER NOT NULL,
    UNIQUE(scan_id, relative_path)
);
CREATE TABLE IF NOT EXISTS scan_changes (
    change_id     INTEGER PRIMARY KEY,
    scan_id       INTEGER NOT NULL REFERENCES scans(scan_id),
    base_id       INTEGER NOT NULL REFERENCES bases(base_id),
    relative_path TEXT NOT NULL,
    change_kind   TEXT NOT NULL,
    field_mask    INTEGER NOT NULL,
    old_size_bytes   INTEGER,
    new_size_bytes   INTEGER,
    old_mtime_ns     INTEGER,
    new_mtime_ns     INTEGER,
    old_content_hash BLOB,
    new_content_hash BLOB
);
CREATE INDEX IF NOT EXISTS scan_changes_by_scan ON scan_changes(scan_id, change_kind);
";

fn main() {
    match run() {
        Ok(code) => std::process::exit(code),
        Err(e) => {
            eprintln!("metainjester: {e}");
            std::process::exit(1);
        }
    }
}

fn run() -> Result<i32, Box<dyn std::error::Error>> {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let base = match args.as_slice() {
        [cmd, path] if cmd == "ingest" => fs::canonicalize(expand_tilde(path))?,
        _ => return Err("usage: metainjester ingest <base-path>".into()),
    };
    if !base.is_dir() {
        return Err(format!("not a directory: {}", base.display()).into());
    }

    let cancelled = Arc::new(AtomicBool::new(false));
    {
        let flag = cancelled.clone();
        ctrlc::set_handler(move || flag.store(true, Ordering::SeqCst))?;
    }

    let started = Instant::now();
    let mut conn = Connection::open(DB_FILE)?;
    conn.execute_batch(SCHEMA)?;

    let base_str = base.to_string_lossy().to_string();
    conn.execute(
        "INSERT OR IGNORE INTO bases (base_path) VALUES (?1)",
        params![base_str],
    )?;
    let (base_id, baseline): (i64, Option<i64>) = conn.query_row(
        "SELECT base_id, last_complete_scan_id FROM bases WHERE base_path = ?1",
        params![base_str],
        |r| Ok((r.get(0)?, r.get(1)?)),
    )?;

    // A scan left in a non-terminal or failed state is resumed, not restarted:
    // its staged rows are durable completed work. Everything else starts fresh.
    let resumable: Option<i64> = conn
        .query_row(
            "SELECT scan_id FROM scans
             WHERE base_id = ?1 AND status IN ('running', 'cancelled', 'failed', 'partial')
             ORDER BY scan_id DESC LIMIT 1",
            params![base_id],
            |r| r.get(0),
        )
        .optional()?;

    let (scan_id, resumed) = match resumable {
        Some(id) => (id, true),
        None => {
            // No scan owns these rows; clearing them is safe.
            conn.execute(
                "DELETE FROM scan_stage_entries WHERE base_id = ?1",
                params![base_id],
            )?;
            conn.execute(
                "INSERT INTO scans (base_id, baseline_scan_id, status, started_at_ns)
                 VALUES (?1, ?2, 'running', ?3)",
                params![base_id, baseline, now_ns()],
            )?;
            (conn.last_insert_rowid(), false)
        }
    };
    if resumed {
        conn.execute(
            "UPDATE scans SET status = 'running' WHERE scan_id = ?1",
            params![scan_id],
        )?;
    }

    let mut paths = Vec::new();
    let mut errors = 0usize;
    walk(&base, &mut paths, &mut errors);

    // Every attempt re-walks, so a path staged earlier but gone from disk now must
    // not survive into promotion as present. Record what this walk actually saw.
    conn.execute_batch(
        "CREATE TEMP TABLE walk_paths (relative_path TEXT PRIMARY KEY);",
    )?;

    let mut hashed = 0usize;
    let mut reused_stage = 0usize;
    let mut total_bytes = 0i64;
    let mut stopped = false;

    let mut i = 0usize;
    while i < paths.len() {
        let tx = conn.transaction()?;
        {
            // Reuse rules, in order: a durable staged row from this scan, then the
            // baseline hash. Both require size and mtime to match exactly.
            let mut staged_lookup = tx.prepare(
                "SELECT size_bytes, mtime_ns FROM scan_stage_entries
                 WHERE scan_id = ?1 AND relative_path = ?2 AND complete = 1",
            )?;
            let mut baseline_lookup = tx.prepare(
                "SELECT size_bytes, mtime_ns, content_hash FROM files
                 WHERE base_id = ?1 AND relative_path = ?2 AND presence = 'present'",
            )?;
            let mut stage = tx.prepare(
                "INSERT INTO scan_stage_entries
                     (scan_id, base_id, relative_path, size_bytes, mtime_ns, content_hash, complete)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, 1)
                 ON CONFLICT(scan_id, relative_path) DO UPDATE SET
                     size_bytes = excluded.size_bytes,
                     mtime_ns = excluded.mtime_ns,
                     content_hash = excluded.content_hash,
                     complete = 1",
            )?;
            let mut seen = tx.prepare(
                "INSERT OR IGNORE INTO walk_paths (relative_path) VALUES (?1)",
            )?;

            let end = (i + BATCH).min(paths.len());
            for path in &paths[i..end] {
                if cancelled.load(Ordering::SeqCst) {
                    stopped = true;
                    break;
                }
                let rel = path
                    .strip_prefix(&base)
                    .unwrap()
                    .to_string_lossy()
                    .to_string();
                seen.execute(params![rel])?;
                let meta = match fs::metadata(path) {
                    Ok(m) => m,
                    Err(e) => {
                        eprintln!("  error {rel}: {e}");
                        errors += 1;
                        continue;
                    }
                };
                let size = meta.len() as i64;
                let mtime_ns = meta
                    .modified()
                    .ok()
                    .and_then(|t| t.duration_since(UNIX_EPOCH).ok())
                    .map(|d| d.as_nanos() as i64)
                    .unwrap_or(0);
                total_bytes += size;

                if let Some((s, m)) = staged_lookup
                    .query_row(params![scan_id, rel], |r| Ok((r.get::<_, i64>(0)?, r.get::<_, i64>(1)?)))
                    .optional()?
                    && s == size
                    && m == mtime_ns
                {
                    reused_stage += 1;
                    continue;
                }

                let prior = baseline_lookup
                    .query_row(params![base_id, rel], |r| {
                        Ok((r.get::<_, i64>(0)?, r.get::<_, i64>(1)?, r.get::<_, Vec<u8>>(2)?))
                    })
                    .optional()?;
                let hash = match prior {
                    Some((s, m, h)) if s == size && m == mtime_ns => h,
                    _ => match hash_file(path) {
                        Ok(h) => {
                            hashed += 1;
                            h
                        }
                        Err(e) => {
                            eprintln!("  error {rel}: {e}");
                            errors += 1;
                            continue;
                        }
                    },
                };
                stage.execute(params![
                    scan_id,
                    base_id,
                    rel,
                    size,
                    mtime_ns,
                    hash.as_slice()
                ])?;
            }
            i = end;
        }
        tx.commit()?;
        if stopped {
            break;
        }
    }

    // Drop staged rows for paths this walk no longer sees. Only a full walk proves
    // absence, so a stopped scan keeps its staging untouched for the next attempt.
    if !stopped {
        conn.execute(
            "DELETE FROM scan_stage_entries WHERE scan_id = ?1
             AND relative_path NOT IN (SELECT relative_path FROM walk_paths)",
            params![scan_id],
        )?;
    }
    let staged: usize = conn.query_row(
        "SELECT COUNT(*) FROM scan_stage_entries WHERE scan_id = ?1 AND complete = 1",
        params![scan_id],
        |r| r.get::<_, i64>(0).map(|n| n as usize),
    )?;

    // A walk that did not finish, or that could not observe every file, cannot
    // establish deletions. Keep the staging and leave the baseline untouched.
    if stopped || errors > 0 {
        let status = if stopped { "cancelled" } else { "partial" };
        conn.execute(
            "UPDATE scans SET status = ?1, finished_at_ns = ?2, error_count = ?3
             WHERE scan_id = ?4",
            params![status, now_ns(), errors as i64, scan_id],
        )?;
        println!("base    {}", base.display());
        println!("scan    {scan_id} ({status}, not promoted)");
        println!("staged  {staged}");
        println!("hashed  {hashed}");
        println!("errors  {errors}");
        println!("elapsed {:.2}s", started.elapsed().as_secs_f64());
        println!("rerun `ingest` on the same path to resume");
        return Ok(if stopped { 130 } else { 1 });
    }

    let (added, updated, deleted, unchanged) = promote(&mut conn, scan_id, base_id, staged)?;

    println!("base    {}", base.display());
    println!("scan    {scan_id}{}", if resumed { " (resumed)" } else { "" });
    println!("files   {staged}");
    println!("bytes   {total_bytes}");
    println!("hashed  {hashed}");
    if resumed {
        println!("staged  {reused_stage} reused");
    }
    println!("errors  {errors}");
    println!("elapsed {:.2}s", started.elapsed().as_secs_f64());
    println!("db      {DB_FILE}");
    println!("added {added}  updated {updated}  deleted {deleted}  unchanged {unchanged}");
    Ok(0)
}

/// Turns a fully staged scan into the new baseline in one transaction: change
/// rows first (they read the outgoing `files` state), then the state itself.
fn promote(
    conn: &mut Connection,
    scan_id: i64,
    base_id: i64,
    staged: usize,
) -> Result<(usize, usize, usize, usize), Box<dyn std::error::Error>> {
    let tx = conn.transaction()?;

    // Added: never seen, or seen and since deleted. A resurrected path keeps its
    // old values in the change row.
    let added = tx.execute(
        "INSERT INTO scan_changes
             (scan_id, base_id, relative_path, change_kind, field_mask,
              old_size_bytes, new_size_bytes, old_mtime_ns, new_mtime_ns,
              old_content_hash, new_content_hash)
         SELECT s.scan_id, s.base_id, s.relative_path, 'added', ?2,
                f.size_bytes, s.size_bytes, f.mtime_ns, s.mtime_ns,
                f.content_hash, s.content_hash
         FROM scan_stage_entries s
         LEFT JOIN files f ON f.base_id = s.base_id AND f.relative_path = s.relative_path
         WHERE s.scan_id = ?1 AND s.complete = 1
           AND (f.file_id IS NULL OR f.presence = 'deleted')",
        params![scan_id, F_ADDED],
    )?;

    let updated = tx.execute(
        "INSERT INTO scan_changes
             (scan_id, base_id, relative_path, change_kind, field_mask,
              old_size_bytes, new_size_bytes, old_mtime_ns, new_mtime_ns,
              old_content_hash, new_content_hash)
         SELECT s.scan_id, s.base_id, s.relative_path, 'updated',
                (CASE WHEN f.size_bytes   <> s.size_bytes   THEN ?2 ELSE 0 END)
              + (CASE WHEN f.mtime_ns     <> s.mtime_ns     THEN ?3 ELSE 0 END)
              + (CASE WHEN f.content_hash <> s.content_hash THEN ?4 ELSE 0 END),
                f.size_bytes, s.size_bytes, f.mtime_ns, s.mtime_ns,
                f.content_hash, s.content_hash
         FROM scan_stage_entries s
         JOIN files f ON f.base_id = s.base_id AND f.relative_path = s.relative_path
         WHERE s.scan_id = ?1 AND s.complete = 1 AND f.presence = 'present'
           AND (f.size_bytes <> s.size_bytes OR f.mtime_ns <> s.mtime_ns
                OR f.content_hash <> s.content_hash)",
        params![scan_id, F_SIZE, F_MTIME, F_HASH],
    )?;

    let deleted = tx.execute(
        "INSERT INTO scan_changes
             (scan_id, base_id, relative_path, change_kind, field_mask,
              old_size_bytes, new_size_bytes, old_mtime_ns, new_mtime_ns,
              old_content_hash, new_content_hash)
         SELECT ?1, f.base_id, f.relative_path, 'deleted', ?3,
                f.size_bytes, NULL, f.mtime_ns, NULL, f.content_hash, NULL
         FROM files f
         WHERE f.base_id = ?2 AND f.presence = 'present'
           AND NOT EXISTS (SELECT 1 FROM scan_stage_entries s
                           WHERE s.scan_id = ?1 AND s.relative_path = f.relative_path
                             AND s.complete = 1)",
        params![scan_id, base_id, F_PRESENCE],
    )?;

    // Only added and updated paths are written. An unchanged path keeps the
    // scan id that actually supplied its state.
    tx.execute(
        "INSERT INTO files
             (base_id, relative_path, presence, size_bytes, mtime_ns, content_hash,
              metadata_from_scan_id, deleted_in_scan_id)
         SELECT s.base_id, s.relative_path, 'present', s.size_bytes, s.mtime_ns,
                s.content_hash, s.scan_id, NULL
         FROM scan_stage_entries s
         LEFT JOIN files f ON f.base_id = s.base_id AND f.relative_path = s.relative_path
         WHERE s.scan_id = ?1 AND s.complete = 1
           AND (f.file_id IS NULL OR f.presence = 'deleted'
                OR f.size_bytes <> s.size_bytes OR f.mtime_ns <> s.mtime_ns
                OR f.content_hash <> s.content_hash)
         ON CONFLICT(base_id, relative_path) DO UPDATE SET
             presence = 'present',
             size_bytes = excluded.size_bytes,
             mtime_ns = excluded.mtime_ns,
             content_hash = excluded.content_hash,
             metadata_from_scan_id = excluded.metadata_from_scan_id,
             deleted_in_scan_id = NULL",
        params![scan_id],
    )?;

    tx.execute(
        "UPDATE files SET presence = 'deleted', deleted_in_scan_id = ?1
         WHERE base_id = ?2 AND presence = 'present'
           AND NOT EXISTS (SELECT 1 FROM scan_stage_entries s
                           WHERE s.scan_id = ?1 AND s.relative_path = files.relative_path
                             AND s.complete = 1)",
        params![scan_id, base_id],
    )?;

    let unchanged = staged - added - updated;
    tx.execute(
        "UPDATE scans SET status = 'complete', finished_at_ns = ?1,
             added_count = ?2, updated_count = ?3, deleted_count = ?4,
             unchanged_count = ?5, error_count = 0
         WHERE scan_id = ?6",
        params![
            now_ns(),
            added as i64,
            updated as i64,
            deleted as i64,
            unchanged as i64,
            scan_id
        ],
    )?;
    tx.execute(
        "UPDATE bases SET last_complete_scan_id = ?1 WHERE base_id = ?2",
        params![scan_id, base_id],
    )?;
    tx.execute(
        "DELETE FROM scan_stage_entries WHERE scan_id = ?1",
        params![scan_id],
    )?;

    tx.commit()?;
    Ok((added, updated, deleted, unchanged))
}

fn now_ns() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos() as i64)
        .unwrap_or(0)
}

/// Expands a leading `~` using $HOME. Explicit application behavior, not the shell's.
fn expand_tilde(path: &str) -> PathBuf {
    let home = std::env::var("HOME").unwrap_or_default();
    if home.is_empty() {
        return PathBuf::from(path);
    }
    match path {
        "~" => PathBuf::from(home),
        p if p.starts_with("~/") => PathBuf::from(home).join(&p[2..]),
        p => PathBuf::from(p),
    }
}

/// Recursively collect eligible regular files: skips hidden entries and symlinks.
fn walk(dir: &Path, out: &mut Vec<PathBuf>, errors: &mut usize) {
    let entries = match fs::read_dir(dir) {
        Ok(e) => e,
        Err(e) => {
            eprintln!("  error {}: {e}", dir.display());
            *errors += 1;
            return;
        }
    };
    for entry in entries.flatten() {
        let name = entry.file_name();
        if name.to_string_lossy().starts_with('.') {
            continue;
        }
        let path = entry.path();
        match entry.file_type() {
            Ok(t) if t.is_symlink() => continue,
            Ok(t) if t.is_dir() => walk(&path, out, errors),
            Ok(t) if t.is_file() => out.push(path),
            _ => {}
        }
    }
}

fn hash_file(path: &Path) -> std::io::Result<Vec<u8>> {
    let mut file = fs::File::open(path)?;
    let mut hasher = Sha256::new();
    let mut buf = vec![0u8; 1024 * 1024];
    loop {
        let n = file.read(&mut buf)?;
        if n == 0 {
            break;
        }
        hasher.update(&buf[..n]);
    }
    Ok(hasher.finalize().to_vec())
}
