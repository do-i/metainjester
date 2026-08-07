use std::collections::{HashMap, HashSet};
use std::fs;
use std::io::Read;
use std::path::{Path, PathBuf};
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use rusqlite::{params, Connection};
use sha2::{Digest, Sha256};

const DB_FILE: &str = "metainjester.sqlite3";

/// `field_mask` bits in `scan_changes`. MIME (4) is unused until MIME lands.
const F_SIZE: i64 = 1;
const F_MTIME: i64 = 2;
const F_HASH: i64 = 8;
const F_PRESENCE: i64 = 16;

const SCHEMA: &str = "
CREATE TABLE IF NOT EXISTS bases (
    base_id   INTEGER PRIMARY KEY,
    base_path TEXT NOT NULL UNIQUE
);
CREATE TABLE IF NOT EXISTS scans (
    scan_id        INTEGER PRIMARY KEY,
    base_id        INTEGER NOT NULL REFERENCES bases(base_id),
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

/// Last completed state of one path, as recorded by an earlier scan.
struct Known {
    present: bool,
    size_bytes: i64,
    mtime_ns: i64,
    content_hash: Vec<u8>,
}

fn main() {
    if let Err(e) = run() {
        eprintln!("metainjester: {e}");
        std::process::exit(1);
    }
}

fn run() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let base = match args.as_slice() {
        [cmd, path] if cmd == "ingest" => fs::canonicalize(expand_tilde(path))?,
        _ => return Err("usage: metainjester ingest <base-path>".into()),
    };
    if !base.is_dir() {
        return Err(format!("not a directory: {}", base.display()).into());
    }

    let started = Instant::now();
    let mut conn = Connection::open(DB_FILE)?;
    conn.execute_batch(SCHEMA)?;

    let base_str = base.to_string_lossy().to_string();
    conn.execute(
        "INSERT OR IGNORE INTO bases (base_path) VALUES (?1)",
        params![base_str],
    )?;
    let base_id: i64 = conn.query_row(
        "SELECT base_id FROM bases WHERE base_path = ?1",
        params![base_str],
        |r| r.get(0),
    )?;

    // The scan row exists before traversal so its id can stamp every row written.
    conn.execute(
        "INSERT INTO scans (base_id, status, started_at_ns) VALUES (?1, 'running', ?2)",
        params![base_id, now_ns()],
    )?;
    let scan_id = conn.last_insert_rowid();

    let mut known: HashMap<String, Known> = HashMap::new();
    {
        let mut stmt = conn.prepare(
            "SELECT relative_path, presence, size_bytes, mtime_ns, content_hash
             FROM files WHERE base_id = ?1",
        )?;
        let rows = stmt.query_map(params![base_id], |r| {
            Ok((
                r.get::<_, String>(0)?,
                Known {
                    present: r.get::<_, String>(1)? == "present",
                    size_bytes: r.get(2)?,
                    mtime_ns: r.get(3)?,
                    content_hash: r.get(4)?,
                },
            ))
        })?;
        for row in rows {
            let (rel, k) = row?;
            known.insert(rel, k);
        }
    }

    let mut paths = Vec::new();
    let mut errors = 0usize;
    walk(&base, &mut paths, &mut errors);

    let (mut added, mut updated, mut unchanged) = (0usize, 0usize, 0usize);
    let mut total_bytes = 0i64;
    let mut seen: HashSet<String> = HashSet::new();
    let mut hashed = 0usize;

    let tx = conn.transaction()?;
    {
        let mut upsert = tx.prepare(
            "INSERT INTO files
                 (base_id, relative_path, presence, size_bytes, mtime_ns, content_hash,
                  metadata_from_scan_id, deleted_in_scan_id)
             VALUES (?1, ?2, 'present', ?3, ?4, ?5, ?6, NULL)
             ON CONFLICT(base_id, relative_path) DO UPDATE SET
                 presence = 'present',
                 size_bytes = excluded.size_bytes,
                 mtime_ns = excluded.mtime_ns,
                 content_hash = excluded.content_hash,
                 metadata_from_scan_id = excluded.metadata_from_scan_id,
                 deleted_in_scan_id = NULL",
        )?;
        let mut change = tx.prepare(
            "INSERT INTO scan_changes
                 (scan_id, base_id, relative_path, change_kind, field_mask,
                  old_size_bytes, new_size_bytes, old_mtime_ns, new_mtime_ns,
                  old_content_hash, new_content_hash)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)",
        )?;

        for path in &paths {
            let rel = path.strip_prefix(&base).unwrap().to_string_lossy().to_string();
            let meta = match fs::metadata(path) {
                Ok(m) => m,
                Err(e) => {
                    eprintln!("  skip {rel}: {e}");
                    errors += 1;
                    // Seen on disk: an unreadable file must not be called deleted.
                    seen.insert(rel);
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
            seen.insert(rel.clone());
            total_bytes += size;

            let old = known.get(&rel);

            // Reuse the stored hash when the file is unchanged by size and mtime.
            if let Some(k) = old
                && k.present
                && k.size_bytes == size
                && k.mtime_ns == mtime_ns
            {
                unchanged += 1;
                continue;
            }

            let hash = match hash_file(path) {
                Ok(h) => h,
                Err(e) => {
                    eprintln!("  skip {rel}: {e}");
                    errors += 1;
                    continue;
                }
            };
            hashed += 1;
            upsert.execute(params![
                base_id,
                rel,
                size,
                mtime_ns,
                hash.as_slice(),
                scan_id
            ])?;

            match old {
                // Absent, or present again at a path last seen deleted.
                None => {
                    added += 1;
                    change.execute(params![
                        scan_id,
                        base_id,
                        rel,
                        "added",
                        F_SIZE | F_MTIME | F_HASH | F_PRESENCE,
                        None::<i64>,
                        size,
                        None::<i64>,
                        mtime_ns,
                        None::<Vec<u8>>,
                        hash.as_slice()
                    ])?;
                }
                Some(k) if !k.present => {
                    added += 1;
                    change.execute(params![
                        scan_id,
                        base_id,
                        rel,
                        "added",
                        F_SIZE | F_MTIME | F_HASH | F_PRESENCE,
                        k.size_bytes,
                        size,
                        k.mtime_ns,
                        mtime_ns,
                        k.content_hash.as_slice(),
                        hash.as_slice()
                    ])?;
                }
                Some(k) => {
                    let mut mask = 0i64;
                    if k.size_bytes != size {
                        mask |= F_SIZE;
                    }
                    if k.mtime_ns != mtime_ns {
                        mask |= F_MTIME;
                    }
                    if k.content_hash != hash {
                        mask |= F_HASH;
                    }
                    updated += 1;
                    change.execute(params![
                        scan_id,
                        base_id,
                        rel,
                        "updated",
                        mask,
                        k.size_bytes,
                        size,
                        k.mtime_ns,
                        mtime_ns,
                        k.content_hash.as_slice(),
                        hash.as_slice()
                    ])?;
                }
            }
        }

        // Anything previously present and not seen this scan is now deleted.
        let mut mark_deleted = tx.prepare(
            "UPDATE files SET presence = 'deleted', deleted_in_scan_id = ?1
             WHERE base_id = ?2 AND relative_path = ?3",
        )?;
        for (rel, k) in &known {
            if k.present && !seen.contains(rel) {
                mark_deleted.execute(params![scan_id, base_id, rel])?;
                change.execute(params![
                    scan_id,
                    base_id,
                    rel,
                    "deleted",
                    F_PRESENCE,
                    k.size_bytes,
                    None::<i64>,
                    k.mtime_ns,
                    None::<i64>,
                    k.content_hash.as_slice(),
                    None::<Vec<u8>>
                ])?;
            }
        }
    }

    let deleted = known
        .iter()
        .filter(|(rel, k)| k.present && !seen.contains(*rel))
        .count();

    tx.execute(
        "UPDATE scans SET status = 'complete', finished_at_ns = ?1,
             added_count = ?2, updated_count = ?3, deleted_count = ?4,
             unchanged_count = ?5, error_count = ?6
         WHERE scan_id = ?7",
        params![
            now_ns(),
            added as i64,
            updated as i64,
            deleted as i64,
            unchanged as i64,
            errors as i64,
            scan_id
        ],
    )?;
    tx.commit()?;

    println!("base    {}", base.display());
    println!("scan    {scan_id}");
    println!("files   {}", seen.len());
    println!("bytes   {total_bytes}");
    println!("hashed  {hashed}");
    println!("errors  {errors}");
    println!("elapsed {:.2}s", started.elapsed().as_secs_f64());
    println!("db      {DB_FILE}");
    println!("added {added}  updated {updated}  deleted {deleted}  unchanged {unchanged}");
    Ok(())
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
            eprintln!("  skip {}: {e}", dir.display());
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
