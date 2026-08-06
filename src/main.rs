use std::fs;
use std::io::Read;
use std::path::{Path, PathBuf};
use std::time::{Instant, UNIX_EPOCH};

use rusqlite::{params, Connection};
use sha2::{Digest, Sha256};

const DB_FILE: &str = "metainjester.sqlite3";
const SCHEMA: &str = "
CREATE TABLE IF NOT EXISTS bases (
    base_id   INTEGER PRIMARY KEY,
    base_path TEXT NOT NULL UNIQUE
);
CREATE TABLE IF NOT EXISTS files (
    file_id       INTEGER PRIMARY KEY,
    base_id       INTEGER NOT NULL REFERENCES bases(base_id),
    relative_path TEXT NOT NULL,
    size_bytes    INTEGER NOT NULL,
    mtime_ns      INTEGER NOT NULL,
    content_hash  BLOB NOT NULL,
    UNIQUE(base_id, relative_path)
);
";

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

    let mut files = Vec::new();
    let mut errors = 0usize;
    walk(&base, &mut files, &mut errors);

    let tx = conn.transaction()?;
    let mut count = 0usize;
    let mut total_bytes = 0i64;
    {
        let mut stmt = tx.prepare(
            "INSERT INTO files (base_id, relative_path, size_bytes, mtime_ns, content_hash)
             VALUES (?1, ?2, ?3, ?4, ?5)
             ON CONFLICT(base_id, relative_path) DO UPDATE SET
                 size_bytes = excluded.size_bytes,
                 mtime_ns = excluded.mtime_ns,
                 content_hash = excluded.content_hash",
        )?;
        for path in &files {
            let rel = path.strip_prefix(&base).unwrap().to_string_lossy().to_string();
            let meta = match fs::metadata(path) {
                Ok(m) => m,
                Err(e) => {
                    eprintln!("  skip {rel}: {e}");
                    errors += 1;
                    continue;
                }
            };
            let hash = match hash_file(path) {
                Ok(h) => h,
                Err(e) => {
                    eprintln!("  skip {rel}: {e}");
                    errors += 1;
                    continue;
                }
            };
            let mtime_ns = meta
                .modified()
                .ok()
                .and_then(|t| t.duration_since(UNIX_EPOCH).ok())
                .map(|d| d.as_nanos() as i64)
                .unwrap_or(0);
            let size = meta.len() as i64;
            stmt.execute(params![base_id, rel, size, mtime_ns, hash.as_slice()])?;
            count += 1;
            total_bytes += size;
        }
    }
    tx.commit()?;

    println!("base    {}", base.display());
    println!("files   {count}");
    println!("bytes   {total_bytes}");
    println!("errors  {errors}");
    println!("elapsed {:.2}s", started.elapsed().as_secs_f64());
    println!("db      {DB_FILE}");
    Ok(())
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
