//! Database discovery, schema v1, and the single-writer guard.

use std::path::{Path, PathBuf};

use rusqlite::{params, Connection, OptionalExtension};

use crate::config::Config;
use crate::AppError;

pub const CWD_DB: &str = "metainjester.sqlite3";
pub const APPLICATION_ID: &str = "metainjester";
pub const SCHEMA_VERSION: &str = "2";

pub const SCHEMA: &str = "
CREATE TABLE IF NOT EXISTS application_metadata (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS writer_lock (
    id             INTEGER PRIMARY KEY CHECK (id = 1),
    pid            INTEGER NOT NULL,
    acquired_at_ns INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS bases (
    base_id   INTEGER PRIMARY KEY,
    base_path TEXT NOT NULL UNIQUE,
    created_at_ns INTEGER NOT NULL,
    last_complete_scan_id INTEGER REFERENCES scans(scan_id),
    skip_hidden            INTEGER NOT NULL,
    skip_mount_boundaries  INTEGER NOT NULL,
    follow_symlinks        INTEGER NOT NULL,
    last_error TEXT
);
CREATE TABLE IF NOT EXISTS scans (
    scan_id          INTEGER PRIMARY KEY,
    base_id          INTEGER NOT NULL REFERENCES bases(base_id),
    baseline_scan_id INTEGER REFERENCES scans(scan_id),
    status           TEXT NOT NULL,
    started_at_ns    INTEGER NOT NULL,
    finished_at_ns   INTEGER,
    skip_hidden             INTEGER NOT NULL,
    skip_mount_boundaries   INTEGER NOT NULL,
    follow_symlinks         INTEGER NOT NULL,
    workers                 INTEGER NOT NULL,
    writer_batch_rows       INTEGER NOT NULL,
    throttle_ms_after_batch INTEGER NOT NULL,
    hash_policy             TEXT NOT NULL,
    discovered_files   INTEGER NOT NULL DEFAULT 0,
    discovered_bytes   INTEGER NOT NULL DEFAULT 0,
    staged_files       INTEGER NOT NULL DEFAULT 0,
    staged_bytes       INTEGER NOT NULL DEFAULT 0,
    hashed_files       INTEGER NOT NULL DEFAULT 0,
    hashed_bytes       INTEGER NOT NULL DEFAULT 0,
    changed_during_hash INTEGER NOT NULL DEFAULT 0,
    added_count     INTEGER,
    updated_count   INTEGER,
    deleted_count   INTEGER,
    unchanged_count INTEGER,
    error_count     INTEGER,
    present_count   INTEGER,
    excluded_hidden  INTEGER NOT NULL DEFAULT 0,
    excluded_mount   INTEGER NOT NULL DEFAULT 0,
    excluded_symlink INTEGER NOT NULL DEFAULT 0,
    free_bytes_before   INTEGER,
    free_bytes_after    INTEGER,
    required_free_bytes INTEGER,
    estimate_source     TEXT,
    duration_ms         INTEGER,
    failure_code    TEXT,
    failure_message TEXT
);
CREATE TABLE IF NOT EXISTS files (
    file_id       INTEGER PRIMARY KEY,
    base_id       INTEGER NOT NULL REFERENCES bases(base_id),
    relative_path BLOB NOT NULL,
    parent_path   TEXT,
    name          TEXT,
    extension     TEXT,
    presence      TEXT NOT NULL,
    size_bytes    INTEGER NOT NULL,
    mtime_ns      INTEGER NOT NULL,
    created_ns    INTEGER,
    mime_type     TEXT,
    mime_source   TEXT NOT NULL,
    hash_algorithm TEXT NOT NULL,
    content_hash   BLOB NOT NULL,
    hash_status    TEXT NOT NULL,
    metadata_from_scan_id INTEGER NOT NULL REFERENCES scans(scan_id),
    deleted_in_scan_id    INTEGER REFERENCES scans(scan_id),
    UNIQUE(base_id, relative_path),
    CHECK (hash_status <> 'complete' OR hash_algorithm <> 'sha256'
           OR length(content_hash) = 32)
);
CREATE INDEX IF NOT EXISTS files_by_base_presence  ON files(base_id, presence);
CREATE INDEX IF NOT EXISTS files_by_base_name      ON files(base_id, name);
CREATE INDEX IF NOT EXISTS files_by_base_extension ON files(base_id, extension);
CREATE INDEX IF NOT EXISTS files_by_base_mime      ON files(base_id, mime_type);
CREATE TABLE IF NOT EXISTS scan_stage_entries (
    scan_id       INTEGER NOT NULL REFERENCES scans(scan_id),
    base_id       INTEGER NOT NULL REFERENCES bases(base_id),
    relative_path BLOB NOT NULL,
    parent_path   TEXT,
    name          TEXT,
    extension     TEXT,
    size_bytes    INTEGER NOT NULL,
    mtime_ns      INTEGER NOT NULL,
    created_ns    INTEGER,
    mime_type     TEXT,
    mime_source   TEXT NOT NULL,
    hash_algorithm TEXT NOT NULL,
    content_hash   BLOB NOT NULL,
    hash_status    TEXT NOT NULL,
    complete       INTEGER NOT NULL,
    UNIQUE(scan_id, relative_path)
);
CREATE TABLE IF NOT EXISTS scan_changes (
    change_id     INTEGER PRIMARY KEY,
    scan_id       INTEGER NOT NULL REFERENCES scans(scan_id),
    base_id       INTEGER NOT NULL REFERENCES bases(base_id),
    relative_path BLOB NOT NULL,
    change_kind   TEXT NOT NULL,
    field_mask    INTEGER NOT NULL,
    old_size_bytes   INTEGER,
    new_size_bytes   INTEGER,
    old_mtime_ns     INTEGER,
    new_mtime_ns     INTEGER,
    old_mime_type    TEXT,
    new_mime_type    TEXT,
    old_content_hash BLOB,
    new_content_hash BLOB
);
CREATE INDEX IF NOT EXISTS scan_changes_by_scan ON scan_changes(scan_id, change_kind);
CREATE TABLE IF NOT EXISTS scan_errors (
    error_id      INTEGER PRIMARY KEY,
    scan_id       INTEGER NOT NULL REFERENCES scans(scan_id),
    relative_path BLOB,
    stage         TEXT NOT NULL,
    error_code    TEXT NOT NULL,
    message       TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS scan_errors_by_scan ON scan_errors(scan_id);
";

/// Convenience views for hand-written queries and SQLiteBrowser. They hold no
/// data, so they are dropped and rebuilt on every *write* open rather than
/// guarded with `IF NOT EXISTS` — that way a changed definition can never go
/// stale in an existing database, and adding one needs no `SCHEMA_VERSION` bump
/// and no recreation. Rebuilding them is a schema write, so it belongs to
/// `ensure_schema` and never runs on the read-only path.
///
/// `current_files` exists because the default click in a browser is `SELECT *
/// FROM files`, which silently includes `deleted` and `unreadable` rows. The
/// text paths here are lossy, exactly like the `parent_path` / `name` /
/// `extension` helper columns: `relative_path` stays the authority, and is
/// carried through unchanged for anyone who needs the real bytes.
pub const VIEWS: &str = "
DROP VIEW IF EXISTS current_files;
CREATE VIEW current_files AS
SELECT f.file_id,
       b.base_path,
       b.base_path || '/' || CAST(f.relative_path AS TEXT) AS absolute_path,
       f.relative_path,
       f.parent_path, f.name, f.extension,
       f.size_bytes, f.mtime_ns, f.created_ns,
       f.mime_type, f.mime_source,
       f.hash_algorithm, f.hash_status,
       hex(f.content_hash) AS content_hash_hex,
       f.metadata_from_scan_id
FROM files f
JOIN bases b ON b.base_id = f.base_id
WHERE f.presence = 'present';
";

pub struct Opened {
    pub conn: Connection,
    pub path: PathBuf,
}

/// Discovery per design §2: a current-directory database wins, but only if it
/// proves it belongs to this application. An unrelated SQLite file in the
/// working directory is a hard error — never silently adopted.
///
/// This is the *writing* entry point, and the only one allowed to create a
/// directory or initialise a file. Read-only commands use `existing_path` +
/// `open_readonly` so that running `status` in a fresh directory reports that
/// there is no database rather than quietly producing one.
pub fn discover(config: &Config) -> Result<Opened, AppError> {
    let cwd_db = std::env::current_dir()
        .map_err(|e| AppError::io(format!("cannot read current directory: {e}")))?
        .join(CWD_DB);

    if cwd_db.exists() {
        let conn = open_conn(&cwd_db)?;
        match ownership(&conn)? {
            Ownership::Ours => return Ok(Opened { conn, path: cwd_db }),
            Ownership::Empty => {
                // An empty file is ours to initialize; a populated foreign one is not.
                if is_empty_db(&conn)? {
                    init(&conn)?;
                    return Ok(Opened { conn, path: cwd_db });
                }
                return Err(AppError::not_ours(cwd_db));
            }
            Ownership::Foreign(id) => {
                return Err(AppError::config(format!(
                    "{} is not a metainjester database (application_id = {id:?})",
                    cwd_db.display()
                )))
            }
        }
    }

    let path = &config.database_path;
    if let Some(parent) = path.parent()
        && !parent.as_os_str().is_empty()
    {
        std::fs::create_dir_all(parent).map_err(|e| {
            AppError::io(format!("cannot create {}: {e}", parent.display()))
        })?;
    }
    let existed = path.exists();
    let conn = open_conn(path)?;
    if existed {
        match ownership(&conn)? {
            Ownership::Ours => {}
            Ownership::Empty if is_empty_db(&conn)? => init(&conn)?,
            _ => return Err(AppError::not_ours(path.clone())),
        }
    } else {
        init(&conn)?;
    }
    Ok(Opened {
        conn,
        path: path.clone(),
    })
}

enum Ownership {
    Ours,
    Empty,
    Foreign(String),
}

fn ownership(conn: &Connection) -> Result<Ownership, AppError> {
    let has_table: bool = conn
        .query_row(
            "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'application_metadata'",
            [],
            |_| Ok(true),
        )
        .optional()
        .map_err(AppError::db)?
        .unwrap_or(false);
    if !has_table {
        return Ok(Ownership::Empty);
    }
    let id: Option<String> = conn
        .query_row(
            "SELECT value FROM application_metadata WHERE key = 'application_id'",
            [],
            |r| r.get(0),
        )
        .optional()
        .map_err(AppError::db)?;
    match id.as_deref() {
        Some(APPLICATION_ID) => Ok(Ownership::Ours),
        Some(other) => Ok(Ownership::Foreign(other.to_string())),
        None => Ok(Ownership::Empty),
    }
}

fn is_empty_db(conn: &Connection) -> Result<bool, AppError> {
    let n: i64 = conn
        .query_row("SELECT COUNT(*) FROM sqlite_master", [], |r| r.get(0))
        .map_err(AppError::db)?;
    Ok(n == 0)
}

fn open_conn(path: &Path) -> Result<Connection, AppError> {
    let conn = Connection::open(path)
        .map_err(|e| AppError::db_at(path, e))?;
    apply_pragmas(&conn)?;
    Ok(conn)
}

/// WAL so SQLiteBrowser can read during a scan; `synchronous = FULL` because a
/// scan commits in batches and a lost batch must never mean a torn database.
pub fn apply_pragmas(conn: &Connection) -> Result<(), AppError> {
    conn.busy_timeout(std::time::Duration::from_secs(5))
        .map_err(AppError::db)?;
    conn.pragma_update(None, "foreign_keys", "ON")
        .map_err(AppError::db)?;
    conn.pragma_update(None, "journal_mode", "WAL")
        .map_err(AppError::db)?;
    conn.pragma_update(None, "synchronous", "FULL")
        .map_err(AppError::db)?;
    Ok(())
}

/// A read-only connection for a hash worker. WAL lets these run while the
/// writer commits.
pub fn open_reader(path: &Path) -> Result<Connection, AppError> {
    let conn = Connection::open_with_flags(
        path,
        rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY | rusqlite::OpenFlags::SQLITE_OPEN_NO_MUTEX,
    )
    .map_err(|e| AppError::db_at(path, e))?;
    conn.busy_timeout(std::time::Duration::from_secs(5))
        .map_err(AppError::db)?;
    Ok(conn)
}

fn init(conn: &Connection) -> Result<(), AppError> {
    conn.execute_batch(SCHEMA).map_err(AppError::db)?;
    conn.execute(
        "INSERT OR REPLACE INTO application_metadata (key, value) VALUES
             ('application_id', ?1), ('schema_version', ?2)",
        params![APPLICATION_ID, SCHEMA_VERSION],
    )
    .map_err(AppError::db)?;
    Ok(())
}

/// Ensures the schema exists on a database we already own, and checks version.
/// `CREATE TABLE IF NOT EXISTS` cannot add a column to a table that already
/// exists, so a database from an older build must be refused loudly rather than
/// left to fail later on a missing column.
pub fn ensure_schema(conn: &Connection, path: &Path) -> Result<(), AppError> {
    conn.execute_batch(SCHEMA).map_err(AppError::db)?;
    conn.execute_batch(VIEWS).map_err(AppError::db)?;
    let version: Option<String> = conn
        .query_row(
            "SELECT value FROM application_metadata WHERE key = 'schema_version'",
            [],
            |r| r.get(0),
        )
        .optional()
        .map_err(AppError::db)?;
    match version.as_deref() {
        None => {
            conn.execute(
                "INSERT OR REPLACE INTO application_metadata (key, value) VALUES
                     ('application_id', ?1), ('schema_version', ?2)",
                params![APPLICATION_ID, SCHEMA_VERSION],
            )
            .map_err(AppError::db)?;
            Ok(())
        }
        Some(SCHEMA_VERSION) => Ok(()),
        Some(other) => Err(AppError::config(format!(
            "database schema_version is {other}, this build needs {SCHEMA_VERSION}.\n  \
             this POC has no migrations yet — delete the database and ingest again:\n  \
             rm {}*",
            path.display()
        ))),
    }
}

/// The read-only half of `ensure_schema`: check the version and stamp nothing.
/// A read-only command must be able to refuse an unreadable schema without
/// writing to say so — and on a connection opened read-only the write would
/// fail anyway.
pub fn check_schema(conn: &Connection, path: &Path) -> Result<(), AppError> {
    let version: Option<String> = conn
        .query_row(
            "SELECT value FROM application_metadata WHERE key = 'schema_version'",
            [],
            |r| r.get(0),
        )
        .optional()
        .map_err(AppError::db)?;
    match version.as_deref() {
        // An unstamped database predates versioning. Only `ingest` may stamp it,
        // so a read-only command accepts it and moves on.
        None | Some(SCHEMA_VERSION) => Ok(()),
        Some(other) => Err(AppError::config(format!(
            "database schema_version is {other}, this build needs {SCHEMA_VERSION}.\n  \
             this POC has no migrations yet — delete the database and ingest again:\n  \
             rm {}*",
            path.display()
        ))),
    }
}

/// The database a read-only command would use, found without bringing one into
/// existence. `None` means there is nothing to report on yet — a fact `status`
/// prints rather than fixes.
pub fn existing_path(config: &Config) -> Result<Option<PathBuf>, AppError> {
    let cwd_db = std::env::current_dir()
        .map_err(|e| AppError::io(format!("cannot read current directory: {e}")))?
        .join(CWD_DB);
    if cwd_db.exists() {
        return Ok(Some(cwd_db));
    }
    if config.database_path.exists() {
        return Ok(Some(config.database_path.clone()));
    }
    Ok(None)
}

/// Opens an existing database for a command that promises not to write. The
/// read-only flag is what makes the promise enforceable rather than merely
/// intended: no `journal_mode` pragma write, no view rebuild, no schema stamp.
/// That matters because `status` is the command you run *while* a scan is
/// running — anything here that opened a write transaction would queue behind
/// the scan's next batch, and would fail outright on a read-only mount.
pub fn open_readonly(path: &Path) -> Result<Connection, AppError> {
    let conn = open_reader(path)?;
    match ownership(&conn)? {
        Ownership::Ours => Ok(conn),
        Ownership::Empty if is_empty_db(&conn)? => Err(AppError::config(format!(
            "{} is empty — `ingest <base-path>` initialises it",
            path.display()
        ))),
        _ => Err(AppError::not_ours(path.to_path_buf())),
    }
}

/// One writer process per database (design §5). A lock left by a killed process
/// is reclaimed once that pid is gone, so a crash never wedges the database.
pub fn acquire_writer_lock(conn: &Connection) -> Result<(), AppError> {
    let pid = std::process::id() as i64;
    conn.execute("BEGIN IMMEDIATE", []).map_err(AppError::db)?;
    let held: Option<i64> = conn
        .query_row("SELECT pid FROM writer_lock WHERE id = 1", [], |r| r.get(0))
        .optional()
        .map_err(AppError::db)?;
    if let Some(other) = held
        && other != pid
        && pid_alive(other)
    {
        let _ = conn.execute("ROLLBACK", []);
        return Err(AppError::busy(other));
    }
    conn.execute(
        "INSERT OR REPLACE INTO writer_lock (id, pid, acquired_at_ns) VALUES (1, ?1, ?2)",
        params![pid, crate::now_ns()],
    )
    .map_err(AppError::db)?;
    conn.execute("COMMIT", []).map_err(AppError::db)?;
    Ok(())
}

pub fn release_writer_lock(conn: &Connection) {
    let pid = std::process::id() as i64;
    let _ = conn.execute("DELETE FROM writer_lock WHERE id = 1 AND pid = ?1", params![pid]);
}

fn pid_alive(pid: i64) -> bool {
    Path::new(&format!("/proc/{pid}")).exists()
}
