//! metainjester — catalogue a directory tree into SQLite and report what
//! changed on each later run. `ingest` is the whole command surface, and it is
//! both the start and the resume command.

mod config;
mod db;
mod history;
mod scan;
mod walk;

use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use config::Config;

/// Exit codes. `130` follows the shell convention for SIGINT.
const EXIT_OK: i32 = 0;
const EXIT_ERROR: i32 = 1;
const EXIT_USAGE: i32 = 2;
const EXIT_CONFIG: i32 = 3;
const EXIT_BUSY: i32 = 4;
const EXIT_NO_SPACE: i32 = 5;
const EXIT_POLICY: i32 = 6;
const EXIT_CANCELLED: i32 = 130;

pub struct AppError {
    pub message: String,
    pub code: i32,
}

impl AppError {
    pub fn usage(m: impl Into<String>) -> AppError {
        AppError { message: m.into(), code: EXIT_USAGE }
    }
    pub fn config(m: impl Into<String>) -> AppError {
        AppError { message: m.into(), code: EXIT_CONFIG }
    }
    pub fn io(m: impl Into<String>) -> AppError {
        AppError { message: m.into(), code: EXIT_ERROR }
    }
    pub fn db(e: rusqlite::Error) -> AppError {
        AppError { message: format!("database: {e}"), code: EXIT_ERROR }
    }
    pub fn db_at(path: &std::path::Path, e: rusqlite::Error) -> AppError {
        AppError {
            message: format!("cannot open {}: {e}", path.display()),
            code: EXIT_ERROR,
        }
    }
    pub fn not_ours(path: PathBuf) -> AppError {
        AppError {
            message: format!(
                "{} exists but is not a metainjester database; refusing to touch it",
                path.display()
            ),
            code: EXIT_CONFIG,
        }
    }
    pub fn busy(pid: i64) -> AppError {
        AppError {
            message: format!("database is busy: another metainjester (pid {pid}) is writing"),
            code: EXIT_BUSY,
        }
    }
    pub fn policy_mismatch(m: impl Into<String>) -> AppError {
        AppError {
            message: format!(
                "policy_mismatch: {}\n  \
                 deletion detection is only safe under the rules the baseline was built with.\n  \
                 restore the previous settings, or index this base into a fresh database.",
                m.into()
            ),
            code: EXIT_POLICY,
        }
    }
    pub fn no_space(free: u64, required: u64) -> AppError {
        AppError {
            message: format!(
                "insufficient free space: {} available, {} must stay free \
                 (storage.minimum_free_space_mib)",
                human(free),
                human(required)
            ),
            code: EXIT_NO_SPACE,
        }
    }
}

fn main() {
    let code = match run() {
        Ok(code) => code,
        Err(e) => {
            eprintln!("metainjester: {}", e.message);
            e.code
        }
    };
    std::process::exit(code);
}

const USAGE: &str = "usage: metainjester ingest <base-path>\n       \
                     metainjester status\n       \
                     metainjester history prune [--apply]\n  \
                     policy and storage settings come from the configuration file, not flags";

fn run() -> Result<i32, AppError> {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let raw_base = match args.as_slice() {
        [cmd, path] if cmd == "ingest" => path.clone(),
        [cmd] if cmd == "status" => return status(),
        // Preview is the default: `prune` alone never deletes anything.
        [a, b] if a == "history" && b == "prune" => return prune(false),
        [a, b, f] if a == "history" && b == "prune" && f == "--apply" => return prune(true),
        _ => return Err(AppError::usage(USAGE)),
    };

    let config = Config::load()?;
    let base = std::fs::canonicalize(config::expand_tilde(&raw_base))
        .map_err(|e| AppError::io(format!("{raw_base}: {e}")))?;
    if !base.is_dir() {
        return Err(AppError::io(format!(
            "not a directory: {}",
            base.display()
        )));
    }

    let cancelled = Arc::new(AtomicBool::new(false));
    {
        let flag = cancelled.clone();
        ctrlc::set_handler(move || flag.store(true, Ordering::SeqCst))
            .map_err(|e| AppError::io(format!("cannot install signal handler: {e}")))?;
    }

    let opened = db::discover(&config)?;
    let mut conn = opened.conn;
    db::ensure_schema(&conn, &opened.path)?;
    db::acquire_writer_lock(&conn)?;

    let result = scan::ingest(&mut conn, &opened.path, &config, &base, &cancelled);
    db::release_writer_lock(&conn);
    let outcome = result?;

    report(&opened.path, &base, &config, &outcome);
    Ok(match outcome.status {
        "complete" => EXIT_OK,
        "cancelled" => EXIT_CANCELLED,
        // Stopping at the floor is the same condition as refusing at the floor,
        // so a script watching for "out of space" sees one code either way.
        _ if outcome.low_space => EXIT_NO_SPACE,
        _ => EXIT_ERROR,
    })
}

/// `status`. Read-only and lock-free, so it stays answerable while a scan is
/// running — which is exactly when you want it. It reports the two things that
/// otherwise require starting a scan to discover: whether a resumable scan is
/// waiting, and whether the current configuration still matches each baseline's
/// inclusion policy.
fn status() -> Result<i32, AppError> {
    use rusqlite::OptionalExtension;

    let config = Config::load()?;
    for path in config::candidate_files() {
        println!(
            "config  {}{}",
            path.display(),
            if path.exists() { "" } else { "  (absent)" }
        );
    }
    println!(
        "policy  hidden {}  mounts {}  symlinks {}",
        if config.skip_hidden { "skip" } else { "include" },
        if config.skip_mount_boundaries { "skip" } else { "cross" },
        if config.follow_symlinks { "follow" } else { "skip" }
    );
    println!(
        "limits  free floor {}  keep_scans {}",
        human(config.minimum_free_space_mib * 1024 * 1024),
        if config.keep_scans == 0 { "unbounded".to_string() } else { config.keep_scans.to_string() }
    );

    // Read-only for real, not just by intent. `discover` + `ensure_schema` would
    // create the directory and file if absent and then take the write lock to
    // rebuild views — which contends with the very scan this command exists to
    // report on, and turns "you have no database" into "you do now".
    let Some(db_path) = db::existing_path(&config)? else {
        println!("db      none yet — `ingest <base-path>` creates one");
        return Ok(EXIT_OK);
    };
    let conn = db::open_readonly(&db_path)?;
    db::check_schema(&conn, &db_path)?;
    println!("db      {}", db_path.display());

    // A live pid here is why the next `ingest` would exit 4; a dead one is
    // reclaimed automatically and never shown.
    let holder: Option<i64> = conn
        .query_row("SELECT pid FROM writer_lock WHERE id = 1", [], |r| r.get(0))
        .optional()
        .map_err(AppError::db)?;
    // A row whose pid is gone is not a held lock — `acquire_writer_lock`
    // reclaims it without a word, so reporting it here would send the user
    // hunting for a scan that already ended.
    if let Some(pid) = holder.filter(|p| db::pid_alive(*p)) {
        println!("writer  pid {pid} holds the write lock");
    }

    let mut stmt = conn
        .prepare(
            "SELECT b.base_id, b.base_path, b.skip_hidden, b.skip_mount_boundaries,
                    b.follow_symlinks, s.scan_id, s.finished_at_ns, s.present_count
             FROM bases b
             LEFT JOIN scans s ON s.scan_id = b.last_complete_scan_id
             ORDER BY b.base_id",
        )
        .map_err(AppError::db)?;
    let bases: Vec<(i64, String, bool, bool, bool, Option<i64>, Option<i64>, Option<i64>)> = stmt
        .query_map([], |r| {
            Ok((
                r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?, r.get(4)?,
                r.get(5)?, r.get(6)?, r.get(7)?,
            ))
        })
        .map_err(AppError::db)?
        .collect::<Result<_, _>>()
        .map_err(AppError::db)?;

    if bases.is_empty() {
        println!("no bases yet — `ingest <base-path>` creates one");
        return Ok(EXIT_OK);
    }

    for (base_id, base_path, hidden, mounts, symlinks, scan_id, finished, present) in bases {
        println!("base    {base_path}");
        match (scan_id, finished) {
            (Some(id), Some(ns)) => println!(
                "  baseline  scan {id}, {} ago, {} file(s) present",
                ago(ns),
                present.unwrap_or(0)
            ),
            _ => println!("  baseline  none — no scan has completed yet"),
        }

        // Must mirror the resumable query in `scan::ingest`, or status would
        // promise a resume the scanner does not perform.
        let resumable: Option<(i64, String, i64)> = conn
            .query_row(
                "SELECT scan_id, status, started_at_ns FROM scans
                 WHERE base_id = ?1 AND status IN ('running','cancelled','failed','partial')
                 ORDER BY scan_id DESC LIMIT 1",
                rusqlite::params![base_id],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
            )
            .optional()
            .map_err(AppError::db)?;
        match resumable {
            Some((id, st, started)) => println!(
                "  resumable scan {id} ({st}, started {} ago) — `ingest {base_path}` continues it",
                ago(started)
            ),
            None => println!("  resumable none"),
        }

        // The exact comparison behind EXIT_POLICY, surfaced before it refuses.
        let mut differs = Vec::new();
        if hidden != config.skip_hidden {
            differs.push("skip_hidden");
        }
        if mounts != config.skip_mount_boundaries {
            differs.push("skip_mount_boundaries");
        }
        if symlinks != config.follow_symlinks {
            differs.push("follow_symlinks");
        }
        if !differs.is_empty() {
            println!(
                "  POLICY    {} differ(s) from this baseline; `ingest` will refuse (exit {EXIT_POLICY})",
                differs.join(", ")
            );
        }
    }
    Ok(EXIT_OK)
}

/// Coarse on purpose: status answers "recent or stale?", not "when exactly?".
fn ago(then_ns: i64) -> String {
    let secs = (now_ns() - then_ns).max(0) as f64 / 1e9;
    if secs < 90.0 {
        format!("{secs:.0}s")
    } else if secs < 5400.0 {
        format!("{:.0}m", secs / 60.0)
    } else if secs < 172_800.0 {
        format!("{:.1}h", secs / 3600.0)
    } else {
        format!("{:.1}d", secs / 86_400.0)
    }
}

/// `history prune`. Walks no tree and touches no file — it only bounds what the
/// database already holds, so it is the cheap way to act on a lowered
/// `keep_scans` without waiting for the next scan.
fn prune(apply: bool) -> Result<i32, AppError> {
    let config = Config::load()?;
    // The preview counts; only `--apply` deletes. So only `--apply` opens for
    // writing — a preview must not create a database, rebuild views, or reach
    // for the write lock a running scan is holding.
    let (conn, db_path) = if apply {
        let opened = db::discover(&config)?;
        db::ensure_schema(&opened.conn, &opened.path)?;
        (opened.conn, opened.path)
    } else {
        let Some(path) = db::existing_path(&config)? else {
            println!("db      none yet — `ingest <base-path>` creates one");
            return Ok(EXIT_OK);
        };
        let conn = db::open_readonly(&path)?;
        db::check_schema(&conn, &path)?;
        (conn, path)
    };

    println!("db      {}", db_path.display());
    if config.keep_scans == 0 {
        println!("policy  history.keep_scans = 0 — history is kept forever, nothing to prune");
        println!("  set [history] keep_scans = N in the configuration file to bound it");
        return Ok(EXIT_OK);
    }
    println!("policy  history.keep_scans = {}", config.keep_scans);

    // The lock is only taken to delete. A preview is read-only, so it stays
    // available while a scan is running.
    if apply {
        db::acquire_writer_lock(&conn)?;
    }
    let result = history::prune_all(&conn, &config, apply);
    if apply {
        db::release_writer_lock(&conn);
    }
    let bases = result?;

    let mut total = 0u64;
    for b in &bases {
        println!("base    {}", b.base_path);
        match b.cutoff {
            None => println!(
                "  fewer than {} complete scans — nothing has aged out yet",
                config.keep_scans
            ),
            Some(cutoff) => {
                println!(
                    "  keeping scans >= {cutoff}; {} change row(s), {} dead file row(s){}",
                    b.changes,
                    b.files,
                    if apply { " removed" } else { " prunable" }
                );
                total += b.total();
            }
        }
    }
    if bases.is_empty() {
        println!("no bases in this database");
    }

    if total == 0 {
        println!("nothing to do");
    } else if apply {
        println!(
            "removed {total} row(s). The file will not shrink — freed pages are reused, so it \
             stops growing.\n  to reclaim the bytes: sqlite3 {} \"VACUUM INTO 'new.sqlite3'\" \
             then swap it in",
            db_path.display()
        );
    } else {
        println!("preview only — rerun with --apply to delete");
    }
    Ok(EXIT_OK)
}

fn report(db_path: &std::path::Path, base: &std::path::Path, config: &Config, o: &scan::Outcome) {
    println!("base    {}", base.display());
    println!("db      {}", db_path.display());
    println!(
        "scan    {}{}",
        o.scan_id,
        match (o.resumed, o.status) {
            (true, "complete") => "  (resumed)".to_string(),
            (false, "complete") => "  (fresh)".to_string(),
            (true, s) => format!("  (resumed, {s}, not promoted)"),
            (false, s) => format!("  ({s}, not promoted)"),
        }
    );
    println!("workers {}  batch {}", config.workers, config.writer_batch_rows);
    println!("files   {}", o.staged);
    println!("bytes   {}", human(o.discovered_bytes));
    println!(
        "hashed  {} ({})  reused {} baseline, {} staged",
        o.hashed,
        human(o.hashed_bytes),
        o.reused_baseline,
        o.reused_stage
    );
    if o.excluded_hidden + o.excluded_mount + o.excluded_symlink > 0 {
        println!(
            "skipped {} hidden, {} mount, {} symlink",
            o.excluded_hidden, o.excluded_mount, o.excluded_symlink
        );
    }
    println!("errors  {}", o.errors);
    println!(
        "free    {} at start, {} must stay free — {}",
        human(o.free_before),
        human(o.required_free),
        o.estimate_source
    );
    println!("elapsed {:.2}s", o.duration_ms as f64 / 1000.0);

    if o.status == "complete" {
        println!(
            "added {}  updated {}  deleted {}  unchanged {}",
            o.added, o.updated, o.deleted, o.unchanged
        );
        println!("present {}", o.present);
        if o.pruned_changes + o.pruned_files > 0 {
            println!(
                "pruned  {} change row(s), {} dead file row(s) (history.keep_scans = {})",
                o.pruned_changes, o.pruned_files, config.keep_scans
            );
        }
        // Deliberately a count, not a list: a permission slip can cover a large
        // subtree, and the paths are already in the database to be queried.
        if o.unreadable_paths > 0 {
            println!(
                "unreadable {} path(s) could not be read; {} file row(s) held as 'unreadable'",
                o.unreadable_paths, o.unreadable
            );
            println!(
                "  see: SELECT relative_path, error_code, message FROM scan_errors WHERE scan_id = {};",
                o.scan_id
            );
        }
    } else {
        if o.low_space {
            println!(
                "stopped: reached the {} free-space floor — free some space, then rerun",
                human(o.required_free)
            );
        }
        // Otherwise this failure is invisible: an unread base stages nothing, so
        // the counts above look like a clean scan of an empty tree.
        if o.base_unreadable {
            println!(
                "stopped: could not read the base itself — every path in the baseline \
                 depends on it, so nothing was promoted"
            );
        }
        println!("baseline unchanged; rerun `ingest` on the same path to resume");
        if o.errors > 0 {
            println!("  see: SELECT stage, error_code, message FROM scan_errors WHERE scan_id = {};", o.scan_id);
        }
    }
}

pub fn now_ns() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos() as i64)
        .unwrap_or(0)
}

pub fn human(bytes: u64) -> String {
    const UNITS: [&str; 5] = ["B", "KiB", "MiB", "GiB", "TiB"];
    let mut value = bytes as f64;
    let mut unit = 0;
    while value >= 1024.0 && unit < UNITS.len() - 1 {
        value /= 1024.0;
        unit += 1;
    }
    if unit == 0 {
        format!("{bytes} B")
    } else {
        format!("{value:.1} {}", UNITS[unit])
    }
}
