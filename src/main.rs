//! metainjester — catalogue a directory tree into SQLite and report what
//! changed on each later run. `ingest` is the whole command surface, and it is
//! both the start and the resume command.

mod config;
mod db;
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
    pub fn no_space(free: u64, required: u64, source: &str) -> AppError {
        AppError {
            message: format!(
                "insufficient free space: {} available, {} required ({source})",
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

fn run() -> Result<i32, AppError> {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let raw_base = match args.as_slice() {
        [cmd, path] if cmd == "ingest" => path.clone(),
        _ => {
            return Err(AppError::usage(
                "usage: metainjester ingest <base-path>\n  \
                 policy and storage settings come from the configuration file, not flags",
            ))
        }
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
        _ => EXIT_ERROR,
    })
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
        "free    {} available, {} required — {}",
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
