//! Configuration per design §4. System defaults load first, the user file
//! overrides. A malformed file, unknown key, invalid type, or invalid enum is a
//! startup error — never a silent fallback.

use std::path::PathBuf;

use serde::Deserialize;

use crate::AppError;

pub const SYSTEM_CONFIG: &str = "/etc/xdg/metainjester/metainjester.toml";
pub const USER_CONFIG_SUFFIX: &str = ".config/metainjester/metainjester.toml";

/// `"auto"` or an explicit positive integer. Anything else is a startup error.
#[derive(Debug, Clone, Deserialize)]
#[serde(untagged)]
enum AutoOr {
    Word(String),
    Num(u64),
}

impl AutoOr {
    fn resolve(&self, key: &str, auto: u64) -> Result<u64, AppError> {
        match self {
            AutoOr::Word(w) if w == "auto" => Ok(auto),
            AutoOr::Word(w) => Err(AppError::config(format!(
                "{key}: expected \"auto\" or a positive integer, got \"{w}\""
            ))),
            AutoOr::Num(0) => Err(AppError::config(format!("{key}: must be greater than 0"))),
            AutoOr::Num(n) => Ok(*n),
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawFile {
    #[serde(default)]
    scan: Option<RawScan>,
    #[serde(default)]
    storage: Option<RawStorage>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawScan {
    workers: Option<AutoOr>,
    writer_batch_rows: Option<AutoOr>,
    throttle_ms_after_batch: Option<u64>,
    hash_policy: Option<String>,
    skip_hidden: Option<bool>,
    skip_mount_boundaries: Option<bool>,
    follow_symlinks: Option<bool>,
    initial_average_file_kib: Option<u64>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawStorage {
    database_path: Option<String>,
    minimum_free_space_gib: Option<u64>,
}

#[derive(Debug, Clone)]
pub struct Config {
    pub workers: usize,
    pub writer_batch_rows: usize,
    pub queue_items: usize,
    pub throttle_ms_after_batch: u64,
    pub hash_policy: String,
    pub skip_hidden: bool,
    pub skip_mount_boundaries: bool,
    pub follow_symlinks: bool,
    pub initial_average_file_kib: u64,
    pub database_path: PathBuf,
    pub minimum_free_space_gib: u64,
}

impl Config {
    /// System file, then user file on top. Both are optional; either being
    /// unreadable-but-present is an error, since silently ignoring a config the
    /// user wrote is worse than refusing to start.
    pub fn load() -> Result<Config, AppError> {
        let mut merged = RawFile {
            scan: None,
            storage: None,
        };
        let mut files = vec![PathBuf::from(SYSTEM_CONFIG)];
        if let Some(home) = home_dir() {
            files.push(home.join(USER_CONFIG_SUFFIX));
        }
        for path in &files {
            if !path.exists() {
                continue;
            }
            let text = std::fs::read_to_string(path).map_err(|e| {
                AppError::config(format!("cannot read {}: {e}", path.display()))
            })?;
            let raw: RawFile = toml::from_str(&text).map_err(|e| {
                AppError::config(format!("{}: {}", path.display(), first_line(&e.to_string())))
            })?;
            merge(&mut merged, raw);
        }
        Config::resolve(merged)
    }

    fn resolve(raw: RawFile) -> Result<Config, AppError> {
        let scan = raw.scan.unwrap_or(RawScan {
            workers: None,
            writer_batch_rows: None,
            throttle_ms_after_batch: None,
            hash_policy: None,
            skip_hidden: None,
            skip_mount_boundaries: None,
            follow_symlinks: None,
            initial_average_file_kib: None,
        });
        let storage = raw.storage.unwrap_or(RawStorage {
            database_path: None,
            minimum_free_space_gib: None,
        });

        // auto = min(4, max(1, logical_cpus / 2)); more workers usually buys disk
        // contention rather than throughput.
        let cpus = std::thread::available_parallelism()
            .map(|n| n.get() as u64)
            .unwrap_or(1);
        let auto_workers = 4.min(1.max(cpus / 2));
        let workers = match scan.workers {
            Some(v) => v.resolve("scan.workers", auto_workers)?,
            None => auto_workers,
        } as usize;

        let writer_batch_rows = match scan.writer_batch_rows {
            Some(v) => v.resolve("scan.writer_batch_rows", 1000)?,
            None => 1000,
        } as usize;

        let hash_policy = scan
            .hash_policy
            .unwrap_or_else(|| "reuse_when_metadata_matches".to_string());
        if hash_policy != "reuse_when_metadata_matches" {
            return Err(AppError::config(format!(
                "scan.hash_policy: unknown policy \"{hash_policy}\""
            )));
        }

        let initial_average_file_kib = scan.initial_average_file_kib.unwrap_or(200);
        if initial_average_file_kib == 0 {
            return Err(AppError::config(
                "scan.initial_average_file_kib: must be greater than 0",
            ));
        }

        let raw_db = storage
            .database_path
            .unwrap_or_else(|| "~/.local/share/metainjester/metainjester.sqlite3".to_string());
        // Relative paths resolve against the process current directory; tilde
        // expansion is ours, not TOML's.
        let database_path = expand_tilde(&raw_db);

        Ok(Config {
            workers,
            writer_batch_rows,
            queue_items: writer_batch_rows.saturating_mul(4).max(1),
            throttle_ms_after_batch: scan.throttle_ms_after_batch.unwrap_or(0),
            hash_policy,
            skip_hidden: scan.skip_hidden.unwrap_or(true),
            skip_mount_boundaries: scan.skip_mount_boundaries.unwrap_or(true),
            follow_symlinks: scan.follow_symlinks.unwrap_or(false),
            initial_average_file_kib,
            database_path,
            minimum_free_space_gib: storage.minimum_free_space_gib.unwrap_or(2),
        })
    }
}

fn merge(into: &mut RawFile, from: RawFile) {
    if let Some(s) = from.scan {
        match into.scan.as_mut() {
            None => into.scan = Some(s),
            Some(t) => {
                if s.workers.is_some() {
                    t.workers = s.workers;
                }
                if s.writer_batch_rows.is_some() {
                    t.writer_batch_rows = s.writer_batch_rows;
                }
                if s.throttle_ms_after_batch.is_some() {
                    t.throttle_ms_after_batch = s.throttle_ms_after_batch;
                }
                if s.hash_policy.is_some() {
                    t.hash_policy = s.hash_policy;
                }
                if s.skip_hidden.is_some() {
                    t.skip_hidden = s.skip_hidden;
                }
                if s.skip_mount_boundaries.is_some() {
                    t.skip_mount_boundaries = s.skip_mount_boundaries;
                }
                if s.follow_symlinks.is_some() {
                    t.follow_symlinks = s.follow_symlinks;
                }
                if s.initial_average_file_kib.is_some() {
                    t.initial_average_file_kib = s.initial_average_file_kib;
                }
            }
        }
    }
    if let Some(s) = from.storage {
        match into.storage.as_mut() {
            None => into.storage = Some(s),
            Some(t) => {
                if s.database_path.is_some() {
                    t.database_path = s.database_path;
                }
                if s.minimum_free_space_gib.is_some() {
                    t.minimum_free_space_gib = s.minimum_free_space_gib;
                }
            }
        }
    }
}

fn first_line(s: &str) -> String {
    s.lines().next().unwrap_or(s).to_string()
}

pub fn home_dir() -> Option<PathBuf> {
    let home = std::env::var("HOME").ok()?;
    if home.is_empty() {
        return None;
    }
    Some(PathBuf::from(home))
}

/// Expands a leading `~`. Explicit application behavior, not the shell's.
pub fn expand_tilde(path: &str) -> PathBuf {
    let Some(home) = home_dir() else {
        return PathBuf::from(path);
    };
    match path {
        "~" => home,
        p if p.starts_with("~/") => home.join(&p[2..]),
        p => PathBuf::from(p),
    }
}
