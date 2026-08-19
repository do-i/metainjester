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

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawFile {
    #[serde(default)]
    scan: Option<RawScan>,
    #[serde(default)]
    storage: Option<RawStorage>,
    #[serde(default)]
    history: Option<RawHistory>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawScan {
    workers: Option<AutoOr>,
    writer_batch_rows: Option<AutoOr>,
    throttle_ms_after_batch: Option<u64>,
    hash_policy: Option<String>,
    skip_hidden: Option<bool>,
    skip_mount_boundaries: Option<bool>,
    follow_symlinks: Option<bool>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawStorage {
    database_path: Option<String>,
    minimum_free_space_mib: Option<u64>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawHistory {
    keep_scans: Option<u64>,
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
    pub database_path: PathBuf,
    pub minimum_free_space_mib: u64,
    pub keep_scans: u64,
}

/// The files `load` consults, lowest precedence first. Separate from `load` so
/// `status` can report which ones exist without reimplementing the search.
pub fn candidate_files() -> Vec<PathBuf> {
    let mut files = vec![PathBuf::from(SYSTEM_CONFIG)];
    if let Some(home) = home_dir() {
        files.push(home.join(USER_CONFIG_SUFFIX));
    }
    files
}

impl Config {
    /// System file, then user file on top. Both are optional; either being
    /// unreadable-but-present is an error, since silently ignoring a config the
    /// user wrote is worse than refusing to start.
    pub fn load() -> Result<Config, AppError> {
        let mut merged = RawFile::default();
        for path in &candidate_files() {
            if !path.exists() {
                continue;
            }
            let text = std::fs::read_to_string(path).map_err(|e| {
                AppError::config(format!("cannot read {}: {e}", path.display()))
            })?;
            let raw: RawFile = toml::from_str(&text).map_err(|e| {
                AppError::config(format!("{}: {}", path.display(), toml_reason(&e.to_string())))
            })?;
            merge(&mut merged, raw);
        }
        Config::resolve(merged)
    }

    fn resolve(raw: RawFile) -> Result<Config, AppError> {
        let scan = raw.scan.unwrap_or_default();
        let storage = raw.storage.unwrap_or_default();
        let history = raw.history.unwrap_or_default();

        // auto = min(4, max(1, logical_cpus / 2)); more workers usually buys disk
        // contention rather than throughput.
        let cpus = std::thread::available_parallelism()
            .map(|n| n.get() as u64)
            .unwrap_or(1);
        let auto_workers = (cpus / 2).clamp(1, 4);
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
            database_path,
            minimum_free_space_mib: storage.minimum_free_space_mib.unwrap_or(500),
            // 0 keeps everything. Deleting a user's history because they
            // upgraded is not a default worth choosing.
            keep_scans: history.keep_scans.unwrap_or(0),
        })
    }
}

/// Lays one file's section over the accumulated one, key by key. A key the
/// higher-precedence file omits must keep the value below it, which is why this
/// cannot be a whole-section replace.
macro_rules! overlay {
    ($slot:expr, $from:expr, $($field:ident),+ $(,)?) => {{
        let from = $from;
        match $slot.as_mut() {
            None => $slot = Some(from),
            Some(into) => {
                $(if from.$field.is_some() {
                    into.$field = from.$field;
                })+
            }
        }
    }};
}

fn merge(into: &mut RawFile, from: RawFile) {
    if let Some(s) = from.scan {
        overlay!(
            into.scan,
            s,
            workers,
            writer_batch_rows,
            throttle_ms_after_batch,
            hash_policy,
            skip_hidden,
            skip_mount_boundaries,
            follow_symlinks,
        );
    }
    if let Some(s) = from.storage {
        overlay!(into.storage, s, database_path, minimum_free_space_mib);
    }
    if let Some(s) = from.history {
        overlay!(into.history, s, keep_scans);
    }
}

/// A `toml` error prints the location first and the actual reason last, with
/// caret art in between. Keeping only the first line loses the reason — which is
/// the half that says `unknown field ...` after a config key is renamed.
fn toml_reason(s: &str) -> String {
    let mut lines = s.lines().filter(|l| !l.trim().is_empty());
    let first = lines.next().unwrap_or(s).trim().to_string();
    match lines.rfind(|l| !l.trim_start().starts_with(['|', '^'])) {
        Some(last) if last.trim() != first => format!("{first}: {}", last.trim()),
        _ => first,
    }
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
    match path.strip_prefix("~/") {
        Some(rest) => home.join(rest),
        None if path == "~" => home,
        None => PathBuf::from(path),
    }
}
