//! Traversal and the three inclusion policies. The walker stats each entry once
//! and hands the result downstream, so a hash worker never re-stats before it
//! reads.

use std::collections::HashSet;
use std::os::unix::ffi::OsStrExt;
use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::mpsc::SyncSender;
use std::sync::Arc;
use std::time::UNIX_EPOCH;

use crate::config::Config;

pub struct Observed {
    pub path: PathBuf,
    pub rel: Vec<u8>,
    pub size_bytes: i64,
    pub mtime_ns: i64,
    pub created_ns: Option<i64>,
}

pub enum WalkMsg {
    Item(Box<Observed>),
    Error {
        rel: Option<Vec<u8>>,
        code: &'static str,
        message: String,
    },
}

#[derive(Default)]
pub struct Exclusions {
    pub hidden: AtomicU64,
    pub mount: AtomicU64,
    pub symlink: AtomicU64,
    pub discovered_files: AtomicU64,
    pub discovered_bytes: AtomicU64,
}

pub struct Walker<'a> {
    pub base: &'a Path,
    pub config: &'a Config,
    pub tx: SyncSender<WalkMsg>,
    pub cancelled: &'a AtomicBool,
    pub counts: Arc<Exclusions>,
    base_dev: u64,
    visited_dirs: HashSet<(u64, u64)>,
    /// Set when the queue's receivers are gone, which means the run is over and
    /// there is nothing left to walk for.
    downstream_gone: AtomicBool,
}

impl<'a> Walker<'a> {
    pub fn new(
        base: &'a Path,
        config: &'a Config,
        tx: SyncSender<WalkMsg>,
        cancelled: &'a AtomicBool,
        counts: Arc<Exclusions>,
    ) -> std::io::Result<Self> {
        let base_dev = std::fs::metadata(base)?.dev();
        Ok(Walker {
            base,
            config,
            tx,
            cancelled,
            counts,
            base_dev,
            visited_dirs: HashSet::new(),
            downstream_gone: AtomicBool::new(false),
        })
    }

    pub fn run(&mut self) {
        let dir = self.base.to_path_buf();
        self.walk(&dir);
    }

    fn rel_of(&self, path: &Path) -> Vec<u8> {
        path.strip_prefix(self.base)
            .unwrap_or(path)
            .as_os_str()
            .as_bytes()
            .to_vec()
    }

    fn send(&self, msg: WalkMsg) -> bool {
        if self.tx.send(msg).is_err() {
            self.downstream_gone.store(true, Ordering::Relaxed);
            return false;
        }
        true
    }

    fn should_stop(&self) -> bool {
        self.cancelled.load(Ordering::Relaxed) || self.downstream_gone.load(Ordering::Relaxed)
    }

    fn walk(&mut self, dir: &Path) {
        if self.should_stop() {
            return;
        }
        let entries = match std::fs::read_dir(dir) {
            Ok(e) => e,
            Err(e) => {
                self.send(WalkMsg::Error {
                    rel: Some(self.rel_of(dir)),
                    code: error_code(&e),
                    message: e.to_string(),
                });
                return;
            }
        };

        for entry in entries {
            if self.should_stop() {
                return;
            }
            let entry = match entry {
                Ok(e) => e,
                Err(e) => {
                    self.send(WalkMsg::Error {
                        rel: Some(self.rel_of(dir)),
                        code: error_code(&e),
                        message: e.to_string(),
                    });
                    continue;
                }
            };
            let name = entry.file_name();
            if self.config.skip_hidden && name.as_bytes().starts_with(b".") {
                self.counts.hidden.fetch_add(1, Ordering::Relaxed);
                continue;
            }
            let path = entry.path();

            // `DirEntry::metadata` does not follow symlinks, which is what the
            // symlink policy needs to see.
            let meta = match entry.metadata() {
                Ok(m) => m,
                Err(e) => {
                    self.send(WalkMsg::Error {
                        rel: Some(self.rel_of(&path)),
                        code: error_code(&e),
                        message: e.to_string(),
                    });
                    continue;
                }
            };

            if meta.file_type().is_symlink() {
                if !self.config.follow_symlinks {
                    self.counts.symlink.fetch_add(1, Ordering::Relaxed);
                    continue;
                }
                match std::fs::metadata(&path) {
                    Ok(target) => {
                        if target.is_dir() {
                            self.enter_dir(&path, &target);
                        } else if target.is_file() {
                            self.emit(&path, &target);
                        }
                    }
                    Err(e) => self.send_err(&path, &e),
                }
                continue;
            }

            if meta.is_dir() {
                self.enter_dir(&path, &meta);
            } else if meta.is_file() {
                if self.crosses_mount(&meta) {
                    self.counts.mount.fetch_add(1, Ordering::Relaxed);
                    continue;
                }
                self.emit(&path, &meta);
            }
            // Sockets, FIFOs, and devices are not eligible regular files.
        }
    }

    fn enter_dir(&mut self, path: &Path, meta: &std::fs::Metadata) {
        if self.crosses_mount(meta) {
            self.counts.mount.fetch_add(1, Ordering::Relaxed);
            return;
        }
        // Only needed when following symlinks, which is the one way a walk can
        // revisit a directory and loop forever.
        if self.config.follow_symlinks && !self.visited_dirs.insert((meta.dev(), meta.ino())) {
            return;
        }
        self.walk(path);
    }

    fn crosses_mount(&self, meta: &std::fs::Metadata) -> bool {
        self.config.skip_mount_boundaries && meta.dev() != self.base_dev
    }

    fn send_err(&self, path: &Path, e: &std::io::Error) {
        self.send(WalkMsg::Error {
            rel: Some(self.rel_of(path)),
            code: error_code(e),
            message: e.to_string(),
        });
    }

    fn emit(&self, path: &Path, meta: &std::fs::Metadata) {
        let size = meta.len() as i64;
        self.counts.discovered_files.fetch_add(1, Ordering::Relaxed);
        self.counts
            .discovered_bytes
            .fetch_add(size as u64, Ordering::Relaxed);
        self.send(WalkMsg::Item(Box::new(Observed {
            rel: self.rel_of(path),
            path: path.to_path_buf(),
            size_bytes: size,
            mtime_ns: system_time_ns(meta.modified().ok()).unwrap_or(0),
            created_ns: system_time_ns(meta.created().ok()),
        })));
    }
}

pub fn system_time_ns(t: Option<std::time::SystemTime>) -> Option<i64> {
    t.and_then(|t| t.duration_since(UNIX_EPOCH).ok())
        .map(|d| d.as_nanos() as i64)
}

/// Bounded error codes, so `scan_errors.error_code` stays a small enum rather
/// than free text.
pub const UNREADABLE_CODES: [&str; 3] = ["permission_denied", "io_error", "invalid_data"];

/// Splits the bounded codes into the only distinction promotion cares about.
/// An unreadable path leaves its contents unknown, so the baseline under it
/// cannot be trusted; a path that merely vanished or changed under us leaves
/// nothing in doubt and must not hold the whole scan hostage.
pub fn unreadable(code: &str) -> bool {
    UNREADABLE_CODES.contains(&code)
}

pub fn error_code(e: &std::io::Error) -> &'static str {
    match e.kind() {
        std::io::ErrorKind::NotFound => "not_found",
        std::io::ErrorKind::PermissionDenied => "permission_denied",
        std::io::ErrorKind::InvalidData => "invalid_data",
        _ => "io_error",
    }
}

/// Base-relative path split into the SQL search helpers. These are lossy TEXT on
/// purpose: `relative_path` stays the exact BINARY identity, these exist to make
/// SQLiteBrowser queries pleasant.
pub fn helpers(rel: &[u8]) -> (Option<String>, Option<String>, Option<String>) {
    let s = String::from_utf8_lossy(rel);
    let (parent, name) = match s.rfind('/') {
        Some(i) => (Some(s[..i].to_string()), s[i + 1..].to_string()),
        None => (None, s.to_string()),
    };
    let ext = name
        .rfind('.')
        .filter(|i| *i > 0 && *i + 1 < name.len())
        .map(|i| name[i + 1..].to_ascii_lowercase());
    (parent, Some(name), ext)
}

/// Extension-based only, recorded honestly. Never opens the file (design §3).
pub fn mime_of(rel: &[u8]) -> (Option<String>, &'static str) {
    let name = String::from_utf8_lossy(rel).to_string();
    match mime_guess::from_path(&name).first() {
        Some(m) => (Some(m.essence_str().to_string()), "extension"),
        None => (None, "unknown"),
    }
}
