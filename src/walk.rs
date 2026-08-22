//! Traversal and the three inclusion policies. The walker stats each entry once
//! and hands the result downstream, so a hash worker never re-stats before it
//! reads.

use std::collections::HashSet;
use std::os::unix::ffi::OsStrExt;
use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::mpsc::SyncSender;
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
    Item(Observed),
    Error {
        rel: Option<Vec<u8>>,
        code: &'static str,
        message: String,
    },
}

/// Every counter the pipeline shares, in one allocation. The walker fills the
/// discovery and exclusion halves, the hash workers the reuse half; keeping them
/// together is what lets a thread carry one `Arc` instead of seven.
#[derive(Default)]
pub struct Counters {
    pub hidden: AtomicU64,
    pub mount: AtomicU64,
    pub symlink: AtomicU64,
    pub discovered_files: AtomicU64,
    pub discovered_bytes: AtomicU64,
    pub hashed: AtomicU64,
    pub hashed_bytes: AtomicU64,
    pub reused_stage: AtomicU64,
    pub reused_baseline: AtomicU64,
    pub changed_during_hash: AtomicU64,
}

pub struct Walker<'a> {
    base: &'a Path,
    config: &'a Config,
    tx: SyncSender<WalkMsg>,
    cancelled: &'a AtomicBool,
    counts: Arc<Counters>,
    base_dev: u64,
    visited_dirs: HashSet<(u64, u64)>,
    /// Set when the queue's receivers are gone, which means the run is over and
    /// there is nothing left to walk for.
    downstream_gone: bool,
}

/// A directory whose descent is deferred until the parent's `ReadDir` has been
/// dropped. Only the two identity fields `enter_dir` needs are kept — holding a
/// `DirEntry` instead would keep the parent's handle alive through the `Arc`
/// inside it, which is the whole thing this defers to avoid.
struct Pending {
    path: PathBuf,
    dev: u64,
    ino: u64,
}

impl Pending {
    fn new(path: PathBuf, meta: &std::fs::Metadata) -> Pending {
        Pending {
            path,
            dev: meta.dev(),
            ino: meta.ino(),
        }
    }
}

impl<'a> Walker<'a> {
    pub fn new(
        base: &'a Path,
        config: &'a Config,
        tx: SyncSender<WalkMsg>,
        cancelled: &'a AtomicBool,
        counts: Arc<Counters>,
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
            downstream_gone: false,
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

    fn send(&mut self, msg: WalkMsg) {
        if self.tx.send(msg).is_err() {
            self.downstream_gone = true;
        }
    }

    fn send_err(&mut self, path: &Path, e: &std::io::Error) {
        let rel = self.rel_of(path);
        self.send(WalkMsg::Error {
            rel: Some(rel),
            code: error_code(e),
            message: e.to_string(),
        });
    }

    fn should_stop(&self) -> bool {
        self.cancelled.load(Ordering::Relaxed) || self.downstream_gone
    }

    fn walk(&mut self, dir: &Path) {
        if self.should_stop() {
            return;
        }
        let entries = match std::fs::read_dir(dir) {
            Ok(e) => e,
            Err(e) => return self.send_err(dir, &e),
        };

        // Files are emitted inline but directories are only remembered, so that
        // `entries` — this level's open directory handle — is dropped before the
        // first recursive call. Descending inside the loop instead would hold one
        // descriptor per level and hit EMFILE on a deep tree, abandoning the rest
        // of it. Open descriptors are now O(1) in depth; the cost is one `Pending`
        // per subdirectory of the levels on the current path.
        let mut pending: Vec<Pending> = Vec::new();
        for entry in entries {
            if self.should_stop() {
                return;
            }
            let entry = match entry {
                Ok(e) => e,
                Err(e) => {
                    self.send_err(dir, &e);
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
                    self.send_err(&path, &e);
                    continue;
                }
            };

            if meta.file_type().is_symlink() {
                self.visit_symlink(&path, &mut pending);
            } else if meta.is_dir() {
                pending.push(Pending::new(path, &meta));
            } else if meta.is_file() {
                self.visit_file(&path, &meta);
            }
            // Sockets, FIFOs, and devices are not eligible regular files.
        }

        for sub in pending {
            if self.should_stop() {
                return;
            }
            self.enter_dir(&sub);
        }
    }

    fn visit_symlink(&mut self, path: &Path, pending: &mut Vec<Pending>) {
        if !self.config.follow_symlinks {
            self.counts.symlink.fetch_add(1, Ordering::Relaxed);
            return;
        }
        match std::fs::metadata(path) {
            Ok(target) if target.is_dir() => {
                pending.push(Pending::new(path.to_path_buf(), &target))
            }
            // Deliberately not `visit_file`: a followed symlink to a file has
            // never been subject to the mount check, only one to a directory
            // has. Preserved as-is rather than quietly widening exclusion.
            Ok(target) if target.is_file() => self.emit(path, &target),
            Ok(_) => {}
            Err(e) => self.send_err(path, &e),
        }
    }

    fn visit_file(&mut self, path: &Path, meta: &std::fs::Metadata) {
        if self.crosses_mount(meta) {
            self.counts.mount.fetch_add(1, Ordering::Relaxed);
            return;
        }
        self.emit(path, meta);
    }

    fn enter_dir(&mut self, sub: &Pending) {
        if self.config.skip_mount_boundaries && sub.dev != self.base_dev {
            self.counts.mount.fetch_add(1, Ordering::Relaxed);
            return;
        }
        // Only needed when following symlinks, which is the one way a walk can
        // revisit a directory and loop forever.
        if self.config.follow_symlinks && !self.visited_dirs.insert((sub.dev, sub.ino)) {
            return;
        }
        self.walk(&sub.path);
    }

    fn crosses_mount(&self, meta: &std::fs::Metadata) -> bool {
        self.config.skip_mount_boundaries && meta.dev() != self.base_dev
    }

    fn emit(&mut self, path: &Path, meta: &std::fs::Metadata) {
        let size = meta.len() as i64;
        self.counts.discovered_files.fetch_add(1, Ordering::Relaxed);
        self.counts
            .discovered_bytes
            .fetch_add(size as u64, Ordering::Relaxed);
        let rel = self.rel_of(path);
        self.send(WalkMsg::Item(Observed {
            rel,
            path: path.to_path_buf(),
            size_bytes: size,
            mtime_ns: system_time_ns(meta.modified().ok()).unwrap_or(0),
            created_ns: system_time_ns(meta.created().ok()),
        }));
    }
}

pub fn system_time_ns(t: Option<std::time::SystemTime>) -> Option<i64> {
    t.and_then(|t| t.duration_since(UNIX_EPOCH).ok())
        .map(|d| d.as_nanos() as i64)
}

/// The codes that mean a path's contents are unknown, rather than known-absent.
/// This is the only distinction promotion cares about *below* the base: an
/// unreadable path leaves the baseline under it untrustworthy and so shields it,
/// while a path that merely vanished leaves nothing in doubt and must not hold
/// the whole scan hostage. The base itself is judged differently — see
/// `scan::base_unreadable`.
pub const UNREADABLE_CODES: [&str; 3] = ["permission_denied", "io_error", "invalid_data"];

pub fn error_code(e: &std::io::Error) -> &'static str {
    match e.kind() {
        std::io::ErrorKind::NotFound => "not_found",
        std::io::ErrorKind::PermissionDenied => "permission_denied",
        std::io::ErrorKind::InvalidData => "invalid_data",
        _ => "io_error",
    }
}

/// Base-relative path split into the SQL search helpers, plus the MIME guess
/// that extension implies. One lossy decode serves all four: `relative_path`
/// stays the exact BINARY identity, these exist to make SQLiteBrowser queries
/// pleasant, and the MIME guess is extension-based only — it never opens the
/// file (design §3).
pub struct Helpers {
    pub parent: Option<String>,
    pub name: String,
    pub extension: Option<String>,
    pub mime_type: Option<String>,
    pub mime_source: &'static str,
}

pub fn helpers(rel: &[u8]) -> Helpers {
    let s = String::from_utf8_lossy(rel);
    let (parent, name) = match s.rfind('/') {
        Some(i) => (Some(s[..i].to_string()), s[i + 1..].to_string()),
        None => (None, s.into_owned()),
    };
    // `Path::extension` semantics: a leading dot is a stem, a trailing one is
    // not an extension. `mime_guess` matches case-insensitively, so the
    // lowercased helper column is the same key `from_path` would have used.
    let extension = name
        .rfind('.')
        .filter(|i| *i > 0 && *i + 1 < name.len())
        .map(|i| name[i + 1..].to_ascii_lowercase());
    let (mime_type, mime_source) = match extension.as_deref().map(mime_guess::from_ext) {
        Some(guess) => match guess.first() {
            Some(m) => (Some(m.essence_str().to_string()), "extension"),
            None => (None, "unknown"),
        },
        None => (None, "unknown"),
    };
    Helpers {
        parent,
        name,
        extension,
        mime_type,
        mime_source,
    }
}
