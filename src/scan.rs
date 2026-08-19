//! The scan lifecycle of design §7: preflight, staging, validate, promote.
//!
//! The pipeline is walker -> bounded queue -> N hash workers -> bounded queue ->
//! staging writer. Only the writer holds the write connection; workers open
//! their own read-only connections, which WAL allows to run during a scan.

use std::borrow::Cow;
use std::io::Read;
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{Receiver, SyncSender, sync_channel};
use std::sync::{Arc, LazyLock, Mutex};
use std::time::{Duration, Instant};

use rusqlite::{Connection, OptionalExtension, params};

use crate::config::Config;
use crate::db;
use crate::history;
use crate::walk::{self, Counters, WalkMsg, Walker};
use crate::{AppError, now_ns};

/// `field_mask` bits in `scan_changes`.
const F_SIZE: i64 = 1;
const F_MTIME: i64 = 2;
const F_MIME: i64 = 4;
const F_HASH: i64 = 8;
const F_PRESENCE: i64 = 16;
const F_ADDED: i64 = F_SIZE | F_MTIME | F_MIME | F_HASH | F_PRESENCE;

const HASH_BUF: usize = 1024 * 1024;

/// The two lookups every hash worker runs, hoisted out of `worker` so the
/// pipeline can prove they prepare while a failure can still be returned.
const STAGED_LOOKUP_SQL: &str = "SELECT size_bytes, mtime_ns FROM scan_stage_entries
     WHERE scan_id = ?1 AND relative_path = ?2 AND complete = 1";
const BASELINE_LOOKUP_SQL: &str = "SELECT size_bytes, mtime_ns, content_hash FROM files
     WHERE base_id = ?1 AND relative_path = ?2 AND presence = 'present'
       AND hash_status = 'complete'";

/// The three statements the staging writer runs per batch. At module level with
/// the lookups above so every statement this file executes per row is in one
/// place rather than buried mid-function.
const STAGE_UPSERT_SQL: &str = "INSERT INTO scan_stage_entries
         (scan_id, base_id, relative_path, parent_path, name, extension,
          size_bytes, mtime_ns, created_ns, mime_type, mime_source,
          hash_algorithm, content_hash, hash_status, complete)
     VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, 'sha256', ?12, ?13, 1)
     ON CONFLICT(scan_id, relative_path) DO UPDATE SET
         parent_path = excluded.parent_path,
         name = excluded.name,
         extension = excluded.extension,
         size_bytes = excluded.size_bytes,
         mtime_ns = excluded.mtime_ns,
         created_ns = excluded.created_ns,
         mime_type = excluded.mime_type,
         mime_source = excluded.mime_source,
         content_hash = excluded.content_hash,
         hash_status = excluded.hash_status,
         complete = 1";
const WALK_PATH_SQL: &str = "INSERT OR IGNORE INTO walk_paths (relative_path) VALUES (?1)";
const SCAN_ERROR_SQL: &str = "INSERT INTO scan_errors
         (scan_id, relative_path, stage, error_code, message)
     VALUES (?1, ?2, ?3, ?4, ?5)";

/// `walk::UNREADABLE_CODES` as a SQL `IN` list, built once. Derived from the
/// array rather than written out, so the two can never drift apart.
static UNREADABLE_CODES_SQL: LazyLock<String> =
    LazyLock::new(|| walk::UNREADABLE_CODES.map(|c| format!("'{c}'")).join(", "));

pub struct Outcome {
    pub scan_id: i64,
    pub resumed: bool,
    pub status: &'static str,
    pub added: usize,
    pub updated: usize,
    pub deleted: usize,
    pub unchanged: usize,
    pub present: usize,
    pub staged: usize,
    pub hashed: u64,
    pub hashed_bytes: u64,
    pub reused_stage: u64,
    pub reused_baseline: u64,
    pub errors: usize,
    pub low_space: bool,
    /// Why a scan can stop with no errors of its own worth listing: the base
    /// itself went unread, which shields the whole baseline from promotion.
    pub base_unreadable: bool,
    pub pruned_changes: u64,
    pub pruned_files: u64,
    pub unreadable: usize,
    pub unreadable_paths: usize,
    pub discovered_bytes: u64,
    pub excluded_hidden: u64,
    pub excluded_mount: u64,
    pub excluded_symlink: u64,
    pub free_before: u64,
    pub required_free: u64,
    pub estimate_source: String,
    pub duration_ms: u64,
}

struct StagedRow {
    rel: Vec<u8>,
    size_bytes: i64,
    mtime_ns: i64,
    created_ns: Option<i64>,
    content_hash: Vec<u8>,
    hash_status: &'static str,
}

enum WriteMsg {
    Row(StagedRow),
    /// Already staged durably by an earlier attempt; nothing to write.
    Reused(Vec<u8>),
    Error {
        rel: Option<Vec<u8>>,
        stage: &'static str,
        code: &'static str,
        message: String,
    },
}

/// The base row this scan runs against, and the baseline promotion will move.
struct Base {
    id: i64,
    baseline: Option<i64>,
}

/// The free-space decision, made once in preflight and reported verbatim.
struct Space {
    free_before: u64,
    required: u64,
    source: String,
}

pub fn ingest(
    conn: &mut Connection,
    db_path: &Path,
    config: &Config,
    base_path: &Path,
    cancelled: &Arc<AtomicBool>,
) -> Result<Outcome, AppError> {
    let started = Instant::now();

    // 1-2. Register or match the base, then validate the inclusion policy before
    // anything touches staging. A rescan under different rules could otherwise
    // mark a whole excluded subtree deleted.
    let base = register_base(conn, config, base_path)?;
    // 3. Preflight. Refuse before a scan row exists.
    let prior_files = present_count(conn, base.id)?;
    let space = preflight(db_path, config)?;
    // 4. Resume a non-terminal scan, or start a new one.
    let (scan_id, resumed) = open_scan(conn, config, &base, &space)?;
    reset_walk_paths(conn)?;

    let low_space = Arc::new(AtomicBool::new(false));
    let counts = Arc::new(Counters::default());

    let errors = Pipeline {
        db_path,
        config,
        base_path,
        base_id: base.id,
        scan_id,
        prior_files,
        cancelled,
        low_space: &low_space,
        counts: &counts,
    }
    .run(conn)?;

    let stopped = cancelled.load(Ordering::SeqCst);
    let (staged, staged_bytes) = staged_totals(conn, scan_id)?;
    record_walk_totals(conn, scan_id, &counts, staged, staged_bytes, errors)?;

    let mut outcome = new_outcome(scan_id, resumed, staged, errors, &counts, space);
    outcome.unreadable_paths = unreadable_path_count(conn, scan_id)?;
    outcome.base_unreadable = base_unreadable(conn, scan_id)?;
    outcome.low_space = low_space.load(Ordering::SeqCst);

    // 7. File-level errors no longer block: a vanished file is simply gone, and
    // an unreadable directory is shielded at promotion rather than allowed to
    // record its contents as deleted. A cancelled scan still keeps its staging
    // and leaves the baseline exactly as it was, and so does an unreadable base
    // — that one failure puts every path in the baseline in doubt at once, and
    // no prefix can shield them because the base prefixes everything.
    if stopped || outcome.base_unreadable {
        outcome.duration_ms = started.elapsed().as_millis() as u64;
        finish_unpromoted(conn, db_path, config, stopped, &mut outcome)?;
        return Ok(outcome);
    }

    // Every attempt re-walks, so a path staged earlier but absent now must not
    // survive into promotion as present.
    drop_vanished_staged(conn, scan_id)?;
    // The counts written before that cleanup are the truth for a scan that stops
    // early — its staging is kept exactly as it stands — but a scan that reaches
    // here has just dropped rows the re-walk did not see, and the stored columns
    // would otherwise keep claiming them.
    let (staged_after, staged_bytes_after) = staged_totals(conn, scan_id)?;
    outcome.staged = staged_after;

    promote(conn, scan_id, base.id, &mut outcome)?;
    run_retention(conn, config, base.id, &mut outcome);

    outcome.duration_ms = started.elapsed().as_millis() as u64;
    conn.execute(
        "UPDATE scans SET duration_ms = ?1, free_bytes_after = ?2,
             staged_files = ?4, staged_bytes = ?5
         WHERE scan_id = ?3",
        params![
            outcome.duration_ms as i64,
            free_bytes(db_path).unwrap_or(0) as i64,
            scan_id,
            staged_after as i64,
            staged_bytes_after
        ],
    )
    .map_err(AppError::db)?;
    Ok(outcome)
}

/// Registers the base if it is new, then refuses if the configured inclusion
/// policy is not the one its baseline was built with.
fn register_base(conn: &Connection, config: &Config, base_path: &Path) -> Result<Base, AppError> {
    let base_str = base_path.to_string_lossy().into_owned();
    conn.execute(
        "INSERT OR IGNORE INTO bases
             (base_path, created_at_ns, skip_hidden, skip_mount_boundaries, follow_symlinks)
         VALUES (?1, ?2, ?3, ?4, ?5)",
        params![
            base_str,
            now_ns(),
            config.skip_hidden,
            config.skip_mount_boundaries,
            config.follow_symlinks
        ],
    )
    .map_err(AppError::db)?;

    let (id, baseline, skip_hidden, skip_mount, follow_symlinks): (
        i64,
        Option<i64>,
        bool,
        bool,
        bool,
    ) = conn
        .query_row(
            "SELECT base_id, last_complete_scan_id, skip_hidden, skip_mount_boundaries,
                    follow_symlinks
             FROM bases WHERE base_path = ?1",
            params![base_str],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?, r.get(4)?)),
        )
        .map_err(AppError::db)?;

    if skip_hidden != config.skip_hidden
        || skip_mount != config.skip_mount_boundaries
        || follow_symlinks != config.follow_symlinks
    {
        let msg = format!(
            "base was indexed with skip_hidden={skip_hidden}, \
             skip_mount_boundaries={skip_mount}, follow_symlinks={follow_symlinks}; \
             configuration now says {}, {}, {}",
            config.skip_hidden, config.skip_mount_boundaries, config.follow_symlinks
        );
        conn.execute(
            "UPDATE bases SET last_error = ?1 WHERE base_id = ?2",
            params![msg, id],
        )
        .map_err(AppError::db)?;
        return Err(AppError::policy_mismatch(msg));
    }
    Ok(Base { id, baseline })
}

fn present_count(conn: &Connection, base_id: i64) -> Result<u64, AppError> {
    conn.query_row(
        "SELECT COUNT(*) FROM files WHERE base_id = ?1 AND presence = 'present'",
        params![base_id],
        |r| r.get::<_, i64>(0).map(|n| n as u64),
    )
    .map_err(AppError::db)
}

fn preflight(db_path: &Path, config: &Config) -> Result<Space, AppError> {
    let required = free_space_floor(config);
    let free_before = free_bytes(db_path)?;
    if free_before < required {
        return Err(AppError::no_space(free_before, required));
    }
    Ok(Space {
        free_before,
        required,
        source: format!(
            "storage.minimum_free_space_mib = {}, rechecked every batch",
            config.minimum_free_space_mib
        ),
    })
}

/// Resume a non-terminal scan, or start a new one. A scan left `running` is a
/// crash; `cancelled`/`partial`/`failed` are ordinary stops. All are durable
/// completed work worth reusing.
fn open_scan(
    conn: &Connection,
    config: &Config,
    base: &Base,
    space: &Space,
) -> Result<(i64, bool), AppError> {
    let resumable: Option<i64> = conn
        .query_row(
            "SELECT scan_id FROM scans
             WHERE base_id = ?1 AND status IN ('running','cancelled','failed','partial')
             ORDER BY scan_id DESC LIMIT 1",
            params![base.id],
            |r| r.get(0),
        )
        .optional()
        .map_err(AppError::db)?;

    if let Some(id) = resumable {
        conn.execute(
            "UPDATE scans SET status = 'running', failure_code = NULL,
                 failure_message = NULL WHERE scan_id = ?1",
            params![id],
        )
        .map_err(AppError::db)?;
        // Diagnostics from the attempt we are retrying describe a state that no
        // longer holds; keeping them would contradict this run's counts.
        conn.execute("DELETE FROM scan_errors WHERE scan_id = ?1", params![id])
            .map_err(AppError::db)?;
        return Ok((id, true));
    }

    conn.execute(
        "DELETE FROM scan_stage_entries WHERE base_id = ?1",
        params![base.id],
    )
    .map_err(AppError::db)?;
    conn.execute(
        "INSERT INTO scans
             (base_id, baseline_scan_id, status, started_at_ns,
              skip_hidden, skip_mount_boundaries, follow_symlinks,
              workers, writer_batch_rows, throttle_ms_after_batch, hash_policy,
              free_bytes_before, required_free_bytes, estimate_source)
         VALUES (?1, ?2, 'running', ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13)",
        params![
            base.id,
            base.baseline,
            now_ns(),
            config.skip_hidden,
            config.skip_mount_boundaries,
            config.follow_symlinks,
            config.workers as i64,
            config.writer_batch_rows as i64,
            config.throttle_ms_after_batch as i64,
            config.hash_policy,
            space.free_before as i64,
            space.required as i64,
            space.source
        ],
    )
    .map_err(AppError::db)?;
    Ok((conn.last_insert_rowid(), false))
}

fn reset_walk_paths(conn: &Connection) -> Result<(), AppError> {
    conn.execute_batch(
        "CREATE TEMP TABLE IF NOT EXISTS walk_paths (relative_path BLOB PRIMARY KEY);
         DELETE FROM walk_paths;",
    )
    .map_err(AppError::db)
}

/// Rows staged durably, and their bytes. Read before promotion and again after
/// the re-walk cleanup, so the two callers can never disagree on the shape.
fn staged_totals(conn: &Connection, scan_id: i64) -> Result<(usize, i64), AppError> {
    conn.query_row(
        "SELECT COUNT(*), IFNULL(SUM(size_bytes), 0) FROM scan_stage_entries
         WHERE scan_id = ?1 AND complete = 1",
        params![scan_id],
        |r| Ok((r.get::<_, i64>(0)? as usize, r.get(1)?)),
    )
    .map_err(AppError::db)
}

fn record_walk_totals(
    conn: &Connection,
    scan_id: i64,
    counts: &Counters,
    staged: usize,
    staged_bytes: i64,
    errors: usize,
) -> Result<(), AppError> {
    conn.execute(
        "UPDATE scans SET discovered_files = ?1, discovered_bytes = ?2, staged_files = ?3,
             staged_bytes = ?12,
             hashed_files = ?4, hashed_bytes = ?5, changed_during_hash = ?6,
             excluded_hidden = ?7, excluded_mount = ?8, excluded_symlink = ?9,
             error_count = ?10
         WHERE scan_id = ?11",
        params![
            counts.discovered_files.load(Ordering::Relaxed) as i64,
            counts.discovered_bytes.load(Ordering::Relaxed) as i64,
            staged as i64,
            counts.hashed.load(Ordering::Relaxed) as i64,
            counts.hashed_bytes.load(Ordering::Relaxed) as i64,
            counts.changed_during_hash.load(Ordering::Relaxed) as i64,
            counts.hidden.load(Ordering::Relaxed) as i64,
            counts.mount.load(Ordering::Relaxed) as i64,
            counts.symlink.load(Ordering::Relaxed) as i64,
            errors as i64,
            scan_id,
            staged_bytes
        ],
    )
    .map_err(AppError::db)?;
    Ok(())
}

fn new_outcome(
    scan_id: i64,
    resumed: bool,
    staged: usize,
    errors: usize,
    counts: &Counters,
    space: Space,
) -> Outcome {
    Outcome {
        scan_id,
        resumed,
        status: "complete",
        added: 0,
        updated: 0,
        deleted: 0,
        unchanged: 0,
        present: 0,
        staged,
        hashed: counts.hashed.load(Ordering::Relaxed),
        hashed_bytes: counts.hashed_bytes.load(Ordering::Relaxed),
        reused_stage: counts.reused_stage.load(Ordering::Relaxed),
        reused_baseline: counts.reused_baseline.load(Ordering::Relaxed),
        errors,
        low_space: false,
        base_unreadable: false,
        pruned_changes: 0,
        pruned_files: 0,
        unreadable: 0,
        unreadable_paths: 0,
        discovered_bytes: counts.discovered_bytes.load(Ordering::Relaxed),
        excluded_hidden: counts.hidden.load(Ordering::Relaxed),
        excluded_mount: counts.mount.load(Ordering::Relaxed),
        excluded_symlink: counts.symlink.load(Ordering::Relaxed),
        free_before: space.free_before,
        required_free: space.required,
        estimate_source: space.source,
        duration_ms: 0,
    }
}

/// Closes out a scan that must not promote: staging is kept for the resume and
/// the baseline is left exactly as it was.
fn finish_unpromoted(
    conn: &Connection,
    db_path: &Path,
    config: &Config,
    stopped: bool,
    outcome: &mut Outcome,
) -> Result<(), AppError> {
    let (status, code) = if outcome.low_space {
        ("partial", "low_space")
    } else if stopped {
        ("cancelled", "cancelled")
    } else {
        ("partial", "base_unreadable")
    };
    let message = if outcome.low_space {
        format!(
            "stopped at the {} MiB free-space floor; free space and rerun to resume",
            config.minimum_free_space_mib
        )
    } else if outcome.base_unreadable {
        "base directory is unreadable; baseline left unchanged".to_string()
    } else {
        format!("{} error(s); baseline left unchanged", outcome.errors)
    };
    outcome.status = status;
    conn.execute(
        "UPDATE scans SET status = ?1, finished_at_ns = ?2, duration_ms = ?3,
             failure_code = ?4, failure_message = ?5, free_bytes_after = ?6
         WHERE scan_id = ?7",
        params![
            status,
            now_ns(),
            outcome.duration_ms as i64,
            code,
            message,
            free_bytes(db_path).unwrap_or(0) as i64,
            outcome.scan_id
        ],
    )
    .map_err(AppError::db)?;
    Ok(())
}

fn drop_vanished_staged(conn: &Connection, scan_id: i64) -> Result<(), AppError> {
    conn.execute(
        "DELETE FROM scan_stage_entries WHERE scan_id = ?1
         AND relative_path NOT IN (SELECT relative_path FROM walk_paths)",
        params![scan_id],
    )
    .map_err(AppError::db)?;
    Ok(())
}

/// Retention runs after promotion and outside its transaction. Promotion is the
/// one atomic step that moves the baseline; folding a large delete into it would
/// stretch the window where a crash discards the whole scan. A failure here must
/// not undo a scan that already succeeded, so it is reported and swallowed.
fn run_retention(conn: &Connection, config: &Config, base_id: i64, outcome: &mut Outcome) {
    if config.keep_scans == 0 {
        return;
    }
    match history::prune_base(conn, config, base_id, true) {
        Ok(p) => {
            outcome.pruned_changes = p.changes;
            outcome.pruned_files = p.files;
        }
        Err(e) => eprintln!("metainjester: history prune failed: {}", e.message),
    }
}

/// Everything the walker, the hash workers, and the writer need in common. It
/// exists so the three spawns read as one unit rather than a dozen parameters
/// threaded through by hand.
struct Pipeline<'a> {
    db_path: &'a Path,
    config: &'a Config,
    base_path: &'a Path,
    base_id: i64,
    scan_id: i64,
    prior_files: u64,
    cancelled: &'a Arc<AtomicBool>,
    low_space: &'a Arc<AtomicBool>,
    counts: &'a Arc<Counters>,
}

impl Pipeline<'_> {
    /// Runs the walk to completion and returns the error count staged along the
    /// way. The writer runs on this thread; the rest are scoped.
    fn run(&self, conn: &mut Connection) -> Result<usize, AppError> {
        let (path_tx, path_rx) = sync_channel::<WalkMsg>(self.config.queue_items);
        let (row_tx, row_rx) = sync_channel::<WriteMsg>(self.config.queue_items);
        let path_rx = Arc::new(Mutex::new(path_rx));

        std::thread::scope(|scope| -> Result<usize, AppError> {
            self.spawn_walker(scope, path_tx.clone());
            drop(path_tx);

            for _ in 0..self.config.workers {
                // A worker that cannot prepare its lookups used to return
                // quietly. If every worker did that — WAL shared memory
                // unavailable, say — the scan staged nothing and recorded no
                // error, which is indistinguishable from a walk of an empty
                // tree, and promotion recorded the whole baseline as deleted.
                // Prove them here, where the failure is still a returnable error
                // and no scan is promoted.
                let reader = db::open_reader(self.db_path)?;
                reader.prepare(STAGED_LOOKUP_SQL).map_err(AppError::db)?;
                reader.prepare(BASELINE_LOOKUP_SQL).map_err(AppError::db)?;

                let mut w = Worker {
                    reader,
                    scan_id: self.scan_id,
                    base_id: self.base_id,
                    rx: path_rx.clone(),
                    tx: row_tx.clone(),
                    cancelled: self.cancelled.clone(),
                    counts: self.counts.clone(),
                };
                scope.spawn(move || w.run());
            }
            drop(row_tx);
            // The workers are now the only receiver holders. Keeping a clone
            // here would leave the walker blocked in a full-queue send after a
            // cancel, because send only fails once every receiver is gone.
            drop(path_rx);

            // A writer that gives up has to say so. Its receiver dropping is not
            // a signal anyone upstream acts on, so without this the walker and
            // the hash workers keep going and `thread::scope` cannot return
            // until they have chewed through the whole tree for output nothing
            // will read.
            let result = self.writer(conn, row_rx);
            if result.is_err() {
                self.cancelled.store(true, Ordering::SeqCst);
            }
            result
        })
    }

    fn spawn_walker<'s>(&'s self, scope: &'s std::thread::Scope<'s, '_>, tx: SyncSender<WalkMsg>) {
        let (base, config) = (self.base_path, self.config);
        let counts = self.counts.clone();
        let cancelled = self.cancelled.clone();
        // `Walker::new` consumes the sender, so reporting its failure needs a
        // second clone.
        let err_tx = tx.clone();
        scope.spawn(move || match Walker::new(base, config, tx, &cancelled, counts) {
            Ok(mut w) => w.run(),
            // A base we cannot stat is a base we cannot read, and it prefixes
            // every path in the baseline. This must travel the same channel as a
            // failed `read_dir` on the base — an error row on the empty relative
            // path — because that row is the only thing `base_unreadable` looks
            // for when it decides whether promotion may proceed. Printing the
            // error instead would leave a scan that looks like a successful walk
            // of an empty tree, and promotion would then record the entire
            // baseline deleted.
            Err(e) => {
                let _ = err_tx.send(WalkMsg::Error {
                    rel: Some(Vec::new()),
                    code: walk::error_code(&e),
                    message: format!("cannot stat base: {e}"),
                });
            }
        });
    }

    /// The single staging writer. Commits in batches so an interrupted scan
    /// keeps everything already durable, and throttles only between batches.
    fn writer(&self, conn: &mut Connection, rx: Receiver<WriteMsg>) -> Result<usize, AppError> {
        let mut errors = 0usize;
        let mut processed = 0u64;
        let mut batch: Vec<WriteMsg> = Vec::with_capacity(self.config.writer_batch_rows);
        let mut progress = Progress::new(self.prior_files);

        while let Ok(first) = rx.recv() {
            batch.push(first);
            while batch.len() < self.config.writer_batch_rows {
                let Ok(msg) = rx.try_recv() else { break };
                batch.push(msg);
            }
            // Errors are not files, so they do not count toward a total drawn
            // from a previous scan's file count.
            let files = batch
                .iter()
                .filter(|m| !matches!(m, WriteMsg::Error { .. }))
                .count() as u64;
            errors += self.flush(conn, &mut batch)?;
            processed += files;
            // The batch just committed is the last thing written before the
            // floor is rechecked, so the scan stops with room still left rather
            // than after taking it. Cancelling is how the walker and the hash
            // workers hear about it; `low_space` is what tells them apart from a
            // Ctrl-C.
            if free_bytes(self.db_path)? < free_space_floor(self.config) {
                self.low_space.store(true, Ordering::SeqCst);
                self.cancelled.store(true, Ordering::SeqCst);
                break;
            }
            progress.tick(
                self.counts.discovered_files.load(Ordering::Relaxed),
                processed,
                self.cancelled,
            );
            if self.config.throttle_ms_after_batch > 0 {
                std::thread::sleep(Duration::from_millis(self.config.throttle_ms_after_batch));
            }
        }
        errors += self.flush(conn, &mut batch)?;
        progress.finish();
        Ok(errors)
    }

    fn flush(&self, conn: &mut Connection, batch: &mut Vec<WriteMsg>) -> Result<usize, AppError> {
        if batch.is_empty() {
            return Ok(0);
        }
        let mut errors = 0usize;
        let tx = conn.transaction().map_err(AppError::db)?;
        {
            let mut stage = tx.prepare_cached(STAGE_UPSERT_SQL).map_err(AppError::db)?;
            let mut seen = tx.prepare_cached(WALK_PATH_SQL).map_err(AppError::db)?;
            let mut err_stmt = tx.prepare_cached(SCAN_ERROR_SQL).map_err(AppError::db)?;

            for msg in batch.drain(..) {
                match msg {
                    WriteMsg::Row(row) => {
                        // One lossy decode per row yields the three helper
                        // columns and the MIME guess together.
                        let h = walk::helpers(&row.rel);
                        stage
                            .execute(params![
                                self.scan_id,
                                self.base_id,
                                &row.rel,
                                h.parent,
                                h.name,
                                h.extension,
                                row.size_bytes,
                                row.mtime_ns,
                                row.created_ns,
                                h.mime_type,
                                h.mime_source,
                                row.content_hash,
                                row.hash_status
                            ])
                            .map_err(AppError::db)?;
                        seen.execute(params![&row.rel]).map_err(AppError::db)?;
                    }
                    WriteMsg::Reused(rel) => {
                        seen.execute(params![&rel]).map_err(AppError::db)?;
                    }
                    WriteMsg::Error {
                        rel,
                        stage: st,
                        code,
                        message,
                    } => {
                        errors += 1;
                        err_stmt
                            .execute(params![self.scan_id, rel, st, code, truncate(&message)])
                            .map_err(AppError::db)?;
                    }
                }
            }
        }
        tx.commit().map_err(AppError::db)?;
        Ok(errors)
    }
}

/// One hash worker. Every send is checked: a failed send means the writer is
/// gone, and continuing past that hashes the rest of the tree into a dead
/// channel while the scope waits for it.
struct Worker {
    reader: Connection,
    scan_id: i64,
    base_id: i64,
    rx: Arc<Mutex<Receiver<WalkMsg>>>,
    tx: SyncSender<WriteMsg>,
    cancelled: Arc<AtomicBool>,
    counts: Arc<Counters>,
}

/// Whether the worker should keep pulling from the queue. `Stop` always means
/// the far end is gone or the scan was cancelled — never a per-file problem.
#[derive(PartialEq)]
enum Flow {
    Continue,
    Stop,
}

impl Worker {
    fn run(&mut self) {
        // The pipeline already proved these prepare, so reaching the failure arm
        // is a race with something changing underneath us. It still must not be
        // quiet: a worker that returns without a word leaves a walk that looks
        // empty.
        let mut staged_lookup = match self.reader.prepare(STAGED_LOOKUP_SQL) {
            Ok(s) => s,
            Err(e) => return self.died(e),
        };
        let mut baseline_lookup = match self.reader.prepare(BASELINE_LOOKUP_SQL) {
            Ok(s) => s,
            Err(e) => return self.died(e),
        };
        // Hashing reuses one buffer for the worker's whole life. Allocating and
        // zeroing a megabyte per file cost far more than the reads did on a tree
        // of mostly small files.
        let mut buf = vec![0u8; HASH_BUF];

        while let Some(msg) = self.next() {
            let flow = match msg {
                WalkMsg::Error { rel, code, message } => self.send(WriteMsg::Error {
                    rel,
                    stage: "walk",
                    code,
                    message,
                }),
                WalkMsg::Item(item) => {
                    self.stage(item, &mut staged_lookup, &mut baseline_lookup, &mut buf)
                }
            };
            if flow == Flow::Stop {
                return;
            }
        }
    }

    /// The next walk message, or `None` when the worker should stop.
    fn next(&self) -> Option<WalkMsg> {
        if self.cancelled.load(Ordering::Relaxed) {
            return None;
        }
        let msg = self.rx.lock().ok()?.recv();
        msg.ok()
    }

    fn send(&self, msg: WriteMsg) -> Flow {
        match self.tx.send(msg) {
            Ok(()) => Flow::Continue,
            Err(_) => Flow::Stop,
        }
    }

    /// Reuse order: a durable staged row from this scan, then the baseline hash,
    /// then hashing the file. Both reuses require size and mtime to match
    /// exactly.
    fn stage(
        &self,
        item: walk::Observed,
        staged_lookup: &mut rusqlite::Statement<'_>,
        baseline_lookup: &mut rusqlite::Statement<'_>,
        buf: &mut [u8],
    ) -> Flow {
        let staged_hit = staged_lookup
            .query_row(params![self.scan_id, &item.rel], |r| {
                Ok((r.get::<_, i64>(0)?, r.get::<_, i64>(1)?))
            })
            .optional()
            .ok()
            .flatten();
        if let Some((s, m)) = staged_hit
            && s == item.size_bytes
            && m == item.mtime_ns
        {
            self.counts.reused_stage.fetch_add(1, Ordering::Relaxed);
            return self.send(WriteMsg::Reused(item.rel));
        }

        let prior = baseline_lookup
            .query_row(params![self.base_id, &item.rel], |r| {
                Ok((
                    r.get::<_, i64>(0)?,
                    r.get::<_, i64>(1)?,
                    r.get::<_, Vec<u8>>(2)?,
                ))
            })
            .optional()
            .ok()
            .flatten();

        let (content_hash, hash_status) = match prior {
            Some((s, m, h)) if s == item.size_bytes && m == item.mtime_ns => {
                self.counts.reused_baseline.fetch_add(1, Ordering::Relaxed);
                (h, "complete")
            }
            _ => match self.hash(&item, buf) {
                Ok(pair) => pair,
                Err(flow) => return flow,
            },
        };

        self.send(WriteMsg::Row(StagedRow {
            rel: item.rel,
            size_bytes: item.size_bytes,
            mtime_ns: item.mtime_ns,
            created_ns: item.created_ns,
            content_hash,
            hash_status,
        }))
    }

    /// Hashes the file and decides whether the digest can be trusted. `Err`
    /// carries the flow decision for a failure already reported downstream.
    fn hash(
        &self,
        item: &walk::Observed,
        buf: &mut [u8],
    ) -> Result<(Vec<u8>, &'static str), Flow> {
        let h = match hash_file(&item.path, buf) {
            Ok(h) => h,
            Err(e) => {
                let flow = self.send(WriteMsg::Error {
                    rel: Some(item.rel.clone()),
                    stage: "hash",
                    code: walk::error_code(&e),
                    message: e.to_string(),
                });
                // Nothing to stage for a file we could not read, so this file is
                // finished either way.
                return Err(if flow == Flow::Stop {
                    Flow::Stop
                } else {
                    Flow::Continue
                });
            }
        };
        self.counts.hashed.fetch_add(1, Ordering::Relaxed);
        self.counts
            .hashed_bytes
            .fetch_add(item.size_bytes.max(0) as u64, Ordering::Relaxed);

        // A file rewritten while we streamed it produces a digest of no
        // particular version, so say so rather than store it.
        let stale = match std::fs::metadata(&item.path) {
            Ok(after) => {
                after.len() as i64 != item.size_bytes
                    || walk::system_time_ns(after.modified().ok()).unwrap_or(0) != item.mtime_ns
            }
            Err(_) => false,
        };
        if !stale {
            return Ok((h, "complete"));
        }
        self.counts
            .changed_during_hash
            .fetch_add(1, Ordering::Relaxed);
        if self.send(WriteMsg::Error {
            rel: Some(item.rel.clone()),
            stage: "hash",
            code: "changed_during_hash",
            message: "file changed while being hashed".into(),
        }) == Flow::Stop
        {
            return Err(Flow::Stop);
        }
        Ok((h, "changed_during_hash"))
    }

    /// A hash worker that cannot start. Cancelling is what keeps the baseline
    /// safe: it stops the scan short of promotion and keeps the staging for a
    /// resume, rather than letting an empty-looking walk speak for the whole
    /// tree.
    fn died(&self, e: rusqlite::Error) {
        self.cancelled.store(true, Ordering::SeqCst);
        let _ = self.tx.send(WriteMsg::Error {
            rel: None,
            stage: "worker",
            code: "io_error",
            message: format!("hash worker could not start: {e}"),
        });
    }
}

fn truncate(s: &str) -> Cow<'_, str> {
    const MAX: usize = 300;
    if s.len() <= MAX {
        return Cow::Borrowed(s);
    }
    let mut end = MAX;
    while end > 0 && !s.is_char_boundary(end) {
        end -= 1;
    }
    Cow::Owned(format!("{}…", &s[..end]))
}

/// How many distinct paths the walk could not read. The summary reports only
/// this count; `scan_errors` holds which ones, for the query the user runs.
fn unreadable_path_count(conn: &Connection, scan_id: i64) -> Result<usize, AppError> {
    conn.query_row(
        &format!(
            "SELECT COUNT(DISTINCT relative_path) FROM scan_errors
             WHERE scan_id = ?1 AND error_code IN ({})",
            *UNREADABLE_CODES_SQL
        ),
        params![scan_id],
        |r| r.get::<_, i64>(0).map(|n| n as usize),
    )
    .map_err(AppError::db)
}

/// True when the walk could not read the base itself, which arrives as an error
/// on the empty relative path.
///
/// Any error code counts here, unlike the `UNREADABLE_CODES` that shield paths
/// deeper in the tree. Nothing can produce a row on the empty relative path
/// having actually enumerated the base, so its presence always means the base
/// went unread — and an unread base prefixes every path in the baseline at
/// once. Refusing costs one rerun; promoting on this evidence records the whole
/// tree as deleted, which is the expensive direction to be wrong in.
fn base_unreadable(conn: &Connection, scan_id: i64) -> Result<bool, AppError> {
    conn.query_row(
        "SELECT EXISTS (SELECT 1 FROM scan_errors
                        WHERE scan_id = ?1 AND relative_path IS NOT NULL
                          AND length(relative_path) = 0)",
        params![scan_id],
        |r| r.get::<_, i64>(0).map(|n| n != 0),
    )
    .map_err(AppError::db)
}

/// SQL predicate: `alias`'s path is an unreadable path from this scan, or lies
/// under one. Comparison is byte-wise on purpose — `relative_path` is a BLOB
/// holding the exact filesystem identity, and SQLite's `||` would coerce it to
/// text and mangle any path that is not valid UTF-8. Assumes `?1` is `scan_id`.
fn shielded(alias: &str) -> String {
    format!(
        "EXISTS (SELECT 1 FROM scan_errors e
                 WHERE e.scan_id = ?1 AND e.relative_path IS NOT NULL
                   AND length(e.relative_path) > 0
                   AND e.error_code IN ({codes})
                   AND ({alias}.relative_path = e.relative_path
                        OR (substr({alias}.relative_path, 1, length(e.relative_path))
                              = e.relative_path
                            AND substr({alias}.relative_path,
                                       length(e.relative_path) + 1, 1) = x'2f')))",
        codes = *UNREADABLE_CODES_SQL
    )
}

/// Promotion (design §7 step 8), in one transaction. Each step below is one
/// statement against that transaction, in an order the next step depends on:
/// change rows are written first because they read the outgoing `files` state,
/// and the presence updates run after the upsert has claimed everything the
/// scan actually saw.
fn promote(
    conn: &mut Connection,
    scan_id: i64,
    base_id: i64,
    outcome: &mut Outcome,
) -> Result<(), AppError> {
    let tx = conn.transaction().map_err(AppError::db)?;

    let added = record_added(&tx, scan_id)?;
    let updated = record_updated(&tx, scan_id)?;
    let deleted = record_deleted(&tx, scan_id, base_id)?;
    upsert_present(&tx, scan_id)?;
    let unreadable = mark_unreadable(&tx, scan_id, base_id)?;
    mark_deleted(&tx, scan_id, base_id)?;

    let present: i64 = tx
        .query_row(
            "SELECT COUNT(*) FROM files WHERE base_id = ?1 AND presence = 'present'",
            params![base_id],
            |r| r.get(0),
        )
        .map_err(AppError::db)?;
    let unchanged = outcome.staged.saturating_sub(added + updated);

    finalize_scan(
        &tx,
        scan_id,
        base_id,
        &Promoted {
            added,
            updated,
            deleted,
            unchanged,
            present,
            errors: outcome.errors,
        },
    )?;
    tx.commit().map_err(AppError::db)?;

    outcome.added = added;
    outcome.updated = updated;
    outcome.deleted = deleted;
    outcome.unchanged = unchanged;
    outcome.present = present as usize;
    outcome.unreadable = unreadable;
    Ok(())
}

/// The counts promotion writes back to the `scans` row.
struct Promoted {
    added: usize,
    updated: usize,
    deleted: usize,
    unchanged: usize,
    present: i64,
    errors: usize,
}

/// Staged paths the baseline does not hold, or holds only as `deleted`.
fn record_added(tx: &rusqlite::Transaction<'_>, scan_id: i64) -> Result<usize, AppError> {
    tx.execute(
        "INSERT INTO scan_changes
             (scan_id, base_id, relative_path, change_kind, field_mask,
              old_size_bytes, new_size_bytes, old_mtime_ns, new_mtime_ns,
              old_mime_type, new_mime_type, old_content_hash, new_content_hash)
         SELECT s.scan_id, s.base_id, s.relative_path, 'added', ?2,
                f.size_bytes, s.size_bytes, f.mtime_ns, s.mtime_ns,
                f.mime_type, s.mime_type, f.content_hash, s.content_hash
         FROM scan_stage_entries s
         LEFT JOIN files f ON f.base_id = s.base_id AND f.relative_path = s.relative_path
         WHERE s.scan_id = ?1 AND s.complete = 1
           AND (f.file_id IS NULL OR f.presence = 'deleted')",
        params![scan_id, F_ADDED],
    )
    .map_err(AppError::db)
}

/// Staged paths whose size, mtime, MIME, or hash moved. The mask records which.
fn record_updated(tx: &rusqlite::Transaction<'_>, scan_id: i64) -> Result<usize, AppError> {
    tx.execute(
        "INSERT INTO scan_changes
             (scan_id, base_id, relative_path, change_kind, field_mask,
              old_size_bytes, new_size_bytes, old_mtime_ns, new_mtime_ns,
              old_mime_type, new_mime_type, old_content_hash, new_content_hash)
         SELECT s.scan_id, s.base_id, s.relative_path, 'updated',
                (CASE WHEN f.size_bytes   <> s.size_bytes   THEN ?2 ELSE 0 END)
              + (CASE WHEN f.mtime_ns     <> s.mtime_ns     THEN ?3 ELSE 0 END)
              + (CASE WHEN f.mime_type IS NOT s.mime_type   THEN ?4 ELSE 0 END)
              + (CASE WHEN f.content_hash <> s.content_hash THEN ?5 ELSE 0 END),
                f.size_bytes, s.size_bytes, f.mtime_ns, s.mtime_ns,
                f.mime_type, s.mime_type, f.content_hash, s.content_hash
         FROM scan_stage_entries s
         JOIN files f ON f.base_id = s.base_id AND f.relative_path = s.relative_path
         WHERE s.scan_id = ?1 AND s.complete = 1
           AND f.presence IN ('present', 'unreadable')
           AND (f.size_bytes <> s.size_bytes OR f.mtime_ns <> s.mtime_ns
                OR f.mime_type IS NOT s.mime_type OR f.content_hash <> s.content_hash)",
        params![scan_id, F_SIZE, F_MTIME, F_MIME, F_HASH],
    )
    .map_err(AppError::db)
}

/// A path the walk could not read is not evidence of deletion, so it earns
/// neither a change row here nor a `deleted` presence in `mark_deleted`.
fn record_deleted(
    tx: &rusqlite::Transaction<'_>,
    scan_id: i64,
    base_id: i64,
) -> Result<usize, AppError> {
    tx.execute(
        &format!(
            "INSERT INTO scan_changes
                 (scan_id, base_id, relative_path, change_kind, field_mask,
                  old_size_bytes, new_size_bytes, old_mtime_ns, new_mtime_ns,
                  old_mime_type, new_mime_type, old_content_hash, new_content_hash)
             SELECT ?1, f.base_id, f.relative_path, 'deleted', ?3,
                    f.size_bytes, NULL, f.mtime_ns, NULL, f.mime_type, NULL,
                    f.content_hash, NULL
             FROM files f
             WHERE f.base_id = ?2 AND f.presence IN ('present', 'unreadable')
               AND NOT EXISTS (SELECT 1 FROM scan_stage_entries s
                               WHERE s.scan_id = ?1 AND s.relative_path = f.relative_path
                                 AND s.complete = 1)
               AND NOT {}",
            shielded("f")
        ),
        params![scan_id, base_id, F_PRESENCE],
    )
    .map_err(AppError::db)
}

/// Only added and updated paths are written. An unchanged path keeps the scan
/// id that actually supplied its state.
fn upsert_present(tx: &rusqlite::Transaction<'_>, scan_id: i64) -> Result<(), AppError> {
    tx.execute(
        "INSERT INTO files
             (base_id, relative_path, parent_path, name, extension, presence,
              size_bytes, mtime_ns, created_ns, mime_type, mime_source,
              hash_algorithm, content_hash, hash_status,
              metadata_from_scan_id, deleted_in_scan_id)
         SELECT s.base_id, s.relative_path, s.parent_path, s.name, s.extension, 'present',
                s.size_bytes, s.mtime_ns, s.created_ns, s.mime_type, s.mime_source,
                s.hash_algorithm, s.content_hash, s.hash_status, s.scan_id, NULL
         FROM scan_stage_entries s
         LEFT JOIN files f ON f.base_id = s.base_id AND f.relative_path = s.relative_path
         WHERE s.scan_id = ?1 AND s.complete = 1
           AND (f.file_id IS NULL OR f.presence IN ('deleted', 'unreadable')
                OR f.size_bytes <> s.size_bytes OR f.mtime_ns <> s.mtime_ns
                OR f.mime_type IS NOT s.mime_type OR f.content_hash <> s.content_hash)
         ON CONFLICT(base_id, relative_path) DO UPDATE SET
             parent_path = excluded.parent_path,
             name = excluded.name,
             extension = excluded.extension,
             presence = 'present',
             size_bytes = excluded.size_bytes,
             mtime_ns = excluded.mtime_ns,
             created_ns = excluded.created_ns,
             mime_type = excluded.mime_type,
             mime_source = excluded.mime_source,
             content_hash = excluded.content_hash,
             hash_status = excluded.hash_status,
             metadata_from_scan_id = excluded.metadata_from_scan_id,
             deleted_in_scan_id = NULL",
        params![scan_id],
    )
    .map_err(AppError::db)?;
    Ok(())
}

/// Unseen because unreadable, not because gone. These rows stay queryable as
/// their own presence so the user can fix the permission and rerun, or decide
/// the paths really are finished with. A later scan that can read the path
/// restores them silently: `upsert_present` sets `present` again, and no change
/// row is written unless the file itself actually changed.
fn mark_unreadable(
    tx: &rusqlite::Transaction<'_>,
    scan_id: i64,
    base_id: i64,
) -> Result<usize, AppError> {
    tx.execute(
        &format!(
            "UPDATE files SET presence = 'unreadable'
             WHERE base_id = ?2 AND presence IN ('present', 'unreadable')
               AND NOT EXISTS (SELECT 1 FROM scan_stage_entries s
                               WHERE s.scan_id = ?1 AND s.relative_path = files.relative_path
                                 AND s.complete = 1)
               AND {}",
            shielded("files")
        ),
        params![scan_id, base_id],
    )
    .map_err(AppError::db)
}

/// Every other absence really is gone.
fn mark_deleted(
    tx: &rusqlite::Transaction<'_>,
    scan_id: i64,
    base_id: i64,
) -> Result<(), AppError> {
    tx.execute(
        &format!(
            "UPDATE files SET presence = 'deleted', deleted_in_scan_id = ?1
             WHERE base_id = ?2 AND presence IN ('present', 'unreadable')
               AND NOT EXISTS (SELECT 1 FROM scan_stage_entries s
                               WHERE s.scan_id = ?1 AND s.relative_path = files.relative_path
                                 AND s.complete = 1)
               AND NOT {}",
            shielded("files")
        ),
        params![scan_id, base_id],
    )
    .map_err(AppError::db)?;
    Ok(())
}

/// Stamps the scan complete and moves the baseline, then drains staging. The
/// last three statements of the promotion transaction.
fn finalize_scan(
    tx: &rusqlite::Transaction<'_>,
    scan_id: i64,
    base_id: i64,
    p: &Promoted,
) -> Result<(), AppError> {
    tx.execute(
        "UPDATE scans SET status = 'complete', finished_at_ns = ?1,
             added_count = ?2, updated_count = ?3, deleted_count = ?4,
             unchanged_count = ?5, error_count = ?8, present_count = ?6
         WHERE scan_id = ?7",
        params![
            now_ns(),
            p.added as i64,
            p.updated as i64,
            p.deleted as i64,
            p.unchanged as i64,
            p.present,
            scan_id,
            p.errors as i64
        ],
    )
    .map_err(AppError::db)?;
    tx.execute(
        "UPDATE bases SET last_complete_scan_id = ?1, last_error = NULL WHERE base_id = ?2",
        params![scan_id, base_id],
    )
    .map_err(AppError::db)?;
    tx.execute(
        "DELETE FROM scan_stage_entries WHERE scan_id = ?1",
        params![scan_id],
    )
    .map_err(AppError::db)?;
    Ok(())
}

/// Count over total. The only total available before the walk finishes is the
/// previous scan's file count — an exact one would need a second full traversal
/// — so it is shown as an estimate and dropped entirely once exceeded, rather
/// than reporting a percentage above 100. A first scan has no prior count and
/// shows discovery alone.
struct Progress {
    prior_files: u64,
    last: Instant,
    active: bool,
    tty: bool,
}

impl Progress {
    fn new(prior_files: u64) -> Progress {
        Progress {
            prior_files,
            last: Instant::now(),
            active: false,
            // A redraw needs a terminal to redraw over; piped output gets none.
            tty: std::io::IsTerminal::is_terminal(&std::io::stderr()),
        }
    }

    fn tick(&mut self, discovered: u64, processed: u64, cancelled: &AtomicBool) {
        if !self.tty
            || cancelled.load(Ordering::Relaxed)
            || self.last.elapsed() < Duration::from_millis(250)
        {
            return;
        }
        self.last = Instant::now();
        self.active = true;
        let of_total = if self.prior_files > 0 && processed <= self.prior_files {
            format!(
                " / ~{} ({:.0}%)",
                self.prior_files,
                processed as f64 / self.prior_files as f64 * 100.0
            )
        } else {
            String::new()
        };
        eprint!("\r  discovered {discovered}  processed {processed}{of_total}          ");
    }

    fn finish(&mut self) {
        if self.active {
            eprint!("\r                                                                  \r");
        }
    }
}

/// Streams the file through the caller's buffer. The buffer belongs to the
/// worker and outlives every file it hashes.
fn hash_file(path: &Path, buf: &mut [u8]) -> std::io::Result<Vec<u8>> {
    use sha2::{Digest, Sha256};
    let mut file = std::fs::File::open(path)?;
    let mut hasher = Sha256::new();
    loop {
        let n = file.read(buf)?;
        if n == 0 {
            break;
        }
        hasher.update(&buf[..n]);
    }
    Ok(hasher.finalize().to_vec())
}

/// The one space rule. Predicting a scan's appetite needed a file count that
/// does not exist before the first walk, so nothing predicts any more: the floor
/// is checked before the scan and again after every committed batch. Stopping
/// mid-scan is cheap because staging is durable and the next `ingest` resumes.
fn free_space_floor(config: &Config) -> u64 {
    config.minimum_free_space_mib.saturating_mul(1024 * 1024)
}

fn free_bytes(db_path: &Path) -> Result<u64, AppError> {
    let dir = db_path.parent().filter(|p| !p.as_os_str().is_empty());
    let target = dir.unwrap_or(Path::new("."));
    let stat = rustix::fs::statvfs(target)
        .map_err(|e| AppError::io(format!("cannot stat filesystem at {}: {e}", target.display())))?;
    Ok(stat.f_bavail.saturating_mul(stat.f_frsize))
}
