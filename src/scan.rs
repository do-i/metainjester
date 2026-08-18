//! The scan lifecycle of design §7: preflight, staging, validate, promote.
//!
//! The pipeline is walker -> bounded queue -> N hash workers -> bounded queue ->
//! staging writer. Only the writer holds the write connection; workers open
//! their own read-only connections, which WAL allows to run during a scan.

use std::io::Read;
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::mpsc::{sync_channel, Receiver, SyncSender};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use rusqlite::{params, Connection, OptionalExtension};

use crate::config::Config;
use crate::db;
use crate::history;
use crate::walk::{self, Exclusions, WalkMsg, Walker};
use crate::{now_ns, AppError};

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
    Row(Box<StagedRow>),
    /// Already staged durably by an earlier attempt; nothing to write.
    Reused(Vec<u8>),
    Error {
        rel: Option<Vec<u8>>,
        stage: &'static str,
        code: &'static str,
        message: String,
    },
}

pub fn ingest(
    conn: &mut Connection,
    db_path: &Path,
    config: &Config,
    base: &Path,
    cancelled: &Arc<AtomicBool>,
) -> Result<Outcome, AppError> {
    let started = Instant::now();
    let base_str = base.to_string_lossy().to_string();

    // 1-2. Register or match the base, then validate the inclusion policy before
    // anything touches staging. A rescan under different rules could otherwise
    // mark a whole excluded subtree deleted.
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
    let (base_id, baseline, skip_hidden, skip_mount, follow_symlinks): (
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
            params![msg, base_id],
        )
        .map_err(AppError::db)?;
        return Err(AppError::policy_mismatch(msg));
    }

    // 3. Preflight. Refuse before a scan row exists.
    let prior_files: u64 = conn
        .query_row(
            "SELECT COUNT(*) FROM files WHERE base_id = ?1 AND presence = 'present'",
            params![base_id],
            |r| r.get::<_, i64>(0).map(|n| n as u64),
        )
        .map_err(AppError::db)?;
    let required_free = free_space_floor(config);
    let estimate_source = format!(
        "storage.minimum_free_space_mib = {}, rechecked every batch",
        config.minimum_free_space_mib
    );
    let free_before = free_bytes(db_path)?;
    if free_before < required_free {
        return Err(AppError::no_space(free_before, required_free));
    }

    // 4. Resume a non-terminal scan, or start a new one. A scan left `running`
    // is a crash; `cancelled`/`partial`/`failed` are ordinary stops. All are
    // durable completed work worth reusing.
    let resumable: Option<i64> = conn
        .query_row(
            "SELECT scan_id FROM scans
             WHERE base_id = ?1 AND status IN ('running','cancelled','failed','partial')
             ORDER BY scan_id DESC LIMIT 1",
            params![base_id],
            |r| r.get(0),
        )
        .optional()
        .map_err(AppError::db)?;

    let (scan_id, resumed) = match resumable {
        Some(id) => {
            conn.execute(
                "UPDATE scans SET status = 'running', failure_code = NULL,
                     failure_message = NULL WHERE scan_id = ?1",
                params![id],
            )
            .map_err(AppError::db)?;
            // Diagnostics from the attempt we are retrying describe a state that
            // no longer holds; keeping them would contradict this run's counts.
            conn.execute("DELETE FROM scan_errors WHERE scan_id = ?1", params![id])
                .map_err(AppError::db)?;
            (id, true)
        }
        None => {
            conn.execute(
                "DELETE FROM scan_stage_entries WHERE base_id = ?1",
                params![base_id],
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
                    base_id,
                    baseline,
                    now_ns(),
                    config.skip_hidden,
                    config.skip_mount_boundaries,
                    config.follow_symlinks,
                    config.workers as i64,
                    config.writer_batch_rows as i64,
                    config.throttle_ms_after_batch as i64,
                    config.hash_policy,
                    free_before as i64,
                    required_free as i64,
                    estimate_source
                ],
            )
            .map_err(AppError::db)?;
            (conn.last_insert_rowid(), false)
        }
    };

    conn.execute_batch(
        "CREATE TEMP TABLE IF NOT EXISTS walk_paths (relative_path BLOB PRIMARY KEY);
         DELETE FROM walk_paths;",
    )
    .map_err(AppError::db)?;

    let low_space = Arc::new(AtomicBool::new(false));
    let counts = Arc::new(Exclusions::default());
    let hashed = Arc::new(AtomicU64::new(0));
    let hashed_bytes = Arc::new(AtomicU64::new(0));
    let reused_stage = Arc::new(AtomicU64::new(0));
    let reused_baseline = Arc::new(AtomicU64::new(0));
    let changed_during_hash = Arc::new(AtomicU64::new(0));

    let pipeline = run_pipeline(
        conn,
        db_path,
        config,
        base,
        base_id,
        scan_id,
        prior_files,
        cancelled,
        &low_space,
        &counts,
        &hashed,
        &hashed_bytes,
        &reused_stage,
        &reused_baseline,
        &changed_during_hash,
    )?;

    let stopped = cancelled.load(Ordering::SeqCst);
    let errors = pipeline.errors;

    let (staged, staged_bytes): (usize, i64) = conn
        .query_row(
            "SELECT COUNT(*), IFNULL(SUM(size_bytes), 0) FROM scan_stage_entries
             WHERE scan_id = ?1 AND complete = 1",
            params![scan_id],
            |r| Ok((r.get::<_, i64>(0)? as usize, r.get(1)?)),
        )
        .map_err(AppError::db)?;

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
            hashed.load(Ordering::Relaxed) as i64,
            hashed_bytes.load(Ordering::Relaxed) as i64,
            changed_during_hash.load(Ordering::Relaxed) as i64,
            counts.hidden.load(Ordering::Relaxed) as i64,
            counts.mount.load(Ordering::Relaxed) as i64,
            counts.symlink.load(Ordering::Relaxed) as i64,
            errors as i64,
            scan_id,
            staged_bytes
        ],
    )
    .map_err(AppError::db)?;

    let mut outcome = Outcome {
        scan_id,
        resumed,
        status: "complete",
        added: 0,
        updated: 0,
        deleted: 0,
        unchanged: 0,
        present: 0,
        staged,
        hashed: hashed.load(Ordering::Relaxed),
        hashed_bytes: hashed_bytes.load(Ordering::Relaxed),
        reused_stage: reused_stage.load(Ordering::Relaxed),
        reused_baseline: reused_baseline.load(Ordering::Relaxed),
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
        free_before,
        required_free,
        estimate_source,
        duration_ms: 0,
    };

    // 7. File-level errors no longer block: a vanished file is simply gone, and
    // an unreadable directory is shielded at promotion rather than allowed to
    // record its contents as deleted. A cancelled scan still keeps its staging
    // and leaves the baseline exactly as it was, and so does an unreadable base
    // — that one failure puts every path in the baseline in doubt at once, and
    // no prefix can shield them because the base prefixes everything.
    outcome.unreadable_paths = unreadable_path_count(conn, scan_id)?;
    let base_unreadable = base_unreadable(conn, scan_id)?;
    outcome.low_space = low_space.load(Ordering::SeqCst);
    outcome.base_unreadable = base_unreadable;
    if stopped || base_unreadable {
        let (status, code) = if outcome.low_space {
            ("partial", "low_space")
        } else if stopped {
            ("cancelled", "cancelled")
        } else {
            ("partial", "base_unreadable")
        };
        outcome.status = status;
        outcome.duration_ms = started.elapsed().as_millis() as u64;
        conn.execute(
            "UPDATE scans SET status = ?1, finished_at_ns = ?2, duration_ms = ?3,
                 failure_code = ?4, failure_message = ?5, free_bytes_after = ?6
             WHERE scan_id = ?7",
            params![
                status,
                now_ns(),
                outcome.duration_ms as i64,
                code,
                if outcome.low_space {
                    format!(
                        "stopped at the {} MiB free-space floor; \
                         free space and rerun to resume",
                        config.minimum_free_space_mib
                    )
                } else if base_unreadable {
                    "base directory is unreadable; baseline left unchanged".to_string()
                } else {
                    format!("{errors} error(s); baseline left unchanged")
                },
                free_bytes(db_path).unwrap_or(0) as i64,
                scan_id
            ],
        )
        .map_err(AppError::db)?;
        return Ok(outcome);
    }

    // Every attempt re-walks, so a path staged earlier but absent now must not
    // survive into promotion as present.
    conn.execute(
        "DELETE FROM scan_stage_entries WHERE scan_id = ?1
         AND relative_path NOT IN (SELECT relative_path FROM walk_paths)",
        params![scan_id],
    )
    .map_err(AppError::db)?;
    // Same row scan as before, now also summing, so `scans.staged_files` can be
    // corrected from it. The counts written before the cleanup are the truth for
    // a scan that stops early — its staging is kept exactly as it stands — but a
    // scan that reaches here has just dropped rows the re-walk did not see, and
    // the stored columns would otherwise keep claiming them.
    let (staged_after, staged_bytes_after): (usize, i64) = conn
        .query_row(
            "SELECT COUNT(*), IFNULL(SUM(size_bytes), 0) FROM scan_stage_entries
             WHERE scan_id = ?1 AND complete = 1",
            params![scan_id],
            |r| Ok((r.get::<_, i64>(0)? as usize, r.get(1)?)),
        )
        .map_err(AppError::db)?;
    outcome.staged = staged_after;

    promote(conn, scan_id, base_id, &mut outcome)?;

    // Retention runs after promotion and outside its transaction. Promotion is
    // the one atomic step that moves the baseline; folding a large delete into
    // it would stretch the window where a crash discards the whole scan. A
    // failure here must not undo a scan that already succeeded, so it is
    // reported and swallowed.
    if config.keep_scans > 0 {
        match history::prune_base(conn, config, base_id, true) {
            Ok(p) => {
                outcome.pruned_changes = p.changes;
                outcome.pruned_files = p.files;
            }
            Err(e) => eprintln!("metainjester: history prune failed: {}", e.message),
        }
    }

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

struct PipelineResult {
    errors: usize,
}

#[allow(clippy::too_many_arguments)]
fn run_pipeline(
    conn: &mut Connection,
    db_path: &Path,
    config: &Config,
    base: &Path,
    base_id: i64,
    scan_id: i64,
    prior_files: u64,
    cancelled: &Arc<AtomicBool>,
    low_space: &Arc<AtomicBool>,
    counts: &Arc<Exclusions>,
    hashed: &Arc<AtomicU64>,
    hashed_bytes: &Arc<AtomicU64>,
    reused_stage: &Arc<AtomicU64>,
    reused_baseline: &Arc<AtomicU64>,
    changed_during_hash: &Arc<AtomicU64>,
) -> Result<PipelineResult, AppError> {
    let (path_tx, path_rx) = sync_channel::<WalkMsg>(config.queue_items);
    let (row_tx, row_rx) = sync_channel::<WriteMsg>(config.queue_items);
    let path_rx = Arc::new(Mutex::new(path_rx));
    let mut errors = 0usize;

    let result = std::thread::scope(|scope| -> Result<usize, AppError> {
        // Walker.
        {
            let counts = counts.clone();
            let cancelled = cancelled.clone();
            let tx = path_tx.clone();
            // `Walker::new` consumes the sender, so reporting its failure needs
            // a second clone.
            let err_tx = path_tx.clone();
            scope.spawn(move || match Walker::new(base, config, tx, &cancelled, counts) {
                Ok(mut w) => w.run(),
                // A base we cannot stat is a base we cannot read, and it
                // prefixes every path in the baseline. This must travel the
                // same channel as a failed `read_dir` on the base — an error
                // row on the empty relative path — because that row is the only
                // thing `base_unreadable` looks for when it decides whether
                // promotion may proceed. Printing the error instead would leave
                // a scan that looks like a successful walk of an empty tree,
                // and promotion would then record the entire baseline deleted.
                Err(e) => {
                    let _ = err_tx.send(WalkMsg::Error {
                        rel: Some(Vec::new()),
                        code: walk::error_code(&e),
                        message: format!("cannot stat base: {e}"),
                    });
                }
            });
        }
        drop(path_tx);

        // Hash workers.
        for _ in 0..config.workers {
            let rx = path_rx.clone();
            let tx = row_tx.clone();
            let cancelled = cancelled.clone();
            let (hashed, hashed_bytes) = (hashed.clone(), hashed_bytes.clone());
            let (reused_stage, reused_baseline) =
                (reused_stage.clone(), reused_baseline.clone());
            let changed_during_hash = changed_during_hash.clone();
            let reader = db::open_reader(db_path)?;
            // A worker that cannot prepare its lookups used to return quietly.
            // If every worker did that — WAL shared memory unavailable, say —
            // the scan staged nothing and recorded no error, which is
            // indistinguishable from a walk of an empty tree, and promotion
            // recorded the whole baseline as deleted. Prove them here, where
            // the failure is still a returnable error and no scan is promoted.
            reader.prepare(STAGED_LOOKUP_SQL).map_err(AppError::db)?;
            reader.prepare(BASELINE_LOOKUP_SQL).map_err(AppError::db)?;
            scope.spawn(move || {
                worker(
                    reader,
                    scan_id,
                    base_id,
                    &rx,
                    &tx,
                    &cancelled,
                    &hashed,
                    &hashed_bytes,
                    &reused_stage,
                    &reused_baseline,
                    &changed_during_hash,
                );
            });
        }
        drop(row_tx);
        // The workers are now the only receiver holders. Keeping a clone here
        // would leave the walker blocked in a full-queue send after a cancel,
        // because send only fails once every receiver is gone.
        drop(path_rx);

        // A writer that gives up has to say so. Its receiver dropping is not a
        // signal anyone upstream acts on, so without this the walker and the
        // hash workers keep going and `thread::scope` cannot return until they
        // have chewed through the whole tree for output nothing will read.
        let result = writer(
            conn,
            db_path,
            config,
            scan_id,
            base_id,
            prior_files,
            row_rx,
            counts,
            cancelled,
            low_space,
        );
        if result.is_err() {
            cancelled.store(true, Ordering::SeqCst);
        }
        result
    })?;
    errors += result;
    Ok(PipelineResult { errors })
}

/// One hash worker. Every send is checked: a failed send means the writer is
/// gone, and continuing past that hashes the rest of the tree into a dead
/// channel while the scope waits for it.
#[allow(clippy::too_many_arguments)]
fn worker(
    reader: Connection,
    scan_id: i64,
    base_id: i64,
    rx: &Arc<Mutex<Receiver<WalkMsg>>>,
    tx: &SyncSender<WriteMsg>,
    cancelled: &AtomicBool,
    hashed: &AtomicU64,
    hashed_bytes: &AtomicU64,
    reused_stage: &AtomicU64,
    reused_baseline: &AtomicU64,
    changed_during_hash: &AtomicU64,
) {
    // The pipeline already proved these prepare, so reaching the failure arm is
    // a race with something changing underneath us. It still must not be quiet:
    // a worker that returns without a word leaves a walk that looks empty.
    let mut staged_lookup = match reader.prepare(STAGED_LOOKUP_SQL) {
        Ok(s) => s,
        Err(e) => return worker_died(cancelled, tx, e),
    };
    let mut baseline_lookup = match reader.prepare(BASELINE_LOOKUP_SQL) {
        Ok(s) => s,
        Err(e) => return worker_died(cancelled, tx, e),
    };

    loop {
        if cancelled.load(Ordering::Relaxed) {
            return;
        }
        let msg = {
            let guard = match rx.lock() {
                Ok(g) => g,
                Err(_) => return,
            };
            guard.recv()
        };
        let Ok(msg) = msg else { return };

        let item = match msg {
            WalkMsg::Error {
                rel,
                code,
                message,
            } => {
                if tx
                    .send(WriteMsg::Error {
                        rel,
                        stage: "walk",
                        code,
                        message,
                    })
                    .is_err()
                {
                    return;
                }
                continue;
            }
            WalkMsg::Item(item) => item,
        };

        // Reuse order: a durable staged row from this scan, then the baseline
        // hash. Both require size and mtime to match exactly.
        let staged_hit = staged_lookup
            .query_row(params![scan_id, &item.rel], |r| {
                Ok((r.get::<_, i64>(0)?, r.get::<_, i64>(1)?))
            })
            .optional()
            .ok()
            .flatten();
        if let Some((s, m)) = staged_hit
            && s == item.size_bytes
            && m == item.mtime_ns
        {
            reused_stage.fetch_add(1, Ordering::Relaxed);
            if tx.send(WriteMsg::Reused(item.rel.clone())).is_err() {
                return;
            }
            continue;
        }

        let prior = baseline_lookup
            .query_row(params![base_id, &item.rel], |r| {
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
                reused_baseline.fetch_add(1, Ordering::Relaxed);
                (h, "complete")
            }
            _ => match hash_file(&item.path) {
                Ok(h) => {
                    hashed.fetch_add(1, Ordering::Relaxed);
                    hashed_bytes.fetch_add(item.size_bytes.max(0) as u64, Ordering::Relaxed);
                    // A file rewritten while we streamed it produces a digest of
                    // no particular version, so say so rather than store it.
                    match std::fs::metadata(&item.path) {
                        Ok(after)
                            if after.len() as i64 != item.size_bytes
                                || walk::system_time_ns(after.modified().ok()).unwrap_or(0)
                                    != item.mtime_ns =>
                        {
                            changed_during_hash.fetch_add(1, Ordering::Relaxed);
                            if tx
                                .send(WriteMsg::Error {
                                    rel: Some(item.rel.clone()),
                                    stage: "hash",
                                    code: "changed_during_hash",
                                    message: "file changed while being hashed".into(),
                                })
                                .is_err()
                            {
                                return;
                            }
                            (h, "changed_during_hash")
                        }
                        _ => (h, "complete"),
                    }
                }
                Err(e) => {
                    if tx
                        .send(WriteMsg::Error {
                            rel: Some(item.rel.clone()),
                            stage: "hash",
                            code: walk::error_code(&e),
                            message: e.to_string(),
                        })
                        .is_err()
                    {
                        return;
                    }
                    continue;
                }
            },
        };

        if tx
            .send(WriteMsg::Row(Box::new(StagedRow {
                rel: item.rel.clone(),
                size_bytes: item.size_bytes,
                mtime_ns: item.mtime_ns,
                created_ns: item.created_ns,
                content_hash,
                hash_status,
            })))
            .is_err()
        {
            return;
        }
    }
}

/// A hash worker that cannot start. Cancelling is what keeps the baseline safe:
/// it stops the scan short of promotion and keeps the staging for a resume,
/// rather than letting an empty-looking walk speak for the whole tree.
fn worker_died(cancelled: &AtomicBool, tx: &SyncSender<WriteMsg>, e: rusqlite::Error) {
    cancelled.store(true, Ordering::SeqCst);
    let _ = tx.send(WriteMsg::Error {
        rel: None,
        stage: "worker",
        code: "io_error",
        message: format!("hash worker could not start: {e}"),
    });
}

/// The single staging writer. Commits in batches so an interrupted scan keeps
/// everything already durable, and throttles only between batches.
#[allow(clippy::too_many_arguments)]
fn writer(
    conn: &mut Connection,
    db_path: &Path,
    config: &Config,
    scan_id: i64,
    base_id: i64,
    prior_files: u64,
    rx: Receiver<WriteMsg>,
    counts: &Arc<Exclusions>,
    cancelled: &AtomicBool,
    low_space: &AtomicBool,
) -> Result<usize, AppError> {
    let mut errors = 0usize;
    let mut processed = 0u64;
    let mut batch: Vec<WriteMsg> = Vec::with_capacity(config.writer_batch_rows);
    let mut progress = Progress::new(prior_files);

    while let Ok(first) = rx.recv() {
        batch.push(first);
        while batch.len() < config.writer_batch_rows {
            match rx.try_recv() {
                Ok(msg) => batch.push(msg),
                Err(_) => break,
            }
        }
        // Errors are not files, so they do not count toward a total drawn from
        // a previous scan's file count.
        let files = batch
            .iter()
            .filter(|m| !matches!(m, WriteMsg::Error { .. }))
            .count() as u64;
        errors += flush(conn, scan_id, base_id, &mut batch)?;
        processed += files;
        // The batch just committed is the last thing written before the floor is
        // rechecked, so the scan stops with room still left rather than after
        // taking it. Cancelling is how the walker and the hash workers hear
        // about it; `low_space` is what tells them apart from a Ctrl-C.
        if free_bytes(db_path)? < free_space_floor(config) {
            low_space.store(true, Ordering::SeqCst);
            cancelled.store(true, Ordering::SeqCst);
            break;
        }
        progress.tick(
            counts.discovered_files.load(Ordering::Relaxed),
            processed,
            cancelled,
        );
        if config.throttle_ms_after_batch > 0 {
            std::thread::sleep(Duration::from_millis(config.throttle_ms_after_batch));
        }
    }
    errors += flush(conn, scan_id, base_id, &mut batch)?;
    progress.finish();
    Ok(errors)
}

fn flush(
    conn: &mut Connection,
    scan_id: i64,
    base_id: i64,
    batch: &mut Vec<WriteMsg>,
) -> Result<usize, AppError> {
    if batch.is_empty() {
        return Ok(0);
    }
    let mut errors = 0usize;
    let tx = conn.transaction().map_err(AppError::db)?;
    {
        let mut stage = tx
            .prepare_cached(
                "INSERT INTO scan_stage_entries
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
                     complete = 1",
            )
            .map_err(AppError::db)?;
        let mut seen = tx
            .prepare_cached("INSERT OR IGNORE INTO walk_paths (relative_path) VALUES (?1)")
            .map_err(AppError::db)?;
        let mut err_stmt = tx
            .prepare_cached(
                "INSERT INTO scan_errors (scan_id, relative_path, stage, error_code, message)
                 VALUES (?1, ?2, ?3, ?4, ?5)",
            )
            .map_err(AppError::db)?;

        for msg in batch.drain(..) {
            match msg {
                WriteMsg::Row(row) => {
                    let (parent, name, ext) = walk::helpers(&row.rel);
                    let (mime, mime_source) = walk::mime_of(&row.rel);
                    stage
                        .execute(params![
                            scan_id,
                            base_id,
                            &row.rel,
                            parent,
                            name,
                            ext,
                            row.size_bytes,
                            row.mtime_ns,
                            row.created_ns,
                            mime,
                            mime_source,
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
                        .execute(params![scan_id, rel, st, code, truncate(&message)])
                        .map_err(AppError::db)?;
                }
            }
        }
    }
    tx.commit().map_err(AppError::db)?;
    Ok(errors)
}

fn truncate(s: &str) -> String {
    const MAX: usize = 300;
    if s.len() <= MAX {
        return s.to_string();
    }
    let mut end = MAX;
    while end > 0 && !s.is_char_boundary(end) {
        end -= 1;
    }
    format!("{}…", &s[..end])
}

/// How many distinct paths the walk could not read. The summary reports only
/// this count; `scan_errors` holds which ones, for the query the user runs.
fn unreadable_path_count(conn: &Connection, scan_id: i64) -> Result<usize, AppError> {
    let codes = walk::UNREADABLE_CODES
        .iter()
        .map(|c| format!("'{c}'"))
        .collect::<Vec<_>>()
        .join(", ");
    conn.query_row(
        &format!(
            "SELECT COUNT(DISTINCT relative_path) FROM scan_errors
             WHERE scan_id = ?1 AND error_code IN ({codes})"
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
    let codes = walk::UNREADABLE_CODES
        .iter()
        .map(|c| format!("'{c}'"))
        .collect::<Vec<_>>()
        .join(", ");
    format!(
        "EXISTS (SELECT 1 FROM scan_errors e
                 WHERE e.scan_id = ?1 AND e.relative_path IS NOT NULL
                   AND length(e.relative_path) > 0
                   AND e.error_code IN ({codes})
                   AND ({alias}.relative_path = e.relative_path
                        OR (substr({alias}.relative_path, 1, length(e.relative_path))
                              = e.relative_path
                            AND substr({alias}.relative_path,
                                       length(e.relative_path) + 1, 1) = x'2f')))"
    )
}

/// Promotion (design §7 step 8), in one transaction. Change rows are written
/// first because they read the outgoing `files` state.
fn promote(
    conn: &mut Connection,
    scan_id: i64,
    base_id: i64,
    outcome: &mut Outcome,
) -> Result<(), AppError> {
    let tx = conn.transaction().map_err(AppError::db)?;

    let added = tx
        .execute(
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
        .map_err(AppError::db)?;

    let updated = tx
        .execute(
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
        .map_err(AppError::db)?;

    // A path the walk could not read is not evidence of deletion, so it earns
    // neither a change row here nor a `deleted` presence below.
    let deleted = tx
        .execute(
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
        .map_err(AppError::db)?;

    // Only added and updated paths are written. An unchanged path keeps the scan
    // id that actually supplied its state.
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

    // Unseen because unreadable, not because gone. These rows stay queryable as
    // their own presence so the user can fix the permission and rerun, or decide
    // the paths really are finished with. A later scan that can read the path
    // restores them silently: the upsert above sets `present` again, and no
    // change row is written unless the file itself actually changed.
    let unreadable = tx
        .execute(
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
        .map_err(AppError::db)?;

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

    let present: i64 = tx
        .query_row(
            "SELECT COUNT(*) FROM files WHERE base_id = ?1 AND presence = 'present'",
            params![base_id],
            |r| r.get(0),
        )
        .map_err(AppError::db)?;

    let unchanged = outcome.staged.saturating_sub(added + updated);
    tx.execute(
        "UPDATE scans SET status = 'complete', finished_at_ns = ?1,
             added_count = ?2, updated_count = ?3, deleted_count = ?4,
             unchanged_count = ?5, error_count = ?8, present_count = ?6
         WHERE scan_id = ?7",
        params![
            now_ns(),
            added as i64,
            updated as i64,
            deleted as i64,
            unchanged as i64,
            present,
            scan_id,
            outcome.errors as i64
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
    tx.commit().map_err(AppError::db)?;

    outcome.added = added;
    outcome.updated = updated;
    outcome.deleted = deleted;
    outcome.unchanged = unchanged;
    outcome.present = present as usize;
    outcome.unreadable = unreadable;
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

fn hash_file(path: &Path) -> std::io::Result<Vec<u8>> {
    use sha2::{Digest, Sha256};
    let mut file = std::fs::File::open(path)?;
    let mut hasher = Sha256::new();
    let mut buf = vec![0u8; HASH_BUF];
    loop {
        let n = file.read(&mut buf)?;
        if n == 0 {
            break;
        }
        hasher.update(&buf[..n]);
    }
    Ok(hasher.finalize().to_vec())
}

/// Design §10: ~4 KiB per expected file, floored at the configured minimum. A
/// brand-new base has no file count to work from, so only the floor applies.
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
