//! History retention. `scan_changes` grows with change *events*, so a single
//! rename high in a large tree can write millions of rows that nothing ever
//! removes. This bounds that to the newest `history.keep_scans` complete scans
//! per base.
//!
//! Two prunes, sharing one cutoff: the change rows themselves, and the
//! permanently dead tail of `files` rows left behind by deletions. `scans`,
//! `scan_errors`, and `scan_stage_entries` are all kept — keeping `scans` is
//! what makes this safe, since `files.metadata_from_scan_id` on an unchanged
//! file still points at whatever ancient scan supplied it.

use rusqlite::{params, Connection, OptionalExtension};

use crate::config::Config;
use crate::AppError;

pub struct BasePrune {
    /// Only the command fills this in; the post-promote caller has no use for it.
    pub base_path: String,
    /// Oldest scan kept. History strictly before it is prunable. `None` when the
    /// base has fewer complete scans than the window, so nothing ages out yet.
    pub cutoff: Option<i64>,
    pub changes: u64,
    pub files: u64,
}

impl BasePrune {
    pub fn total(&self) -> u64 {
        self.changes + self.files
    }
}

/// Every base in the database. `apply = false` counts without deleting.
pub fn prune_all(
    conn: &Connection,
    config: &Config,
    apply: bool,
) -> Result<Vec<BasePrune>, AppError> {
    let mut stmt = conn
        .prepare("SELECT base_id, base_path FROM bases ORDER BY base_id")
        .map_err(AppError::db)?;
    let bases: Vec<(i64, String)> = stmt
        .query_map([], |r| Ok((r.get(0)?, r.get(1)?)))
        .map_err(AppError::db)?
        .collect::<Result<_, _>>()
        .map_err(AppError::db)?;
    drop(stmt);

    bases
        .into_iter()
        .map(|(base_id, base_path)| {
            prune_base(conn, config, base_id, apply).map(|p| BasePrune { base_path, ..p })
        })
        .collect()
}

/// One base. Called by the `history prune` command and again after every
/// promotion, so the counting and the deleting can never drift apart.
pub fn prune_base(
    conn: &Connection,
    config: &Config,
    base_id: i64,
    apply: bool,
) -> Result<BasePrune, AppError> {
    let mut out = BasePrune {
        base_path: String::new(),
        cutoff: None,
        changes: 0,
        files: 0,
    };
    if config.keep_scans == 0 {
        return Ok(out);
    }
    let Some(cutoff) = cutoff(conn, base_id, config.keep_scans)? else {
        return Ok(out);
    };
    out.cutoff = Some(cutoff);

    let change_where = "base_id = ?1 AND scan_id < ?2";
    // A `deleted` row is the only absence that is settled. `unreadable` is live
    // state — a path we could not read is not evidence that it is gone.
    let file_where = "base_id = ?1 AND presence = 'deleted' AND deleted_in_scan_id < ?2";

    if apply {
        let batch = config.writer_batch_rows;
        out.changes =
            delete_batched(conn, "scan_changes", "change_id", change_where, base_id, cutoff, batch)?;
        out.files = delete_batched(conn, "files", "file_id", file_where, base_id, cutoff, batch)?;
    } else {
        out.changes = count(conn, "scan_changes", change_where, base_id, cutoff)?;
        out.files = count(conn, "files", file_where, base_id, cutoff)?;
    }
    Ok(out)
}

/// The oldest scan the window keeps: the `keep_scans`-th newest *complete* scan.
/// Incomplete scans do not count toward the window — a run of failures must not
/// age out real history.
fn cutoff(conn: &Connection, base_id: i64, keep_scans: u64) -> Result<Option<i64>, AppError> {
    let oldest_kept: Option<i64> = conn
        .query_row(
            "SELECT scan_id FROM scans
             WHERE base_id = ?1 AND status = 'complete'
             ORDER BY scan_id DESC LIMIT 1 OFFSET ?2",
            params![base_id, (keep_scans - 1) as i64],
            |r| r.get(0),
        )
        .optional()
        .map_err(AppError::db)?;
    let Some(oldest_kept) = oldest_kept else {
        return Ok(None);
    };
    // Defensive: the live baseline's own history is never prunable, whatever the
    // window says. The newest complete scan is normally at or after the window
    // edge already, so this only bites if that invariant ever breaks.
    let baseline: Option<i64> = conn
        .query_row(
            "SELECT last_complete_scan_id FROM bases WHERE base_id = ?1",
            params![base_id],
            |r| r.get(0),
        )
        .optional()
        .map_err(AppError::db)?
        .flatten();
    Ok(Some(match baseline {
        Some(b) => oldest_kept.min(b),
        None => oldest_kept,
    }))
}

fn count(
    conn: &Connection,
    table: &str,
    predicate: &str,
    base_id: i64,
    cutoff: i64,
) -> Result<u64, AppError> {
    conn.query_row(
        &format!("SELECT COUNT(*) FROM {table} WHERE {predicate}"),
        params![base_id, cutoff],
        |r| r.get::<_, i64>(0).map(|n| n as u64),
    )
    .map_err(AppError::db)
}

/// Deleting millions of rows in one transaction builds a WAL the size of the
/// thing we are trying to shrink, which is the disk-filling problem this feature
/// exists to solve. So: bounded statements, each its own transaction.
///
/// `DELETE ... LIMIT` needs a compile-time option the system SQLite may not
/// have, hence the subquery.
fn delete_batched(
    conn: &Connection,
    table: &str,
    id_column: &str,
    predicate: &str,
    base_id: i64,
    cutoff: i64,
    batch: usize,
) -> Result<u64, AppError> {
    let sql = format!(
        "DELETE FROM {table} WHERE {id_column} IN
             (SELECT {id_column} FROM {table} WHERE {predicate} LIMIT ?3)"
    );
    let mut total = 0u64;
    loop {
        let n = conn
            .execute(&sql, params![base_id, cutoff, batch as i64])
            .map_err(AppError::db)?;
        total += n as u64;
        if n < batch {
            return Ok(total);
        }
    }
}
