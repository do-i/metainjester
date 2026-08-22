//! Black-box tests: each one builds a tree, runs the real binary against a
//! database of its own, and then reads that database back. Nothing reaches into
//! the crate, so these pin behaviour rather than implementation.
//!
//! Every sandbox gets its own `HOME`, so the configuration under test is the one
//! written here and never the developer's.

use std::ffi::OsStr;
use std::os::unix::ffi::OsStrExt;
use std::path::{Path, PathBuf};
use std::process::Command;

const BIN: &str = env!("CARGO_BIN_EXE_metainjester");

const EXIT_OK: i32 = 0;
const EXIT_CONFIG: i32 = 3;
const EXIT_POLICY: i32 = 6;
const EXIT_INCOMPLETE: i32 = 7;

struct Sandbox {
    root: PathBuf,
}

struct Run {
    code: i32,
    out: String,
}

impl Run {
    /// The value after `key` anywhere in the report, e.g. `files   12` or the
    /// `added 3  updated 0` line. Takes the first occurrence that is followed by
    /// a number, which keeps prose like "not deleted (exit 7)" from matching.
    fn num(&self, key: &str) -> i64 {
        let toks: Vec<&str> = self.out.split_whitespace().collect();
        toks.windows(2)
            .find(|w| w[0] == key && w[1].parse::<i64>().is_ok())
            .map(|w| w[1].parse().unwrap())
            .unwrap_or_else(|| panic!("no numeric `{key}` in:\n{}", self.out))
    }

    fn has(&self, needle: &str) -> bool {
        self.out.contains(needle)
    }

    fn ok(&self) -> &Run {
        assert_eq!(self.code, EXIT_OK, "expected success, got:\n{}", self.out);
        self
    }
}

impl Sandbox {
    fn new(name: &str) -> Sandbox {
        let root = std::env::temp_dir().join(format!("metainjester-it-{name}"));
        let _ = restore_all(&root);
        let _ = std::fs::remove_dir_all(&root);
        std::fs::create_dir_all(root.join("home/.config/metainjester")).unwrap();
        std::fs::create_dir_all(root.join("work")).unwrap();
        std::fs::create_dir_all(root.join("tree")).unwrap();
        // `db::discover` adopts an empty file in the working directory, which is
        // what keeps each test off the shared database.
        std::fs::write(root.join("work/metainjester.sqlite3"), b"").unwrap();
        let s = Sandbox { root };
        s.config("");
        s
    }

    fn tree(&self) -> PathBuf {
        self.root.join("tree")
    }
    fn work(&self) -> PathBuf {
        self.root.join("work")
    }
    fn home(&self) -> PathBuf {
        self.root.join("home")
    }
    fn db_path(&self) -> PathBuf {
        self.work().join("metainjester.sqlite3")
    }

    /// One worker so timings are about the tree, not the pacing knobs. Nothing
    /// else is set: a duplicate key is a TOML error, so callers own the rest.
    /// The throttle already defaults to 0.
    fn config(&self, extra: &str) {
        let body = format!("[scan]\nworkers = 1\n{extra}");
        std::fs::write(
            self.home().join(".config/metainjester/metainjester.toml"),
            body,
        )
        .unwrap();
    }

    fn write(&self, rel: &str, body: &str) {
        let p = self.tree().join(rel);
        std::fs::create_dir_all(p.parent().unwrap()).unwrap();
        std::fs::write(p, body).unwrap();
    }

    /// A name that is not valid UTF-8, which `relative_path` must carry through
    /// as bytes rather than lossily decoding.
    fn write_raw(&self, dir: &str, raw_name: &[u8], body: &str) -> PathBuf {
        let d = self.tree().join(dir);
        std::fs::create_dir_all(&d).unwrap();
        let p = d.join(OsStr::from_bytes(raw_name));
        std::fs::write(&p, body).unwrap();
        p
    }

    fn cmd(&self) -> Command {
        let mut c = Command::new(BIN);
        c.current_dir(self.work()).env("HOME", self.home());
        c
    }

    fn ingest(&self) -> Run {
        self.run(self.cmd().arg("ingest").arg(self.tree()))
    }

    fn ingest_path(&self, p: &Path) -> Run {
        self.run(self.cmd().arg("ingest").arg(p))
    }

    fn status(&self) -> Run {
        self.run(self.cmd().arg("status"))
    }

    /// Runs under a descriptor limit. `sh` is the portable way to set one for a
    /// child; the sandbox path has no shell metacharacters in it.
    fn ingest_with_fd_limit(&self, n: u32) -> Run {
        let script = format!(
            "ulimit -n {n}; exec '{}' ingest '{}'",
            BIN,
            self.tree().display()
        );
        let mut c = Command::new("sh");
        c.arg("-c")
            .arg(script)
            .current_dir(self.work())
            .env("HOME", self.home());
        self.run(&mut c)
    }

    fn run(&self, c: &mut Command) -> Run {
        let o = c.output().expect("failed to run metainjester");
        let mut out = String::from_utf8_lossy(&o.stdout).into_owned();
        out.push_str(&String::from_utf8_lossy(&o.stderr));
        Run {
            code: o.status.code().unwrap_or(-1),
            out,
        }
    }

    fn conn(&self) -> rusqlite::Connection {
        rusqlite::Connection::open(self.db_path()).unwrap()
    }

    fn scalar(&self, sql: &str) -> i64 {
        self.conn().query_row(sql, [], |r| r.get(0)).unwrap()
    }

    /// `None` when the table does not exist, which is itself the expected state
    /// for a run that refused before it initialised anything.
    fn scalar_opt(&self, sql: &str) -> Option<i64> {
        self.conn().query_row(sql, [], |r| r.get(0)).ok()
    }

    /// Looked up by raw bytes, which is the only lookup that is correct for a
    /// BINARY column.
    fn presence(&self, rel: &[u8]) -> Option<String> {
        self.conn()
            .query_row(
                "SELECT presence FROM files WHERE relative_path = ?1",
                rusqlite::params![rel.to_vec()],
                |r| r.get::<_, String>(0),
            )
            .ok()
    }
}

/// chmod 000 does not stop root, so those tests report whether they can run.
fn denies_reads(p: &Path) -> bool {
    std::fs::read_dir(p).is_err()
}

fn chmod(p: &Path, mode: u32) {
    use std::os::unix::fs::PermissionsExt;
    std::fs::set_permissions(p, std::fs::Permissions::from_mode(mode)).unwrap();
}

/// A locked directory left behind would defeat the next run's cleanup.
fn restore_all(root: &Path) -> std::io::Result<()> {
    for e in std::fs::read_dir(root)?.flatten() {
        let p = e.path();
        if p.is_dir() {
            chmod(&p, 0o755);
            let _ = restore_all(&p);
        }
    }
    Ok(())
}

// ---------------------------------------------------------------- baseline --

#[test]
fn fresh_scan_catalogues_every_file() {
    let s = Sandbox::new("fresh");
    s.write("a.txt", "a");
    s.write("sub/b.txt", "bb");
    s.write("sub/deeper/c.txt", "ccc");

    let r = s.ingest();
    r.ok();
    assert_eq!(r.num("files"), 3);
    assert_eq!(r.num("added"), 3);
    assert_eq!(r.num("present"), 3);
    assert_eq!(r.num("errors"), 0);
    assert_eq!(s.scalar("SELECT COUNT(*) FROM files WHERE presence = 'present'"), 3);
    assert_eq!(s.scalar("SELECT COUNT(*) FROM scans WHERE status = 'complete'"), 1);
}

#[test]
fn unchanged_rerun_hashes_nothing() {
    let s = Sandbox::new("rerun");
    s.write("a.txt", "a");
    s.write("sub/b.txt", "bb");
    s.ingest().ok();

    let r = s.ingest();
    r.ok();
    assert_eq!(r.num("hashed"), 0, "an unchanged rerun must not rehash");
    assert_eq!(r.num("unchanged"), 2);
    assert_eq!(r.num("added"), 0);
    assert_eq!(r.num("updated"), 0);
    // and it must not manufacture history
    assert_eq!(s.scalar("SELECT COUNT(*) FROM scan_changes WHERE scan_id = 2"), 0);
}

#[test]
fn edited_file_is_an_update_not_a_rename() {
    let s = Sandbox::new("edit");
    s.write("a.txt", "one");
    s.ingest().ok();
    s.write("a.txt", "two-different-length");

    let r = s.ingest();
    r.ok();
    assert_eq!(r.num("updated"), 1);
    assert_eq!(r.num("added"), 0);
    assert_eq!(r.num("deleted"), 0);
}

#[test]
fn rename_is_delete_plus_add() {
    let s = Sandbox::new("rename");
    s.write("before.txt", "same bytes");
    s.ingest().ok();
    std::fs::rename(s.tree().join("before.txt"), s.tree().join("after.txt")).unwrap();

    let r = s.ingest();
    r.ok();
    // A content-hash match is never treated as proof of a move.
    assert_eq!(r.num("added"), 1);
    assert_eq!(r.num("deleted"), 1);
    assert_eq!(s.presence(b"before.txt").as_deref(), Some("deleted"));
    assert_eq!(s.presence(b"after.txt").as_deref(), Some("present"));
}

#[test]
fn removed_file_is_recorded_deleted() {
    let s = Sandbox::new("delete");
    s.write("keep.txt", "k");
    s.write("gone.txt", "g");
    s.ingest().ok();
    std::fs::remove_file(s.tree().join("gone.txt")).unwrap();

    let r = s.ingest();
    r.ok();
    assert_eq!(r.num("deleted"), 1);
    assert_eq!(s.presence(b"gone.txt").as_deref(), Some("deleted"));
    assert_eq!(s.presence(b"keep.txt").as_deref(), Some("present"));
}

// --------------------------------------------------------------- policies --

#[test]
fn hidden_entries_are_excluded_by_count_only() {
    let s = Sandbox::new("hidden");
    s.write("visible.txt", "v");
    s.write(".hidden.txt", "h");
    s.write(".hiddendir/inside.txt", "i");

    let r = s.ingest();
    r.ok();
    assert_eq!(r.num("files"), 1);
    // The hidden directory is counted once and never descended into.
    assert!(r.has("2 hidden"), "got:\n{}", r.out);
    assert_eq!(s.scalar("SELECT COUNT(*) FROM files"), 1);
}

#[test]
fn symlinks_are_excluded_unless_followed() {
    let s = Sandbox::new("symlink");
    s.write("real.txt", "r");
    std::os::unix::fs::symlink("real.txt", s.tree().join("link.txt")).unwrap();

    let r = s.ingest();
    r.ok();
    assert_eq!(r.num("files"), 1, "a symlink is not a regular file");
    assert!(r.has("1 symlink"), "got:\n{}", r.out);

    // Following it makes the same path eligible, under a fresh base.
    let s2 = Sandbox::new("symlink-follow");
    s2.config("follow_symlinks = true\n");
    s2.write("real.txt", "r");
    std::os::unix::fs::symlink("real.txt", s2.tree().join("link.txt")).unwrap();
    let r2 = s2.ingest();
    r2.ok();
    assert_eq!(r2.num("files"), 2);
}

#[test]
fn symlink_loop_terminates() {
    let s = Sandbox::new("loop");
    s.config("follow_symlinks = true\n");
    s.write("a/b/c/deep.txt", "d");
    s.write("a/top.txt", "t");
    // c -> a, so a naive walk would descend for ever.
    std::os::unix::fs::symlink("../..", s.tree().join("a/b/c/back")).unwrap();

    let r = s.ingest();
    r.ok();
    assert_eq!(r.num("files"), 2, "each real file seen exactly once");
}

#[test]
fn changing_inclusion_policy_is_refused() {
    let s = Sandbox::new("policy");
    s.write("visible.txt", "v");
    s.write(".hidden.txt", "h");
    s.ingest().ok();

    // Under the new policy every hidden path would look deleted, so the rescan
    // must refuse rather than record that.
    s.config("skip_hidden = false\n");
    let r = s.ingest();
    assert_eq!(r.code, EXIT_POLICY, "got:\n{}", r.out);
    assert!(r.has("policy_mismatch"));
    assert_eq!(s.scalar("SELECT COUNT(*) FROM files WHERE presence = 'deleted'"), 0);
}

// ------------------------------------------------------------ descriptors --

/// The regression guard for the walker holding one directory handle per level.
/// Before the fix this lost every file below roughly the descriptor limit while
/// still reporting `complete`.
#[test]
fn deep_tree_survives_a_low_descriptor_limit() {
    let s = Sandbox::new("deep");
    let depth = 600;
    let mut p = s.tree().join("t");
    std::fs::create_dir_all(&p).unwrap();
    for _ in 0..depth {
        p = p.join("d");
        std::fs::create_dir(&p).unwrap();
        std::fs::write(p.join("f.txt"), "x").unwrap();
    }

    let r = s.ingest_with_fd_limit(64);
    r.ok();
    assert_eq!(r.num("files"), depth, "descriptors must not bound tree depth");
    assert_eq!(r.num("errors"), 0);
    assert_eq!(s.scalar("SELECT unreadable_dirs FROM scans WHERE scan_id = 1"), 0);
}

// ---------------------------------------------------------------- shields --

#[test]
fn unreadable_directory_shields_its_rows_and_flags_the_gap() {
    let s = Sandbox::new("shield");
    s.write("open/a.txt", "a");
    s.write("locked/b.txt", "b");
    s.ingest().ok();

    let locked = s.tree().join("locked");
    chmod(&locked, 0o000);
    if !denies_reads(&locked) {
        chmod(&locked, 0o755);
        eprintln!("skipping: running as root, chmod 000 does not deny reads");
        return;
    }

    let r = s.ingest();
    // Promoted — one unreadable directory must not hold the whole scan hostage —
    // but no longer silently, and no longer exit 0.
    assert_eq!(r.code, EXIT_INCOMPLETE, "got:\n{}", r.out);
    assert!(r.has("NOT catalogued"), "the gap must be stated:\n{}", r.out);
    assert_eq!(s.scalar("SELECT unreadable_dirs FROM scans WHERE scan_id = 2"), 1);

    // Absence under an unreadable path is not proof of deletion.
    assert_eq!(s.presence(b"locked/b.txt").as_deref(), Some("unreadable"));
    assert_eq!(s.presence(b"open/a.txt").as_deref(), Some("present"));
    assert_eq!(s.scalar("SELECT COUNT(*) FROM files WHERE presence = 'deleted'"), 0);

    // Restoring the permission lands exactly where a readable scan would have,
    // and records no change for a file that never changed.
    chmod(&locked, 0o755);
    let r3 = s.ingest();
    r3.ok();
    assert_eq!(s.presence(b"locked/b.txt").as_deref(), Some("present"));
    assert_eq!(
        s.scalar("SELECT COUNT(*) FROM scan_changes WHERE scan_id = 3"),
        0,
        "returning to readable is not itself a change"
    );
}

/// Shielding compares whole path segments, not string prefixes. `locked` is a
/// byte prefix of `lockedtoo`, so a naive `LIKE 'locked%'` would wrongly protect
/// a sibling's genuinely deleted rows.
#[test]
fn shielding_respects_path_boundaries() {
    let s = Sandbox::new("prefix");
    s.write("locked/a.txt", "a");
    s.write("lockedtoo/b.txt", "b");
    s.ingest().ok();

    let locked = s.tree().join("locked");
    chmod(&locked, 0o000);
    if !denies_reads(&locked) {
        chmod(&locked, 0o755);
        eprintln!("skipping: running as root, chmod 000 does not deny reads");
        return;
    }
    // The sibling really is gone, and nothing about `locked` should save it.
    std::fs::remove_dir_all(s.tree().join("lockedtoo")).unwrap();

    let r = s.ingest();
    assert_eq!(r.code, EXIT_INCOMPLETE, "got:\n{}", r.out);
    assert_eq!(s.presence(b"locked/a.txt").as_deref(), Some("unreadable"));
    assert_eq!(
        s.presence(b"lockedtoo/b.txt").as_deref(),
        Some("deleted"),
        "a sibling sharing a byte prefix must not be shielded"
    );
    chmod(&locked, 0o755);
}

#[test]
fn unreadable_base_refuses_to_promote() {
    let s = Sandbox::new("base-unreadable");
    s.write("a.txt", "a");
    s.ingest().ok();

    chmod(&s.tree(), 0o000);
    if !denies_reads(&s.tree()) {
        chmod(&s.tree(), 0o755);
        eprintln!("skipping: running as root, chmod 000 does not deny reads");
        return;
    }
    let r = s.ingest();
    chmod(&s.tree(), 0o755);

    assert_ne!(r.code, EXIT_OK, "got:\n{}", r.out);
    assert!(r.has("baseline unchanged"), "got:\n{}", r.out);
    // The base prefixes every path, so nothing could be shielded — the baseline
    // has to be left exactly as it was instead.
    assert_eq!(s.presence(b"a.txt").as_deref(), Some("present"));
    assert_eq!(
        s.scalar("SELECT COUNT(*) FROM scans WHERE status = 'partial'"),
        1
    );
}

// ------------------------------------------------------------- durability --

/// A killed scan must leave the baseline exactly as it was, and the next
/// `ingest` on that base must pick the attempt back up.
#[test]
fn killed_scan_leaves_the_baseline_untouched_and_resumes() {
    let s = Sandbox::new("resume");
    // One row per batch with a pause after each, so the kill lands mid-scan
    // rather than racing a scan that has already finished.
    s.config("writer_batch_rows = 1\nthrottle_ms_after_batch = 60\n");
    for i in 0..80 {
        s.write(&format!("f{i:03}.txt"), &format!("body {i}"));
    }

    let mut child = s
        .cmd()
        .arg("ingest")
        .arg(s.tree())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn()
        .unwrap();
    std::thread::sleep(std::time::Duration::from_millis(700));
    let running_now = s.scalar("SELECT COUNT(*) FROM scans WHERE status = 'running'");
    child.kill().unwrap();
    let _ = child.wait();

    assert_eq!(running_now, 1, "the kill must land while a scan is running");
    // Nothing was promoted: no baseline, and no file rows at all.
    assert_eq!(s.scalar("SELECT COUNT(*) FROM files"), 0);
    assert_eq!(
        s.scalar("SELECT COUNT(*) FROM bases WHERE last_complete_scan_id IS NOT NULL"),
        0
    );
    // The staged work survived the kill, which is what makes the resume cheap.
    assert!(s.scalar("SELECT COUNT(*) FROM scan_stage_entries") > 0);

    // A dead writer's lock is reclaimed rather than blocking for ever.
    s.config("writer_batch_rows = 1000\nthrottle_ms_after_batch = 0\n");
    let r = s.ingest();
    r.ok();
    assert!(r.has("(resumed)"), "expected a resume, got:\n{}", r.out);
    assert_eq!(r.num("files"), 80);
    assert!(
        r.num("hashed") < 80,
        "durably staged rows must not be rehashed, got:\n{}",
        r.out
    );
    assert_eq!(s.scalar("SELECT COUNT(*) FROM files WHERE presence = 'present'"), 80);
    assert_eq!(s.scalar("SELECT COUNT(*) FROM scan_stage_entries"), 0);
}

// ------------------------------------------------------------------ paths --

/// `relative_path` is BINARY. A name that is not valid UTF-8 has to survive the
/// round trip byte for byte; the TEXT helper columns beside it may be lossy.
#[test]
fn non_utf8_names_round_trip_as_bytes() {
    let s = Sandbox::new("bytes");
    s.write("plain.txt", "p");
    let raw: &[u8] = b"od\xffd\xfename.txt";
    s.write_raw("dir", raw, "weird");

    let r = s.ingest();
    r.ok();
    assert_eq!(r.num("files"), 2);

    let mut expected = b"dir/".to_vec();
    expected.extend_from_slice(raw);
    assert_eq!(
        s.presence(&expected).as_deref(),
        Some("present"),
        "the exact bytes must be the stored identity"
    );
    // The lossy helper is a convenience beside it, never the identity.
    assert_eq!(
        s.scalar("SELECT COUNT(*) FROM files WHERE name LIKE '%name.txt'"),
        1
    );

    // And it must still compare correctly on a rerun rather than churning.
    let r2 = s.ingest();
    r2.ok();
    assert_eq!(r2.num("unchanged"), 2);
    assert_eq!(r2.num("added"), 0);
}

// -------------------------------------------------------------- discovery --

#[test]
fn a_foreign_database_is_refused() {
    let s = Sandbox::new("foreign");
    s.write("a.txt", "a");
    // Someone else's SQLite file, sitting where ours would go.
    {
        let c = rusqlite::Connection::open(s.db_path()).unwrap();
        c.execute_batch("CREATE TABLE somebody_elses (x); INSERT INTO somebody_elses VALUES (1);")
            .unwrap();
    }

    let r = s.ingest();
    assert_eq!(r.code, EXIT_CONFIG, "got:\n{}", r.out);
    assert!(r.has("not a metainjester database") || r.has("not ours"), "got:\n{}", r.out);
}

#[test]
fn status_is_read_only_and_reports_no_database() {
    let s = Sandbox::new("status");
    std::fs::remove_file(s.db_path()).unwrap();

    let r = s.status();
    r.ok();
    assert!(r.has("none yet"), "got:\n{}", r.out);
    // Reporting on a missing database must not quietly create one.
    assert!(!s.db_path().exists(), "`status` created a database");
}

#[test]
fn status_reports_the_baseline_and_stays_read_only() {
    let s = Sandbox::new("status-baseline");
    s.write("a.txt", "a");
    s.ingest().ok();
    let before = std::fs::metadata(s.db_path()).unwrap().len();

    let r = s.status();
    r.ok();
    assert!(r.has("baseline"), "got:\n{}", r.out);
    assert_eq!(s.scalar("SELECT COUNT(*) FROM scans"), 1, "`status` started a scan");
    assert_eq!(std::fs::metadata(s.db_path()).unwrap().len(), before);
}

#[test]
fn a_missing_base_is_an_error_not_an_empty_scan() {
    let s = Sandbox::new("missing-base");
    let r = s.ingest_path(&s.root.join("does-not-exist"));
    assert_ne!(r.code, EXIT_OK, "got:\n{}", r.out);
    assert_eq!(
        s.scalar_opt("SELECT COUNT(*) FROM bases").unwrap_or(0),
        0,
        "a base that cannot be read must not be registered"
    );
}
