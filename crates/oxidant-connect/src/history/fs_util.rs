//! Filesystem primitives the journal's durability rests on.
//!
//! Two rules, both spelled out because there is no in-tree precedent to inherit — `checkpoint.rs`
//! gets its atomicity from object-store `PUT` and says so:
//!
//! 1. **Every file is created 0600 and every directory 0700, at create time** (`OpenOptions::mode`,
//!    `DirBuilder::mode`), never chmod'd afterwards, so there is no window in which 30 days of
//!    query text is world-readable.
//! 2. **A `rename` is not durable until the containing directory is fsynced.** On ext4/xfs the
//!    rename can be reordered past a crash even though the file's own data was synced.

use std::fs::{DirBuilder, File, OpenOptions};
use std::io;
use std::path::Path;

#[cfg(unix)]
use std::os::unix::fs::{DirBuilderExt, OpenOptionsExt};

/// File mode for everything the engine writes under the data dir.
#[cfg(unix)]
pub(crate) const FILE_MODE: u32 = 0o600;
/// Directory mode for everything the engine creates under the data dir.
#[cfg(unix)]
pub(crate) const DIR_MODE: u32 = 0o700;

/// `mkdir -p` with 0700 on every component this call creates.
pub(crate) fn create_dir_secure(path: &Path) -> io::Result<()> {
    if path.is_dir() {
        return Ok(());
    }
    let mut builder = DirBuilder::new();
    builder.recursive(true);
    #[cfg(unix)]
    builder.mode(DIR_MODE);
    builder.create(path)
}

/// Create a file that must not already exist, 0600.
pub(crate) fn create_new_secure(path: &Path) -> io::Result<File> {
    let mut opts = OpenOptions::new();
    opts.write(true).create_new(true);
    #[cfg(unix)]
    opts.mode(FILE_MODE);
    opts.open(path)
}

/// Open for append, creating at 0600 if absent.
pub(crate) fn append_secure(path: &Path) -> io::Result<File> {
    let mut opts = OpenOptions::new();
    opts.append(true).create(true);
    #[cfg(unix)]
    opts.mode(FILE_MODE);
    opts.open(path)
}

/// Truncating create, 0600 (the compaction `.tmp`, which is redone on every boot that finds one).
pub(crate) fn create_secure(path: &Path) -> io::Result<File> {
    let mut opts = OpenOptions::new();
    opts.write(true).create(true).truncate(true);
    #[cfg(unix)]
    opts.mode(FILE_MODE);
    opts.open(path)
}

/// fsync a directory so a `rename`/`unlink` inside it survives a crash.
///
/// Opening a directory read-only and calling `sync_all` is the portable-enough spelling; on
/// platforms that refuse it the error is swallowed, because a failed directory sync must degrade
/// durability, not fail the write that already succeeded.
pub(crate) fn fsync_dir(path: &Path) {
    if let Ok(dir) = File::open(path) {
        let _ = dir.sync_all();
    }
}

/// `rename` + directory fsync — the only durable rename in this design.
pub(crate) fn rename_durable(from: &Path, to: &Path, dir: &Path) -> io::Result<()> {
    std::fs::rename(from, to)?;
    fsync_dir(dir);
    Ok(())
}
