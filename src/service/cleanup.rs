use std::path::Path;

use chrono::{DateTime, Local};
use tokio::fs;

/// Deletes files under `dir` whose modification time is older than
/// `retention_days`. A `retention_days` of `0` disables cleanup (keep
/// forever). Missing directories or unreadable entries are skipped silently
/// since this is best-effort housekeeping, not a correctness-critical path.
pub(crate) async fn cleanup_old_files(dir: &Path, retention_days: u32) {
    if retention_days == 0 {
        return;
    }
    let cutoff = Local::now() - chrono::Duration::days(retention_days as i64);
    let Ok(mut entries) = fs::read_dir(dir).await else {
        return;
    };
    while let Ok(Some(entry)) = entries.next_entry().await {
        let is_file = matches!(entry.file_type().await, Ok(file_type) if file_type.is_file());
        if !is_file {
            continue;
        }
        let Ok(metadata) = entry.metadata().await else {
            continue;
        };
        let Ok(modified) = metadata.modified() else {
            continue;
        };
        let modified: DateTime<Local> = modified.into();
        if modified < cutoff {
            let _ = fs::remove_file(entry.path()).await;
        }
    }
}
