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
    cleanup_directory(dir, cutoff, false).await;
}

async fn cleanup_directory(dir: &Path, cutoff: DateTime<Local>, remove_when_empty: bool) {
    let Ok(mut entries) = fs::read_dir(dir).await else {
        return;
    };
    while let Ok(Some(entry)) = entries.next_entry().await {
        let Ok(file_type) = entry.file_type().await else {
            continue;
        };
        if file_type.is_dir() {
            // Run logs are stored below log_dir in a directory per runnable.
            // file_type() does not follow symlinks, so recursion remains
            // inside the owned directory tree.
            Box::pin(cleanup_directory(&entry.path(), cutoff, true)).await;
            continue;
        }
        if !file_type.is_file() {
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

    if remove_when_empty {
        let is_empty = match fs::read_dir(dir).await {
            Ok(mut entries) => matches!(entries.next_entry().await, Ok(None)),
            Err(_) => false,
        };
        if is_empty {
            let _ = fs::remove_dir(dir).await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn cleanup_removes_nested_logs_and_empty_runnable_directories() {
        let temp = tempfile::tempdir().unwrap();
        let runnable_dir = temp.path().join("backup");
        fs::create_dir_all(&runnable_dir).await.unwrap();
        fs::write(runnable_dir.join("run.stdout.log"), "old")
            .await
            .unwrap();
        fs::write(temp.path().join("direct-alert.json"), "old")
            .await
            .unwrap();

        cleanup_directory(temp.path(), Local::now() + chrono::Duration::days(1), false).await;

        assert!(!runnable_dir.exists());
        assert!(!temp.path().join("direct-alert.json").exists());
        assert!(temp.path().exists());
    }
}
