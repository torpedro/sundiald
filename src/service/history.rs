use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use chrono::{DateTime, Local};
use rusqlite::{Connection, params};
use tokio::task;

use crate::config::JobConfig;

#[derive(Debug, Clone)]
pub(crate) struct HistoryDb {
    path: PathBuf,
}

impl HistoryDb {
    pub(crate) async fn open(state_dir: &Path) -> Result<Self> {
        let path = state_dir.join("history.sqlite3");
        let db = Self { path };
        db.initialize().await?;
        Ok(db)
    }

    async fn initialize(&self) -> Result<()> {
        let path = self.path.clone();
        run_blocking(move || {
            let connection = open_connection(&path)?;
            connection.execute_batch(
                r#"
                CREATE TABLE IF NOT EXISTS job_runs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    job_uuid TEXT NOT NULL,
                    job_name TEXT NOT NULL,
                    job_group TEXT,
                    trigger_kind TEXT NOT NULL CHECK (trigger_kind IN ('automatic', 'manual')),
                    triggered_at TEXT NOT NULL,
                    finished_at TEXT,
                    duration_ms INTEGER,
                    exit_code INTEGER
                );
                CREATE INDEX IF NOT EXISTS idx_job_runs_job_uuid_triggered_at
                    ON job_runs(job_uuid, triggered_at);
                CREATE INDEX IF NOT EXISTS idx_job_runs_triggered_at
                    ON job_runs(triggered_at);
                "#,
            )?;
            Ok(())
        })
        .await
    }

    pub(crate) async fn record_triggered(
        &self,
        job: &JobConfig,
        triggered_at: DateTime<Local>,
        manual: bool,
    ) -> Result<i64> {
        let path = self.path.clone();
        let job_uuid = job
            .uuid
            .expect("job uuid must be assigned before recording history")
            .to_string();
        let job_name = job.name.clone();
        let job_group = job.group.clone();
        let trigger_kind = if manual { "manual" } else { "automatic" }.to_string();
        let triggered_at = triggered_at.to_rfc3339();

        run_blocking(move || {
            let connection = open_connection(&path)?;
            connection.execute(
                r#"
                INSERT INTO job_runs (
                    job_uuid,
                    job_name,
                    job_group,
                    trigger_kind,
                    triggered_at
                ) VALUES (?1, ?2, ?3, ?4, ?5)
                "#,
                params![job_uuid, job_name, job_group, trigger_kind, triggered_at],
            )?;
            Ok(connection.last_insert_rowid())
        })
        .await
    }

    pub(crate) async fn record_finished(
        &self,
        run_id: i64,
        started_at: DateTime<Local>,
        finished_at: DateTime<Local>,
        exit_code: Option<i32>,
    ) -> Result<()> {
        let path = self.path.clone();
        let finished_at_text = finished_at.to_rfc3339();
        let duration_ms = finished_at
            .signed_duration_since(started_at)
            .num_milliseconds()
            .max(0);

        run_blocking(move || {
            let connection = open_connection(&path)?;
            connection.execute(
                r#"
                UPDATE job_runs
                SET finished_at = ?1,
                    duration_ms = ?2,
                    exit_code = ?3
                WHERE id = ?4
                "#,
                params![finished_at_text, duration_ms, exit_code, run_id],
            )?;
            Ok(())
        })
        .await
    }
}

fn open_connection(path: &Path) -> Result<Connection> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("failed to create history db dir {}", parent.display()))?;
    }
    Connection::open(path).with_context(|| format!("failed to open history db {}", path.display()))
}

async fn run_blocking<T>(f: impl FnOnce() -> Result<T> + Send + 'static) -> Result<T>
where
    T: Send + 'static,
{
    task::spawn_blocking(f)
        .await
        .context("history db task panicked")?
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Schedule;
    use uuid::Uuid;

    #[tokio::test]
    async fn records_trigger_and_finish_for_a_job_run() {
        let temp = tempfile::tempdir().unwrap();
        let history = HistoryDb::open(temp.path()).await.unwrap();
        let started_at = Local::now();
        let job = JobConfig {
            uuid: Some(Uuid::new_v4()),
            name: "example".to_string(),
            command: "true".to_string(),
            schedule: Schedule {
                manual_only: true,
                seconds: vec!["0".to_string()],
                minutes: vec!["*".to_string()],
                hours: vec!["*".to_string()],
                days_of_week: vec!["*".to_string()],
                days_of_month: vec!["*".to_string()],
                months: vec!["*".to_string()],
            },
            alert_if_running_for_longer_than: None,
            group: Some("ops".to_string()),
            source_path: None,
        };

        let run_id = history
            .record_triggered(&job, started_at, true)
            .await
            .unwrap();
        let finished_at = started_at + chrono::Duration::milliseconds(25);
        history
            .record_finished(run_id, started_at, finished_at, Some(42))
            .await
            .unwrap();

        let connection = Connection::open(temp.path().join("history.sqlite3")).unwrap();
        let row = connection
            .query_row(
                "SELECT job_name, job_group, trigger_kind, finished_at, duration_ms, exit_code FROM job_runs WHERE id = ?1",
                params![run_id],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, Option<String>>(1)?,
                        row.get::<_, String>(2)?,
                        row.get::<_, Option<String>>(3)?,
                        row.get::<_, i64>(4)?,
                        row.get::<_, Option<i32>>(5)?,
                    ))
                },
            )
            .unwrap();

        assert_eq!(row.0, "example");
        assert_eq!(row.1.as_deref(), Some("ops"));
        assert_eq!(row.2, "manual");
        assert!(row.3.is_some());
        assert_eq!(row.4, 25);
        assert_eq!(row.5, Some(42));
    }
}
