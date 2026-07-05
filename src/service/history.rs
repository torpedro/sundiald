use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use chrono::{DateTime, Local};
use rusqlite::{Connection, OptionalExtension, params};
use serde::{Deserialize, Serialize};
use tokio::task;
use uuid::Uuid;

use crate::config::JobConfig;

#[derive(Debug, Clone)]
pub(crate) struct HistoryDb {
    path: PathBuf,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunHistoryEntry {
    pub id: i64,
    pub job_uuid: Uuid,
    pub job_name: String,
    pub job_group: Option<String>,
    pub trigger_kind: String,
    pub triggered_at: DateTime<Local>,
    pub finished_at: Option<DateTime<Local>>,
    pub duration_ms: Option<i64>,
    pub exit_code: Option<i32>,
    pub log_path: Option<PathBuf>,
    pub status: Option<String>,
    pub terminated_by_signal: Option<String>,
    pub error: Option<String>,
}

impl HistoryDb {
    pub(crate) async fn open(state_dir: &Path) -> Result<Self> {
        let path = state_dir.join("history.sqlite3");
        let db = Self { path };
        db.initialize().await?;
        Ok(db)
    }

    #[cfg(test)]
    pub(crate) fn test_at(path: PathBuf) -> Self {
        Self { path }
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
                    exit_code INTEGER,
                    log_path TEXT,
                    status TEXT,
                    terminated_by_signal TEXT,
                    error TEXT
                );
                CREATE INDEX IF NOT EXISTS idx_job_runs_job_uuid_triggered_at
                    ON job_runs(job_uuid, triggered_at);
                CREATE INDEX IF NOT EXISTS idx_job_runs_triggered_at
                    ON job_runs(triggered_at);
                "#,
            )?;
            ensure_column(&connection, "log_path", "TEXT")?;
            ensure_column(&connection, "status", "TEXT")?;
            ensure_column(&connection, "terminated_by_signal", "TEXT")?;
            ensure_column(&connection, "error", "TEXT")?;
            Ok(())
        })
        .await
    }

    pub(crate) async fn record_triggered(
        &self,
        job: &JobConfig,
        triggered_at: DateTime<Local>,
        manual: bool,
        log_path: &Path,
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
        let log_path = log_path.display().to_string();

        run_blocking(move || {
            let connection = open_connection(&path)?;
            connection.execute(
                r#"
                INSERT INTO job_runs (
                    job_uuid,
                    job_name,
                    job_group,
                    trigger_kind,
                    triggered_at,
                    log_path
                ) VALUES (?1, ?2, ?3, ?4, ?5, ?6)
                "#,
                params![
                    job_uuid,
                    job_name,
                    job_group,
                    trigger_kind,
                    triggered_at,
                    log_path
                ],
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
        status: &str,
        terminated_by_signal: Option<&str>,
        error: Option<&str>,
    ) -> Result<()> {
        let path = self.path.clone();
        let finished_at_text = finished_at.to_rfc3339();
        let duration_ms = finished_at
            .signed_duration_since(started_at)
            .num_milliseconds()
            .max(0);
        let status = status.to_string();
        let terminated_by_signal = terminated_by_signal.map(str::to_string);
        let error = error.map(str::to_string);

        run_blocking(move || {
            let connection = open_connection(&path)?;
            connection.execute(
                r#"
                UPDATE job_runs
                SET finished_at = ?1,
                    duration_ms = ?2,
                    exit_code = ?3,
                    status = ?4,
                    terminated_by_signal = ?5,
                    error = ?6
                WHERE id = ?7
                "#,
                params![
                    finished_at_text,
                    duration_ms,
                    exit_code,
                    status,
                    terminated_by_signal,
                    error,
                    run_id
                ],
            )?;
            Ok(())
        })
        .await
    }

    pub(crate) async fn runs_for_job(
        &self,
        job_uuid: Uuid,
        limit: usize,
    ) -> Result<Vec<RunHistoryEntry>> {
        let path = self.path.clone();
        let limit = i64::try_from(limit.min(500)).unwrap_or(500);
        run_blocking(move || {
            let connection = open_connection(&path)?;
            let mut statement = connection.prepare(
                r#"
                SELECT id,
                       job_uuid,
                       job_name,
                       job_group,
                       trigger_kind,
                       triggered_at,
                       finished_at,
                       duration_ms,
                       exit_code,
                       log_path,
                       status,
                       terminated_by_signal,
                       error
                FROM job_runs
                WHERE job_uuid = ?1
                ORDER BY triggered_at DESC, id DESC
                LIMIT ?2
                "#,
            )?;
            let rows =
                statement.query_map(params![job_uuid.to_string(), limit], run_history_from_row)?;
            rows.collect::<std::result::Result<Vec<_>, _>>()
                .context("failed to read run history")
        })
        .await
    }

    pub(crate) async fn latest_log_path_for_job(&self, job_uuid: Uuid) -> Result<Option<PathBuf>> {
        let path = self.path.clone();
        run_blocking(move || {
            let connection = open_connection(&path)?;
            let path = connection
                .query_row(
                    r#"
                    SELECT log_path
                    FROM job_runs
                    WHERE job_uuid = ?1 AND log_path IS NOT NULL
                    ORDER BY triggered_at DESC, id DESC
                    LIMIT 1
                    "#,
                    params![job_uuid.to_string()],
                    |row| row.get::<_, String>(0),
                )
                .optional()?;
            Ok(path.map(PathBuf::from))
        })
        .await
    }
}

fn ensure_column(connection: &Connection, name: &str, sql_type: &str) -> Result<()> {
    let mut statement = connection.prepare("PRAGMA table_info(job_runs)")?;
    let columns = statement.query_map([], |row| row.get::<_, String>(1))?;
    for column in columns {
        if column? == name {
            return Ok(());
        }
    }
    connection.execute(
        &format!("ALTER TABLE job_runs ADD COLUMN {name} {sql_type}"),
        [],
    )?;
    Ok(())
}

fn run_history_from_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<RunHistoryEntry> {
    let job_uuid = row.get::<_, String>(1)?;
    let triggered_at = row.get::<_, String>(5)?;
    let finished_at = row.get::<_, Option<String>>(6)?;
    Ok(RunHistoryEntry {
        id: row.get(0)?,
        job_uuid: Uuid::parse_str(&job_uuid).map_err(|error| {
            rusqlite::Error::FromSqlConversionFailure(
                1,
                rusqlite::types::Type::Text,
                Box::new(error),
            )
        })?,
        job_name: row.get(2)?,
        job_group: row.get(3)?,
        trigger_kind: row.get(4)?,
        triggered_at: DateTime::parse_from_rfc3339(&triggered_at)
            .map_err(|error| {
                rusqlite::Error::FromSqlConversionFailure(
                    5,
                    rusqlite::types::Type::Text,
                    Box::new(error),
                )
            })?
            .with_timezone(&Local),
        finished_at: finished_at
            .map(|value| {
                DateTime::parse_from_rfc3339(&value)
                    .map(|time| time.with_timezone(&Local))
                    .map_err(|error| {
                        rusqlite::Error::FromSqlConversionFailure(
                            6,
                            rusqlite::types::Type::Text,
                            Box::new(error),
                        )
                    })
            })
            .transpose()?,
        duration_ms: row.get(7)?,
        exit_code: row.get(8)?,
        log_path: row.get::<_, Option<String>>(9)?.map(PathBuf::from),
        status: row.get(10)?,
        terminated_by_signal: row.get(11)?,
        error: row.get(12)?,
    })
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
            .record_triggered(&job, started_at, true, &temp.path().join("example.log"))
            .await
            .unwrap();
        let finished_at = started_at + chrono::Duration::milliseconds(25);
        history
            .record_finished(
                run_id,
                started_at,
                finished_at,
                Some(42),
                "failed",
                Some("SIGTERM"),
                Some("terminated by SIGTERM"),
            )
            .await
            .unwrap();

        let connection = Connection::open(temp.path().join("history.sqlite3")).unwrap();
        let row = connection
            .query_row(
                "SELECT job_name, job_group, trigger_kind, finished_at, duration_ms, exit_code, log_path, status, terminated_by_signal, error FROM job_runs WHERE id = ?1",
                params![run_id],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, Option<String>>(1)?,
                        row.get::<_, String>(2)?,
                        row.get::<_, Option<String>>(3)?,
                        row.get::<_, i64>(4)?,
                        row.get::<_, Option<i32>>(5)?,
                        row.get::<_, Option<String>>(6)?,
                        row.get::<_, Option<String>>(7)?,
                        row.get::<_, Option<String>>(8)?,
                        row.get::<_, Option<String>>(9)?,
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
        assert!(row.6.as_deref().unwrap().ends_with("example.log"));
        assert_eq!(row.7.as_deref(), Some("failed"));
        assert_eq!(row.8.as_deref(), Some("SIGTERM"));
        assert_eq!(row.9.as_deref(), Some("terminated by SIGTERM"));

        let history_rows = history.runs_for_job(job.uuid.unwrap(), 10).await.unwrap();
        assert_eq!(history_rows.len(), 1);
        assert_eq!(history_rows[0].id, run_id);
        assert_eq!(history_rows[0].status.as_deref(), Some("failed"));

        let latest_log = history
            .latest_log_path_for_job(job.uuid.unwrap())
            .await
            .unwrap()
            .unwrap();
        assert!(latest_log.ends_with("example.log"));
    }
}
