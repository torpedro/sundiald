use std::{
    collections::BTreeMap,
    io::ErrorKind,
    path::{Path, PathBuf},
};

use anyhow::{Context, Result};
use chrono::{DateTime, Local};
use serde::{Deserialize, Serialize};
use tokio::fs;
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StateSnapshot {
    pub updated_at: DateTime<Local>,
    pub jobs: Vec<JobState>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JobState {
    /// The job's stable identity (`JobConfig::uuid`). This, not `name`, is
    /// what state is actually tracked by, so a rename (same uuid, new name)
    /// carries its run history and live status over correctly.
    pub uuid: Uuid,
    /// Display name as of the last time this entry was written. Not
    /// necessarily current if the job has been renamed since without a
    /// subsequent run (see `reconcile`, which refreshes it from config).
    pub name: String,
    pub status: JobStatus,
    pub pid: Option<u32>,
    pub started_at: Option<DateTime<Local>>,
    pub finished_at: Option<DateTime<Local>>,
    pub exit_code: Option<i32>,
    pub log_path: Option<PathBuf>,
    pub last_error: Option<String>,
    pub terminated_by_signal: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum JobStatus {
    Idle,
    Running,
    Succeeded,
    Failed,
    StartFailed,
    /// Was `Running` when the service last stopped (crash or restart); the
    /// child process died with the service and its actual outcome is
    /// unknown.
    Interrupted,
}

impl JobStatus {
    pub fn is_running(&self) -> bool {
        matches!(self, Self::Running)
    }
}

impl std::fmt::Display for JobStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Idle => write!(f, "idle"),
            Self::Running => write!(f, "running"),
            Self::Succeeded => write!(f, "succeeded"),
            Self::Failed => write!(f, "failed"),
            Self::StartFailed => write!(f, "start_failed"),
            Self::Interrupted => write!(f, "interrupted"),
        }
    }
}

fn idle_job_state(uuid: Uuid, name: String) -> JobState {
    JobState {
        uuid,
        name,
        status: JobStatus::Idle,
        pid: None,
        started_at: None,
        finished_at: None,
        exit_code: None,
        log_path: None,
        last_error: None,
        terminated_by_signal: None,
    }
}

impl StateSnapshot {
    pub fn new(jobs: impl IntoIterator<Item = (Uuid, String)>) -> Self {
        Self {
            updated_at: Local::now(),
            jobs: jobs
                .into_iter()
                .map(|(uuid, name)| idle_job_state(uuid, name))
                .collect(),
        }
    }

    /// Loads a previously persisted snapshot. A missing file means this is the
    /// first run; unreadable or corrupt state is returned as an error so startup
    /// does not silently discard run history.
    pub async fn load(state_dir: &Path) -> Result<Option<Self>> {
        let path = state_path(state_dir);
        let bytes = match fs::read(&path).await {
            Ok(bytes) => bytes,
            Err(error) if error.kind() == ErrorKind::NotFound => return Ok(None),
            Err(error) => {
                return Err(error).with_context(|| format!("failed to read {}", path.display()));
            }
        };
        serde_json::from_slice::<Self>(&bytes)
            .map(Some)
            .with_context(|| format!("failed to parse {}", path.display()))
    }

    /// Reconciles a loaded snapshot against the current set of configured
    /// jobs (by uuid): adds entries for newly configured jobs, drops entries
    /// for jobs no longer configured, refreshes each entry's display name
    /// from the current config (in case of a rename since the last save),
    /// and marks any job that was `Running` when the snapshot was last
    /// saved as `Interrupted`, since that process died along with the
    /// previous service instance and can't still be running.
    pub fn reconcile(mut self, jobs: impl IntoIterator<Item = (Uuid, String)>) -> Self {
        let mut by_uuid = self
            .jobs
            .drain(..)
            .map(|job| (job.uuid, job))
            .collect::<BTreeMap<_, _>>();

        let jobs = jobs
            .into_iter()
            .map(|(uuid, name)| {
                let mut job = by_uuid
                    .remove(&uuid)
                    .unwrap_or_else(|| idle_job_state(uuid, name.clone()));
                job.name = name;
                if job.status.is_running() {
                    job.status = JobStatus::Interrupted;
                    job.pid = None;
                    job.last_error =
                        Some("sundiald service restarted while this job was running".to_string());
                }
                job
            })
            .collect();

        self.jobs = jobs;
        self.updated_at = Local::now();
        self
    }

    pub fn upsert(&mut self, state: JobState) {
        let mut by_uuid = self
            .jobs
            .drain(..)
            .map(|job| (job.uuid, job))
            .collect::<BTreeMap<_, _>>();
        by_uuid.insert(state.uuid, state);
        self.jobs = by_uuid.into_values().collect();
        self.updated_at = Local::now();
    }

    pub async fn save(&self, state_dir: &Path) -> Result<()> {
        fs::create_dir_all(state_dir).await?;
        let path = state_path(state_dir);
        let encoded = serde_json::to_vec_pretty(self)?;
        fs::write(&path, encoded)
            .await
            .with_context(|| format!("failed to write {}", path.display()))
    }
}

fn state_path(state_dir: &Path) -> PathBuf {
    state_dir.join("state.json")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn running_job_state(uuid: Uuid, name: &str) -> JobState {
        JobState {
            uuid,
            name: name.to_string(),
            status: JobStatus::Running,
            pid: Some(123),
            started_at: Some(Local::now()),
            finished_at: None,
            exit_code: None,
            log_path: None,
            last_error: None,
            terminated_by_signal: None,
        }
    }

    #[tokio::test]
    async fn reconcile_marks_a_running_job_as_interrupted_after_restart() {
        let temp = tempfile::tempdir().unwrap();
        let job_id = Uuid::new_v4();
        let mut snapshot = StateSnapshot::new(vec![(job_id, "sleepy".to_string())]);
        snapshot.upsert(running_job_state(job_id, "sleepy"));
        snapshot.save(temp.path()).await.unwrap();

        let loaded = StateSnapshot::load(temp.path())
            .await
            .unwrap()
            .unwrap()
            .reconcile(vec![(job_id, "sleepy".to_string())]);

        let job = loaded.jobs.iter().find(|job| job.uuid == job_id).unwrap();
        assert!(matches!(job.status, JobStatus::Interrupted));
        assert!(job.pid.is_none());
    }

    #[test]
    fn reconcile_refreshes_display_name_and_prunes_removed_jobs() {
        let kept_id = Uuid::new_v4();
        let removed_id = Uuid::new_v4();
        let mut snapshot = StateSnapshot::new(vec![
            (kept_id, "old-name".to_string()),
            (removed_id, "gone".to_string()),
        ]);

        let reconciled = snapshot
            .clone()
            .reconcile(vec![(kept_id, "new-name".to_string())]);

        assert_eq!(reconciled.jobs.len(), 1);
        assert_eq!(reconciled.jobs[0].uuid, kept_id);
        assert_eq!(reconciled.jobs[0].name, "new-name");

        // Adding a job the snapshot never saw before yields a fresh Idle entry.
        let new_id = Uuid::new_v4();
        snapshot = snapshot.reconcile(vec![(new_id, "brand-new".to_string())]);
        assert_eq!(snapshot.jobs.len(), 1);
        assert!(matches!(snapshot.jobs[0].status, JobStatus::Idle));
    }

    #[tokio::test]
    async fn load_reports_corrupt_state_instead_of_resetting() {
        let temp = tempfile::tempdir().unwrap();
        fs::write(state_path(temp.path()), b"not json")
            .await
            .unwrap();

        let error = StateSnapshot::load(temp.path()).await.unwrap_err();

        assert!(error.to_string().contains("failed to parse"));
    }
}
