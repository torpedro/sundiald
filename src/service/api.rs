use std::{
    collections::{HashMap, HashSet},
    net::SocketAddr,
    path::PathBuf,
    sync::Arc,
};

use anyhow::{Context, Result};
use axum::{
    Json, Router,
    extract::{Path as AxumPath, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::{get, post},
};
use chrono::{DateTime, Local};
use serde::{Deserialize, Serialize};
use serde_json::json;
use tokio::{
    fs,
    net::TcpListener,
    sync::{Mutex, RwLock, mpsc},
};
use uuid::Uuid;

use super::process::{JobControl, SignalKind};
use crate::{
    config::SundialdConfig,
    state::{JobStatus, StateSnapshot},
};

#[derive(Debug, Clone)]
pub(crate) struct ApiState {
    pub(crate) config: Arc<RwLock<SundialdConfig>>,
    /// Path the config was originally loaded from, re-read on `/reload`.
    pub(crate) config_path: PathBuf,
    pub(crate) state: Arc<Mutex<StateSnapshot>>,
    pub(crate) pending_manual: Arc<Mutex<HashSet<String>>>,
    pub(crate) manual_tx: mpsc::UnboundedSender<String>,
    pub(crate) running_controls: Arc<Mutex<HashMap<Uuid, mpsc::UnboundedSender<JobControl>>>>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct StatusResponse {
    pub updated_at: DateTime<Local>,
    pub jobs: Vec<JobStatusResponse>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct JobStatusResponse {
    pub uuid: Uuid,
    pub name: String,
    pub status: JobStatus,
    pub pid: Option<u32>,
    pub started_at: Option<DateTime<Local>>,
    pub finished_at: Option<DateTime<Local>>,
    pub exit_code: Option<i32>,
    pub log_path: Option<PathBuf>,
    pub last_error: Option<String>,
    pub terminated_by_signal: Option<String>,
    pub next_run: Option<DateTime<Local>>,
    pub manual_only: bool,
    pub manual_pending: bool,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct RunResponse {
    pub job: String,
    pub queued: bool,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct TerminateResponse {
    pub job: String,
    pub signaled: bool,
    pub signal: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ReloadResponse {
    pub reloaded: bool,
    pub jobs: usize,
}

pub(crate) async fn run_api(bind: SocketAddr, state: ApiState) -> Result<()> {
    let listener = TcpListener::bind(bind)
        .await
        .with_context(|| format!("failed to bind api on {bind}"))?;
    run_api_on_listener(listener, state).await
}

async fn run_api_on_listener(listener: TcpListener, state: ApiState) -> Result<()> {
    let app = Router::new()
        .route("/health", get(|| async { Json(json!({ "ok": true })) }))
        .route("/status", get(api_status))
        .route("/jobs/{job}/run", post(api_run_job))
        .route("/jobs/{job}/terminate", post(api_terminate_job))
        .route("/jobs/{job}/kill", post(api_kill_job))
        .route("/reload", post(api_reload))
        .with_state(state);
    axum::serve(listener, app)
        .await
        .context("api server failed")
}

async fn api_status(State(api): State<ApiState>) -> Json<StatusResponse> {
    Json(build_status_response(&api).await)
}

/// Reloads config from `api.config_path`, validating (and assigning/persisting
/// any missing job ids) before swapping it in. On failure, the previous
/// config is left untouched and the error is reported in the response
/// instead of just being logged, since this is now a synchronous,
/// user-triggered action rather than a fire-and-forget signal.
async fn api_reload(State(api): State<ApiState>) -> Response {
    match SundialdConfig::load_and_ensure_ids(&api.config_path) {
        Ok(new_config) => {
            let job_count = new_config.jobs.len();
            let _ = fs::create_dir_all(&new_config.state_dir).await;
            let _ = fs::create_dir_all(&new_config.log_dir).await;
            let _ = fs::create_dir_all(&new_config.alert.event_dir).await;
            *api.config.write().await = new_config;
            eprintln!("sundiald config reloaded from {}", api.config_path.display());
            (
                StatusCode::OK,
                Json(ReloadResponse {
                    reloaded: true,
                    jobs: job_count,
                }),
            )
                .into_response()
        }
        Err(error) => {
            let message = format!("{error:#}");
            eprintln!(
                "failed to reload config from {}: {message}; keeping previous config",
                api.config_path.display()
            );
            (
                StatusCode::BAD_REQUEST,
                Json(json!({ "reloaded": false, "error": message })),
            )
                .into_response()
        }
    }
}

async fn api_run_job(State(api): State<ApiState>, AxumPath(job): AxumPath<String>) -> Response {
    match enqueue_manual_request(&api, &job).await {
        Ok(()) => (
            StatusCode::ACCEPTED,
            Json(RunResponse { job, queued: true }),
        )
            .into_response(),
        Err(ApiError::UnknownJob(message)) => {
            (StatusCode::NOT_FOUND, Json(json!({ "error": message }))).into_response()
        }
        Err(ApiError::Conflict(message)) => {
            (StatusCode::CONFLICT, Json(json!({ "error": message }))).into_response()
        }
        Err(ApiError::Unavailable(message)) => (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({ "error": message })),
        )
            .into_response(),
    }
}

async fn api_terminate_job(
    State(api): State<ApiState>,
    AxumPath(job): AxumPath<String>,
) -> Response {
    api_signal_job(api, job, SignalKind::Term).await
}

async fn api_kill_job(State(api): State<ApiState>, AxumPath(job): AxumPath<String>) -> Response {
    api_signal_job(api, job, SignalKind::Kill).await
}

/// Resolves a `job` name from a URL path to the uuid it's actually tracked
/// under. Prefers the current config (covers both the ordinary case and a
/// rename: same uuid, the name in the request is whatever the job is called
/// now or was called a moment ago). Falls back to the persisted state
/// snapshot's last-known name for a job that was removed from the config
/// entirely while still running — that job keeps going under the identity
/// it was spawned with, tracked in `running_controls` by uuid, and would
/// otherwise become permanently unreachable the instant its name drops out
/// of the config.
async fn resolve_job_id(api: &ApiState, job_name: &str) -> Option<Uuid> {
    let id_from_config = api
        .config
        .read()
        .await
        .jobs
        .iter()
        .find(|candidate| candidate.name == job_name)
        .and_then(|candidate| candidate.uuid);
    if id_from_config.is_some() {
        return id_from_config;
    }

    api.state
        .lock()
        .await
        .jobs
        .iter()
        .find(|state| state.name == job_name && state.status.is_running())
        .map(|state| state.uuid)
}

async fn api_signal_job(api: ApiState, job: String, signal: SignalKind) -> Response {
    let Some(uuid) = resolve_job_id(&api, &job).await else {
        return (
            StatusCode::NOT_FOUND,
            Json(json!({ "error": format!("unknown job '{job}'") })),
        )
            .into_response();
    };

    let control = {
        let controls = api.running_controls.lock().await;
        controls.get(&uuid).cloned()
    };

    let Some(control) = control else {
        return (
            StatusCode::CONFLICT,
            Json(json!({ "error": format!("job '{job}' is not running") })),
        )
            .into_response();
    };

    if control.send(JobControl::Signal(signal)).is_err() {
        return (
            StatusCode::CONFLICT,
            Json(json!({ "error": format!("job '{job}' is no longer running") })),
        )
            .into_response();
    }

    (
        StatusCode::ACCEPTED,
        Json(TerminateResponse {
            job,
            signaled: true,
            signal: signal.name().to_string(),
        }),
    )
        .into_response()
}

#[derive(Debug)]
enum ApiError {
    UnknownJob(String),
    Conflict(String),
    Unavailable(String),
}

async fn enqueue_manual_request(api: &ApiState, job: &str) -> std::result::Result<(), ApiError> {
    let uuid = api
        .config
        .read()
        .await
        .jobs
        .iter()
        .find(|candidate| candidate.name == job)
        .and_then(|candidate| candidate.uuid);
    let Some(uuid) = uuid else {
        return Err(ApiError::UnknownJob(format!("unknown job '{job}'")));
    };

    {
        let snapshot = api.state.lock().await;
        if snapshot
            .jobs
            .iter()
            .any(|state| state.uuid == uuid && state.status.is_running())
        {
            return Err(ApiError::Conflict(format!(
                "job '{job}' is already running"
            )));
        }
    }

    {
        let mut pending = api.pending_manual.lock().await;
        if !pending.insert(job.to_string()) {
            return Err(ApiError::Conflict(format!(
                "job '{job}' already has a pending manual run request"
            )));
        }
    }

    if api.manual_tx.send(job.to_string()).is_err() {
        api.pending_manual.lock().await.remove(job);
        return Err(ApiError::Unavailable(
            "service is not accepting manual run requests".to_string(),
        ));
    }

    Ok(())
}

pub(crate) async fn build_status_response(api: &ApiState) -> StatusResponse {
    let now = Local::now();
    let config = api.config.read().await.clone();
    let snapshot = api.state.lock().await.clone();
    let pending = api.pending_manual.lock().await.clone();
    let by_id = snapshot
        .jobs
        .into_iter()
        .map(|job| (job.uuid, job))
        .collect::<HashMap<_, _>>();

    let mut jobs: Vec<JobStatusResponse> = config
        .jobs
        .iter()
        .map(|job| {
            let uuid = job
                .uuid
                .expect("job uuid must be assigned before serving status");
            let state = by_id.get(&uuid);
            JobStatusResponse {
                uuid,
                name: job.name.clone(),
                status: state
                    .map(|state| state.status.clone())
                    .unwrap_or(JobStatus::Idle),
                pid: state.and_then(|state| state.pid),
                started_at: state.and_then(|state| state.started_at),
                finished_at: state.and_then(|state| state.finished_at),
                exit_code: state.and_then(|state| state.exit_code),
                log_path: state.and_then(|state| state.log_path.clone()),
                last_error: state.and_then(|state| state.last_error.clone()),
                terminated_by_signal: state.and_then(|state| state.terminated_by_signal.clone()),
                next_run: job.schedule.next_runs(now, 1).into_iter().next(),
                manual_only: job.schedule.manual_only,
                manual_pending: pending.contains(&job.name),
            }
        })
        .collect();

    // A job still running under an uuid that's no longer in the config at all
    // (genuinely removed, not renamed — a rename keeps its uuid and is
    // already handled above) would otherwise vanish from status entirely,
    // leaving no way to even discover the name needed to terminate/kill it.
    let configured_ids: HashSet<Uuid> = config.jobs.iter().filter_map(|job| job.uuid).collect();
    for (uuid, state) in &by_id {
        if configured_ids.contains(uuid) || !state.status.is_running() {
            continue;
        }
        jobs.push(JobStatusResponse {
            uuid: *uuid,
            name: state.name.clone(),
            status: state.status.clone(),
            pid: state.pid,
            started_at: state.started_at,
            finished_at: state.finished_at,
            exit_code: state.exit_code,
            log_path: state.log_path.clone(),
            last_error: Some(
                "orphaned: no longer in the reloaded config, but was still running when reloaded"
                    .to_string(),
            ),
            terminated_by_signal: state.terminated_by_signal.clone(),
            next_run: None,
            manual_only: true,
            manual_pending: false,
        });
    }

    StatusResponse {
        updated_at: Local::now(),
        jobs,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        config::{AlertConfig, JobConfig, Schedule},
        state::JobState,
    };

    fn test_config(state_dir: PathBuf) -> SundialdConfig {
        SundialdConfig {
            state_dir: state_dir.clone(),
            log_dir: state_dir.join("logs"),
            service_log: state_dir.join("sundiald.log"),
            api_bind: "127.0.0.1:0".parse().unwrap(),
            log_retention_days: 14,
            alert: AlertConfig::default(),
            jobs: vec![JobConfig {
                uuid: Some(Uuid::new_v4()),
                name: "sleepy".to_string(),
                command: "sleep 3".to_string(),
                schedule: Schedule {
                    manual_only: false,
                    seconds: vec!["0".to_string()],
                    minutes: vec!["*".to_string()],
                    hours: vec!["*".to_string()],
                    days_of_week: vec!["*".to_string()],
                    days_of_month: vec!["*".to_string()],
                    months: vec!["*".to_string()],
                },
                alert_if_running_for_longer_than: None,
            }],
        }
    }

    fn test_api(config: SundialdConfig, snapshot: StateSnapshot) -> ApiState {
        let (manual_tx, _manual_rx) = mpsc::unbounded_channel();
        ApiState {
            config: Arc::new(RwLock::new(config)),
            config_path: PathBuf::from("sundiald.yaml"),
            state: Arc::new(Mutex::new(snapshot)),
            pending_manual: Arc::new(Mutex::new(HashSet::new())),
            manual_tx,
            running_controls: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    fn test_api_with_sender(
        config: SundialdConfig,
        snapshot: StateSnapshot,
        manual_tx: mpsc::UnboundedSender<String>,
    ) -> ApiState {
        ApiState {
            config: Arc::new(RwLock::new(config)),
            config_path: PathBuf::from("sundiald.yaml"),
            state: Arc::new(Mutex::new(snapshot)),
            pending_manual: Arc::new(Mutex::new(HashSet::new())),
            manual_tx,
            running_controls: Arc::new(Mutex::new(HashMap::new())),
        }
    }

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
    async fn enqueue_manual_run_rejects_running_job() {
        let temp = tempfile::tempdir().unwrap();
        let config = test_config(temp.path().to_path_buf());
        let job_id = config.jobs[0].uuid.unwrap();
        let mut snapshot = StateSnapshot::new(vec![(job_id, "sleepy".to_string())]);
        snapshot.upsert(running_job_state(job_id, "sleepy"));
        let api = test_api(config, snapshot);

        let error = enqueue_manual_request(&api, "sleepy").await.unwrap_err();

        assert!(
            matches!(error, ApiError::Conflict(message) if message.contains("already running"))
        );
    }

    #[tokio::test]
    async fn enqueue_manual_run_rejects_pending_request() {
        let temp = tempfile::tempdir().unwrap();
        let config = test_config(temp.path().to_path_buf());
        let job_id = config.jobs[0].uuid.unwrap();
        let snapshot = StateSnapshot::new(vec![(job_id, "sleepy".to_string())]);
        let api = test_api(config, snapshot);
        api.pending_manual.lock().await.insert("sleepy".to_string());

        let error = enqueue_manual_request(&api, "sleepy").await.unwrap_err();

        assert!(
            matches!(error, ApiError::Conflict(message) if message.contains("pending manual run request"))
        );
    }

    #[tokio::test]
    async fn api_exposes_status_and_manual_run_endpoint() {
        let temp = tempfile::tempdir().unwrap();
        let config = test_config(temp.path().to_path_buf());
        let job_id = config.jobs[0].uuid.unwrap();
        let snapshot = StateSnapshot::new(vec![(job_id, "sleepy".to_string())]);
        let (manual_tx, mut manual_rx) = mpsc::unbounded_channel();
        let api = test_api_with_sender(config, snapshot, manual_tx);
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let handle = tokio::spawn(run_api_on_listener(listener, api));

        let client = reqwest::Client::new();
        let status: StatusResponse = client
            .get(format!("http://{addr}/status"))
            .send()
            .await
            .unwrap()
            .error_for_status()
            .unwrap()
            .json()
            .await
            .unwrap();

        assert_eq!(status.jobs[0].name, "sleepy");
        assert_eq!(status.jobs[0].uuid, job_id);

        let response = client
            .post(format!("http://{addr}/jobs/sleepy/run"))
            .send()
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::ACCEPTED);
        assert_eq!(manual_rx.recv().await.unwrap(), "sleepy");
        handle.abort();
    }

    #[tokio::test]
    async fn api_kill_endpoint_sends_sigkill_control() {
        let temp = tempfile::tempdir().unwrap();
        let config = test_config(temp.path().to_path_buf());
        let job_id = config.jobs[0].uuid.unwrap();
        let mut snapshot = StateSnapshot::new(vec![(job_id, "sleepy".to_string())]);
        snapshot.upsert(running_job_state(job_id, "sleepy"));
        let (manual_tx, _manual_rx) = mpsc::unbounded_channel();
        let api = test_api_with_sender(config, snapshot, manual_tx);
        let (control_tx, mut control_rx) = mpsc::unbounded_channel();
        api.running_controls.lock().await.insert(job_id, control_tx);
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let handle = tokio::spawn(run_api_on_listener(listener, api));

        let response = reqwest::Client::new()
            .post(format!("http://{addr}/jobs/sleepy/kill"))
            .send()
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::ACCEPTED);
        assert!(matches!(
            control_rx.recv().await.unwrap(),
            JobControl::Signal(SignalKind::Kill)
        ));
        handle.abort();
    }

    #[tokio::test]
    async fn renaming_a_running_job_keeps_it_controllable_and_tracked_under_the_new_name() {
        // The uuid is what identifies the job; the name is just a label. A
        // reload that changes `name` but keeps the same `uuid` must let the
        // running process be seen and controlled under its *new* name,
        // with its live status intact — not treated as a fresh, idle job.
        let temp = tempfile::tempdir().unwrap();
        let mut config = test_config(temp.path().to_path_buf());
        let job_id = config.jobs[0].uuid.unwrap();
        config.jobs[0].name = "renamed".to_string();

        // State was last written under the old name (from before the
        // rename), same uuid.
        let mut snapshot = StateSnapshot::new(vec![(job_id, "renamed".to_string())]);
        snapshot.upsert(running_job_state(job_id, "sleepy"));
        let (manual_tx, _manual_rx) = mpsc::unbounded_channel();
        let api = test_api_with_sender(config, snapshot, manual_tx);
        let (control_tx, mut control_rx) = mpsc::unbounded_channel();
        api.running_controls.lock().await.insert(job_id, control_tx);
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let handle = tokio::spawn(run_api_on_listener(listener, api));

        // /status must show it as running under the NEW name, not idle.
        let status: StatusResponse = reqwest::Client::new()
            .get(format!("http://{addr}/status"))
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        assert_eq!(status.jobs.len(), 1);
        assert_eq!(status.jobs[0].name, "renamed");
        assert_eq!(status.jobs[0].uuid, job_id);
        assert!(matches!(status.jobs[0].status, JobStatus::Running));

        // The NEW name must be able to terminate it.
        let response = reqwest::Client::new()
            .post(format!("http://{addr}/jobs/renamed/terminate"))
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::ACCEPTED);
        assert!(matches!(
            control_rx.recv().await.unwrap(),
            JobControl::Signal(SignalKind::Term)
        ));

        handle.abort();
    }

    #[tokio::test]
    async fn api_terminate_endpoint_still_reaches_a_job_removed_entirely_from_config() {
        // A job that's genuinely deleted (no uuid in the new config at all,
        // not just renamed) keeps running under its last-known name until
        // it finishes, and must remain discoverable/controllable by that
        // name via the state-snapshot fallback.
        let temp = tempfile::tempdir().unwrap();
        let config = SundialdConfig {
            jobs: Vec::new(),
            ..test_config(temp.path().to_path_buf())
        };
        let orphan_id = Uuid::new_v4();
        let mut snapshot = StateSnapshot::new(Vec::new());
        snapshot.upsert(running_job_state(orphan_id, "sleepy"));
        let (manual_tx, _manual_rx) = mpsc::unbounded_channel();
        let api = test_api_with_sender(config, snapshot, manual_tx);
        let (control_tx, mut control_rx) = mpsc::unbounded_channel();
        api.running_controls
            .lock()
            .await
            .insert(orphan_id, control_tx);
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let handle = tokio::spawn(run_api_on_listener(listener, api));

        let status: StatusResponse = reqwest::Client::new()
            .get(format!("http://{addr}/status"))
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        assert!(status.jobs.iter().any(|job| job.name == "sleepy"));

        let response = reqwest::Client::new()
            .post(format!("http://{addr}/jobs/sleepy/terminate"))
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::ACCEPTED);
        assert!(matches!(
            control_rx.recv().await.unwrap(),
            JobControl::Signal(SignalKind::Term)
        ));

        handle.abort();
    }

    #[tokio::test]
    async fn api_reload_endpoint_picks_up_config_changes_from_disk() {
        let temp = tempfile::tempdir().unwrap();
        let config_path = temp.path().join("sundiald.yaml");
        tokio::fs::write(
            &config_path,
            r#"
jobs:
  - name: original
    command: "true"
    schedule:
      minutes: ["0"]
"#,
        )
        .await
        .unwrap();

        let config = SundialdConfig::load_and_ensure_ids(&config_path).unwrap();
        let job_id = config.jobs[0].uuid.unwrap();
        let mut api = test_api(
            config,
            StateSnapshot::new(vec![(job_id, "original".to_string())]),
        );
        api.config_path = config_path.clone();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let api_config = Arc::clone(&api.config);
        let handle = tokio::spawn(run_api_on_listener(listener, api));

        assert_eq!(api_config.read().await.jobs[0].name, "original");

        tokio::fs::write(
            &config_path,
            r#"
jobs:
  - name: reloaded
    command: "true"
    schedule:
      minutes: ["0"]
  - name: second
    command: "true"
    schedule:
      minutes: ["0"]
"#,
        )
        .await
        .unwrap();

        let response = reqwest::Client::new()
            .post(format!("http://{addr}/reload"))
            .send()
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body: ReloadResponse = response.json().await.unwrap();
        assert!(body.reloaded);
        assert_eq!(body.jobs, 2);

        let reloaded_config = api_config.read().await;
        assert_eq!(reloaded_config.jobs[0].name, "reloaded");
        assert_eq!(reloaded_config.jobs[1].name, "second");
        assert!(reloaded_config.jobs[0].uuid.is_some());
        assert!(reloaded_config.jobs[1].uuid.is_some());
        handle.abort();
    }

    #[tokio::test]
    async fn api_reload_endpoint_rejects_invalid_config_and_keeps_previous() {
        let temp = tempfile::tempdir().unwrap();
        let config_path = temp.path().join("sundiald.yaml");
        tokio::fs::write(
            &config_path,
            r#"
jobs:
  - name: original
    command: "true"
    schedule:
      minutes: ["0"]
"#,
        )
        .await
        .unwrap();

        let config = SundialdConfig::load_and_ensure_ids(&config_path).unwrap();
        let job_id = config.jobs[0].uuid.unwrap();
        let mut api = test_api(
            config,
            StateSnapshot::new(vec![(job_id, "original".to_string())]),
        );
        api.config_path = config_path.clone();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let api_config = Arc::clone(&api.config);
        let handle = tokio::spawn(run_api_on_listener(listener, api));

        // Duplicate job names make this config invalid.
        tokio::fs::write(
            &config_path,
            r#"
jobs:
  - name: original
    command: "true"
    schedule:
      minutes: ["0"]
  - name: original
    command: "true"
    schedule:
      minutes: ["0"]
"#,
        )
        .await
        .unwrap();

        let response = reqwest::Client::new()
            .post(format!("http://{addr}/reload"))
            .send()
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        assert_eq!(api_config.read().await.jobs.len(), 1);
        handle.abort();
    }
}
