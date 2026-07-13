use std::{
    collections::{HashMap, HashSet},
    net::SocketAddr,
    path::{Path, PathBuf},
    sync::Arc,
};

use anyhow::{Context, Result};
use axum::{
    Json, Router,
    extract::{Path as AxumPath, Query, State},
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

use super::ServiceCommand;
use super::history::{HistoryDb, RunHistoryEntry};
use super::process::{JobControl, SignalKind};
use crate::{
    config::{JobTrigger, ServiceSchedule, SundialdConfig},
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
    pub(crate) service_tx: mpsc::UnboundedSender<ServiceCommand>,
    pub(crate) running_controls: Arc<Mutex<HashMap<Uuid, mpsc::UnboundedSender<JobControl>>>>,
    pub(crate) history: HistoryDb,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct StatusResponse {
    pub updated_at: DateTime<Local>,
    pub jobs: Vec<JobStatusResponse>,
    pub services: Vec<ServiceStatusResponse>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct JobStatusResponse {
    pub uuid: Uuid,
    pub name: String,
    pub group: Option<String>,
    pub status: JobStatus,
    pub pid: Option<u32>,
    pub started_at: Option<DateTime<Local>>,
    pub finished_at: Option<DateTime<Local>>,
    pub exit_code: Option<i32>,
    pub log_path: Option<PathBuf>,
    pub last_error: Option<String>,
    pub terminated_by_signal: Option<String>,
    pub next_run: Option<DateTime<Local>>,
    pub next_runs: Vec<DateTime<Local>>,
    pub trigger: TriggerStatusResponse,
    pub manual_pending: bool,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct TriggerStatusResponse {
    pub kind: String,
    pub after: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ServiceStatusResponse {
    pub uuid: Uuid,
    pub name: String,
    pub group: Option<String>,
    pub status: JobStatus,
    pub pid: Option<u32>,
    pub started_at: Option<DateTime<Local>>,
    pub finished_at: Option<DateTime<Local>>,
    pub exit_code: Option<i32>,
    pub log_path: Option<PathBuf>,
    pub last_error: Option<String>,
    pub terminated_by_signal: Option<String>,
    pub schedule: String,
    pub expected_running: bool,
    pub next_start: Option<DateTime<Local>>,
    pub next_stop: Option<DateTime<Local>>,
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
pub struct ServiceControlResponse {
    pub service: String,
    pub queued: bool,
    pub action: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ReloadResponse {
    pub reloaded: bool,
    pub jobs: usize,
    pub services: usize,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct HistoryResponse {
    pub job: String,
    pub uuid: Uuid,
    pub runs: Vec<RunHistoryEntry>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct LogResponse {
    pub job: String,
    pub uuid: Uuid,
    pub log_path: PathBuf,
    pub stderr_log_path: Option<PathBuf>,
    pub content: String,
    pub stderr_content: Option<String>,
}

#[derive(Debug, Deserialize)]
struct LimitQuery {
    limit: Option<usize>,
    tail: Option<usize>,
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
        .route("/jobs/{job}/history", get(api_job_history))
        .route("/jobs/{job}/logs/latest", get(api_latest_log))
        .route("/services/{service}/start", post(api_start_service))
        .route("/services/{service}/stop", post(api_stop_service))
        .route("/services/{service}/kill", post(api_kill_service))
        .route("/services/{service}/logs/latest", get(api_latest_log))
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
        Ok(mut new_config) => {
            let runtime_base = match std::env::current_dir() {
                Ok(path) => path,
                Err(error) => {
                    let message = format!("failed to resolve sundiald working directory: {error}");
                    eprintln!("{message}");
                    return (
                        StatusCode::INTERNAL_SERVER_ERROR,
                        Json(json!({ "reloaded": false, "error": message })),
                    )
                        .into_response();
                }
            };
            new_config.absolutize_runtime_paths(&runtime_base);
            let job_count = new_config.jobs.len();
            let service_count = new_config.services.len();
            let _ = fs::create_dir_all(&new_config.state_dir).await;
            let _ = fs::create_dir_all(&new_config.log_dir).await;
            let _ = fs::create_dir_all(&new_config.alert.event_dir).await;
            *api.config.write().await = new_config;
            eprintln!(
                "sundiald config reloaded from {}",
                api.config_path.display()
            );
            (
                StatusCode::OK,
                Json(ReloadResponse {
                    reloaded: true,
                    jobs: job_count,
                    services: service_count,
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

async fn api_start_service(
    State(api): State<ApiState>,
    AxumPath(service): AxumPath<String>,
) -> Response {
    api_control_service(api, service, ServiceAction::Start).await
}

async fn api_stop_service(
    State(api): State<ApiState>,
    AxumPath(service): AxumPath<String>,
) -> Response {
    api_control_service(api, service, ServiceAction::Stop).await
}

async fn api_kill_service(
    State(api): State<ApiState>,
    AxumPath(service): AxumPath<String>,
) -> Response {
    api_control_service(api, service, ServiceAction::Kill).await
}

async fn api_job_history(
    State(api): State<ApiState>,
    AxumPath(job): AxumPath<String>,
    Query(query): Query<LimitQuery>,
) -> Response {
    let Some(uuid) = resolve_job_id(&api, &job).await else {
        return (
            StatusCode::NOT_FOUND,
            Json(json!({ "error": format!("unknown job '{job}'") })),
        )
            .into_response();
    };
    let limit = query.limit.unwrap_or(50).clamp(1, 500);
    match api.history.runs_for_job(uuid, limit).await {
        Ok(runs) => (
            StatusCode::OK,
            Json(HistoryResponse {
                job: resolve_job_label(&api, uuid).await,
                uuid,
                runs,
            }),
        )
            .into_response(),
        Err(error) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({ "error": format!("{error:#}") })),
        )
            .into_response(),
    }
}

async fn api_latest_log(
    State(api): State<ApiState>,
    AxumPath(job): AxumPath<String>,
    Query(query): Query<LimitQuery>,
) -> Response {
    let Some(uuid) = resolve_runnable_id(&api, &job).await else {
        return (
            StatusCode::NOT_FOUND,
            Json(json!({ "error": format!("unknown job or service '{job}'") })),
        )
            .into_response();
    };

    let state_log_path = api
        .state
        .lock()
        .await
        .jobs
        .iter()
        .find(|state| state.uuid == uuid)
        .and_then(|state| state.log_path.clone());
    let log_path = match state_log_path {
        Some(path) => Some(path),
        None => match api.history.latest_log_path_for_job(uuid).await {
            Ok(path) => path,
            Err(error) => {
                return (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(json!({ "error": format!("{error:#}") })),
                )
                    .into_response();
            }
        },
    };
    let Some(log_path) = log_path else {
        return (
            StatusCode::NOT_FOUND,
            Json(json!({ "error": format!("job or service '{job}' has no log file") })),
        )
            .into_response();
    };
    let config = api.config.read().await.clone();
    let runtime_base = std::env::current_dir().unwrap_or_else(|_| PathBuf::from("."));
    let log_path = resolve_log_path(&config.log_dir, &runtime_base, &log_path);
    let stderr_log_path = stderr_log_path_for_stdout(&log_path);
    match fs::read_to_string(&log_path).await {
        Ok(content) => {
            let tail = query.tail.unwrap_or(40).clamp(1, 2_000);
            let stderr = match fs::read_to_string(&stderr_log_path).await {
                Ok(content) => Some((
                    stderr_log_path,
                    tail_lines(&content, tail).unwrap_or_else(|| "(empty log)".to_string()),
                )),
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
                Err(error) => {
                    return (
                        StatusCode::INTERNAL_SERVER_ERROR,
                        Json(json!({
                            "error": format!("failed to read {}: {error}", stderr_log_path.display())
                        })),
                    )
                        .into_response();
                }
            };
            let (stderr_log_path, stderr_content) = stderr
                .map(|(path, content)| (Some(path), Some(content)))
                .unwrap_or((None, None));
            (
                StatusCode::OK,
                Json(LogResponse {
                    job: resolve_runnable_label(&api, uuid).await,
                    uuid,
                    log_path,
                    stderr_log_path,
                    content: tail_lines(&content, tail)
                        .unwrap_or_else(|| "(empty log)".to_string()),
                    stderr_content,
                }),
            )
                .into_response()
        }
        Err(error) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({ "error": format!("failed to read {}: {error}", log_path.display()) })),
        )
            .into_response(),
    }
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
    if let Ok(uuid) = Uuid::parse_str(job_name) {
        let config_has_id = api
            .config
            .read()
            .await
            .jobs
            .iter()
            .any(|candidate| candidate.uuid == Some(uuid));
        if config_has_id {
            return Some(uuid);
        }
        let state_has_running_id = api
            .state
            .lock()
            .await
            .jobs
            .iter()
            .any(|state| state.uuid == uuid && state.status.is_running());
        if state_has_running_id {
            return Some(uuid);
        }
    }

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

async fn resolve_service_id(api: &ApiState, service_name: &str) -> Option<Uuid> {
    if let Ok(uuid) = Uuid::parse_str(service_name) {
        let config_has_id = api
            .config
            .read()
            .await
            .services
            .iter()
            .any(|candidate| candidate.uuid == Some(uuid));
        if config_has_id {
            return Some(uuid);
        }
        let state_has_running_id = api
            .state
            .lock()
            .await
            .jobs
            .iter()
            .any(|state| state.uuid == uuid && state.status.is_running());
        if state_has_running_id {
            return Some(uuid);
        }
    }

    let id_from_config = api
        .config
        .read()
        .await
        .services
        .iter()
        .find(|candidate| candidate.name == service_name)
        .and_then(|candidate| candidate.uuid);
    if id_from_config.is_some() {
        return id_from_config;
    }

    api.state
        .lock()
        .await
        .jobs
        .iter()
        .find(|state| state.name == service_name && state.status.is_running())
        .map(|state| state.uuid)
}

async fn resolve_runnable_id(api: &ApiState, name: &str) -> Option<Uuid> {
    if let Some(uuid) = resolve_job_id(api, name).await {
        return Some(uuid);
    }
    resolve_service_id(api, name).await
}

async fn resolve_job_label(api: &ApiState, uuid: Uuid) -> String {
    if let Some(name) = api
        .config
        .read()
        .await
        .jobs
        .iter()
        .find(|job| job.uuid == Some(uuid))
        .map(|job| job.name.clone())
    {
        return name;
    }

    api.state
        .lock()
        .await
        .jobs
        .iter()
        .find(|state| state.uuid == uuid)
        .map(|state| state.name.clone())
        .unwrap_or_else(|| uuid.to_string())
}

async fn resolve_service_label(api: &ApiState, uuid: Uuid) -> String {
    if let Some(name) = api
        .config
        .read()
        .await
        .services
        .iter()
        .find(|service| service.uuid == Some(uuid))
        .map(|service| service.name.clone())
    {
        return name;
    }

    api.state
        .lock()
        .await
        .jobs
        .iter()
        .find(|state| state.uuid == uuid)
        .map(|state| state.name.clone())
        .unwrap_or_else(|| uuid.to_string())
}

async fn resolve_runnable_label(api: &ApiState, uuid: Uuid) -> String {
    let config = api.config.read().await;
    if let Some(name) = config
        .jobs
        .iter()
        .find(|job| job.uuid == Some(uuid))
        .map(|job| job.name.clone())
    {
        return name;
    }
    if let Some(name) = config
        .services
        .iter()
        .find(|service| service.uuid == Some(uuid))
        .map(|service| service.name.clone())
    {
        return name;
    }
    drop(config);

    api.state
        .lock()
        .await
        .jobs
        .iter()
        .find(|state| state.uuid == uuid)
        .map(|state| state.name.clone())
        .unwrap_or_else(|| uuid.to_string())
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

    if control
        .send(JobControl::Signal {
            kind: signal,
            expected: false,
        })
        .is_err()
    {
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

#[derive(Debug, Clone, Copy)]
enum ServiceAction {
    Start,
    Stop,
    Kill,
}

impl ServiceAction {
    fn label(self) -> &'static str {
        match self {
            Self::Start => "start",
            Self::Stop => "stop",
            Self::Kill => "kill",
        }
    }

    fn command(self, service: String) -> ServiceCommand {
        match self {
            Self::Start => ServiceCommand::Start(service),
            Self::Stop => ServiceCommand::Stop(service),
            Self::Kill => ServiceCommand::Kill(service),
        }
    }
}

async fn api_control_service(api: ApiState, service: String, action: ServiceAction) -> Response {
    let Some(uuid) = resolve_service_id(&api, &service).await else {
        return (
            StatusCode::NOT_FOUND,
            Json(json!({ "error": format!("unknown service '{service}'") })),
        )
            .into_response();
    };
    let service_name = resolve_service_label(&api, uuid).await;

    if api
        .service_tx
        .send(action.command(service_name.clone()))
        .is_err()
    {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({ "error": "service is not accepting service control requests" })),
        )
            .into_response();
    }

    (
        StatusCode::ACCEPTED,
        Json(ServiceControlResponse {
            service: service_name,
            queued: true,
            action: action.label().to_string(),
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
    let uuid = resolve_job_id(api, job).await;
    let Some(uuid) = uuid else {
        return Err(ApiError::UnknownJob(format!("unknown job '{job}'")));
    };
    let job_name = api
        .config
        .read()
        .await
        .jobs
        .iter()
        .find(|candidate| candidate.uuid == Some(uuid))
        .map(|candidate| candidate.name.clone());
    let Some(job_name) = job_name else {
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
        if !pending.insert(job_name.clone()) {
            return Err(ApiError::Conflict(format!(
                "job '{job_name}' already has a pending manual run request"
            )));
        }
    }

    if api.manual_tx.send(job_name.clone()).is_err() {
        api.pending_manual.lock().await.remove(&job_name);
        return Err(ApiError::Unavailable(
            "service is not accepting manual run requests".to_string(),
        ));
    }

    Ok(())
}

pub(crate) async fn build_status_response(api: &ApiState) -> StatusResponse {
    let now = Local::now();
    let runtime_base = std::env::current_dir().unwrap_or_else(|_| PathBuf::from("."));
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
            let next_runs = job
                .trigger
                .schedule()
                .map(|schedule| schedule.next_runs(now, 10))
                .unwrap_or_default();
            JobStatusResponse {
                uuid,
                name: job.name.clone(),
                group: job.group.clone(),
                status: state
                    .map(|state| state.status.clone())
                    .unwrap_or(JobStatus::Idle),
                pid: state.and_then(|state| state.pid),
                started_at: state.and_then(|state| state.started_at),
                finished_at: state.and_then(|state| state.finished_at),
                exit_code: state.and_then(|state| state.exit_code),
                log_path: state
                    .and_then(|state| state.log_path.clone())
                    .map(|path| resolve_log_path(&config.log_dir, &runtime_base, &path)),
                last_error: state.and_then(|state| state.last_error.clone()),
                terminated_by_signal: state.and_then(|state| state.terminated_by_signal.clone()),
                next_run: next_runs.first().copied(),
                next_runs,
                trigger: trigger_response(&job.trigger),
                manual_pending: pending.contains(&job.name),
            }
        })
        .collect();

    // A job still running under an uuid that's no longer in the config at all
    // (genuinely removed, not renamed — a rename keeps its uuid and is
    // already handled above) would otherwise vanish from status entirely,
    // leaving no way to even discover the name needed to terminate/kill it.
    let configured_ids: HashSet<Uuid> = config
        .jobs
        .iter()
        .filter_map(|job| job.uuid)
        .chain(config.services.iter().filter_map(|service| service.uuid))
        .collect();
    for (uuid, state) in &by_id {
        if configured_ids.contains(uuid) || !state.status.is_running() {
            continue;
        }
        jobs.push(JobStatusResponse {
            uuid: *uuid,
            name: state.name.clone(),
            group: None,
            status: state.status.clone(),
            pid: state.pid,
            started_at: state.started_at,
            finished_at: state.finished_at,
            exit_code: state.exit_code,
            log_path: state
                .log_path
                .clone()
                .map(|path| resolve_log_path(&config.log_dir, &runtime_base, &path)),
            last_error: Some(
                "orphaned: no longer in the reloaded config, but was still running when reloaded"
                    .to_string(),
            ),
            terminated_by_signal: state.terminated_by_signal.clone(),
            next_run: None,
            next_runs: Vec::new(),
            trigger: TriggerStatusResponse {
                kind: "manual".to_string(),
                after: None,
            },
            manual_pending: false,
        });
    }

    let mut services: Vec<ServiceStatusResponse> = config
        .services
        .iter()
        .map(|service| {
            let uuid = service
                .uuid
                .expect("service uuid must be assigned before serving status");
            let state = by_id.get(&uuid);
            let (schedule, next_start, next_stop) = match &service.schedule {
                ServiceSchedule::Permanent => ("permanent".to_string(), None, None),
                ServiceSchedule::Window { start, stop } => {
                    let start_runs = start.next_runs(now, 1);
                    let stop_runs = stop.next_runs(now, 1);
                    (
                        "window".to_string(),
                        start_runs.first().copied(),
                        stop_runs.first().copied(),
                    )
                }
            };
            let expected_running = match &service.schedule {
                ServiceSchedule::Permanent => true,
                ServiceSchedule::Window { .. } => super::service_is_inside_runtime(service, now),
            };
            ServiceStatusResponse {
                uuid,
                name: service.name.clone(),
                group: service.group.clone(),
                status: state
                    .map(|state| state.status.clone())
                    .unwrap_or(JobStatus::Idle),
                pid: state.and_then(|state| state.pid),
                started_at: state.and_then(|state| state.started_at),
                finished_at: state.and_then(|state| state.finished_at),
                exit_code: state.and_then(|state| state.exit_code),
                log_path: state
                    .and_then(|state| state.log_path.clone())
                    .map(|path| resolve_log_path(&config.log_dir, &runtime_base, &path)),
                last_error: state.and_then(|state| state.last_error.clone()),
                terminated_by_signal: state.and_then(|state| state.terminated_by_signal.clone()),
                schedule,
                expected_running,
                next_start,
                next_stop,
            }
        })
        .collect();
    services.sort_by(|left, right| {
        left.group
            .cmp(&right.group)
            .then_with(|| left.name.cmp(&right.name))
    });

    StatusResponse {
        updated_at: Local::now(),
        jobs,
        services,
    }
}

fn trigger_response(trigger: &JobTrigger) -> TriggerStatusResponse {
    TriggerStatusResponse {
        kind: trigger.kind().to_string(),
        after: trigger.after().map(str::to_string),
    }
}

fn absolutize_path(base: &Path, path: &Path) -> PathBuf {
    if path.is_absolute() {
        path.to_path_buf()
    } else {
        base.join(path)
    }
}

fn resolve_log_path(log_dir: &Path, runtime_base: &Path, path: &Path) -> PathBuf {
    if path.is_absolute() {
        return path.to_path_buf();
    }
    if path.components().count() == 1 {
        return log_dir.join(path);
    }
    absolutize_path(runtime_base, path)
}

fn stderr_log_path_for_stdout(stdout_log_path: &Path) -> PathBuf {
    let Some(file_name) = stdout_log_path.file_name().and_then(|name| name.to_str()) else {
        return stdout_log_path.with_extension("stderr.log");
    };
    let stderr_file_name = file_name
        .strip_suffix(".stdout.log")
        .map(|stem| format!("{stem}.stderr.log"))
        .unwrap_or_else(|| format!("{file_name}.stderr.log"));
    stdout_log_path.with_file_name(stderr_file_name)
}

fn tail_lines(content: &str, max_lines: usize) -> Option<String> {
    let lines = content.lines().collect::<Vec<_>>();
    if lines.is_empty() {
        return None;
    }

    let start = lines.len().saturating_sub(max_lines);
    let mut output = String::new();
    if start > 0 {
        output.push_str(&format!("... {} earlier line(s) omitted\n", start));
    }
    output.push_str(&lines[start..].join("\n"));
    Some(output)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        config::{AlertConfig, JobConfig, JobTrigger, Schedule, ServiceConfig, ServiceSchedule},
        service::history::HistoryDb,
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
            env: HashMap::new(),
            job_files: Vec::new(),
            jobs: vec![JobConfig {
                uuid: Some(Uuid::new_v4()),
                name: "sleepy".to_string(),
                command: "sleep 3".to_string(),
                trigger: JobTrigger::Schedule(Schedule {
                    seconds: vec!["0".to_string()],
                    minutes: vec!["*".to_string()],
                    hours: vec!["*".to_string()],
                    days_of_week: vec!["*".to_string()],
                    days_of_month: vec!["*".to_string()],
                    months: vec!["*".to_string()],
                }),
                alert_if_running_for_longer_than: None,
                group: None,
                env: HashMap::new(),
                source_path: None,
            }],
            services: Vec::new(),
        }
    }

    fn test_service(name: &str) -> ServiceConfig {
        ServiceConfig {
            uuid: Some(Uuid::new_v4()),
            name: name.to_string(),
            command: "sleep 60".to_string(),
            schedule: ServiceSchedule::Permanent,
            stop_grace_period: Some("5s".to_string()),
            group: None,
            env: HashMap::new(),
            source_path: None,
        }
    }

    fn test_api(config: SundialdConfig, snapshot: StateSnapshot) -> ApiState {
        let (manual_tx, _manual_rx) = mpsc::unbounded_channel();
        let (service_tx, _service_rx) = mpsc::unbounded_channel();
        let history = HistoryDb::test_at(config.state_dir.join("history.sqlite3"));
        ApiState {
            config: Arc::new(RwLock::new(config)),
            config_path: PathBuf::from("sundiald.yaml"),
            state: Arc::new(Mutex::new(snapshot)),
            pending_manual: Arc::new(Mutex::new(HashSet::new())),
            manual_tx,
            service_tx,
            running_controls: Arc::new(Mutex::new(HashMap::new())),
            history,
        }
    }

    fn test_api_with_sender(
        config: SundialdConfig,
        snapshot: StateSnapshot,
        manual_tx: mpsc::UnboundedSender<String>,
    ) -> ApiState {
        let (service_tx, _service_rx) = mpsc::unbounded_channel();
        test_api_with_senders(config, snapshot, manual_tx, service_tx)
    }

    fn test_api_with_senders(
        config: SundialdConfig,
        snapshot: StateSnapshot,
        manual_tx: mpsc::UnboundedSender<String>,
        service_tx: mpsc::UnboundedSender<ServiceCommand>,
    ) -> ApiState {
        let history = HistoryDb::test_at(config.state_dir.join("history.sqlite3"));
        ApiState {
            config: Arc::new(RwLock::new(config)),
            config_path: PathBuf::from("sundiald.yaml"),
            state: Arc::new(Mutex::new(snapshot)),
            pending_manual: Arc::new(Mutex::new(HashSet::new())),
            manual_tx,
            service_tx,
            running_controls: Arc::new(Mutex::new(HashMap::new())),
            history,
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
        assert_eq!(status.jobs[0].next_runs.len(), 10);
        assert_eq!(
            status.jobs[0].next_run,
            status.jobs[0].next_runs.first().copied()
        );

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
    async fn api_manual_run_endpoint_accepts_job_uuid() {
        let temp = tempfile::tempdir().unwrap();
        let config = test_config(temp.path().to_path_buf());
        let job_id = config.jobs[0].uuid.unwrap();
        let snapshot = StateSnapshot::new(vec![(job_id, "sleepy".to_string())]);
        let (manual_tx, mut manual_rx) = mpsc::unbounded_channel();
        let api = test_api_with_sender(config, snapshot, manual_tx);
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let handle = tokio::spawn(run_api_on_listener(listener, api));

        let response = reqwest::Client::new()
            .post(format!("http://{addr}/jobs/{job_id}/run"))
            .send()
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::ACCEPTED);
        assert_eq!(manual_rx.recv().await.unwrap(), "sleepy");
        handle.abort();
    }

    #[tokio::test]
    async fn api_exposes_service_status_and_start_endpoint() {
        let temp = tempfile::tempdir().unwrap();
        let mut config = test_config(temp.path().to_path_buf());
        let job_id = config.jobs[0].uuid.unwrap();
        let service = test_service("worker");
        let service_id = service.uuid.unwrap();
        config.services.push(service);
        let snapshot = StateSnapshot::new(vec![
            (job_id, "sleepy".to_string()),
            (service_id, "worker".to_string()),
        ]);
        let (manual_tx, _manual_rx) = mpsc::unbounded_channel();
        let (service_tx, mut service_rx) = mpsc::unbounded_channel();
        let api = test_api_with_senders(config, snapshot, manual_tx, service_tx);
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let handle = tokio::spawn(run_api_on_listener(listener, api));

        let status: StatusResponse = reqwest::Client::new()
            .get(format!("http://{addr}/status"))
            .send()
            .await
            .unwrap()
            .error_for_status()
            .unwrap()
            .json()
            .await
            .unwrap();

        assert_eq!(status.services.len(), 1);
        assert_eq!(status.services[0].name, "worker");
        assert_eq!(status.services[0].schedule, "permanent");

        let response = reqwest::Client::new()
            .post(format!("http://{addr}/services/{service_id}/start"))
            .send()
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::ACCEPTED);
        assert!(matches!(
            service_rx.recv().await.unwrap(),
            ServiceCommand::Start(service) if service == "worker"
        ));
        handle.abort();
    }

    #[tokio::test]
    async fn api_status_does_not_duplicate_running_services_as_orphan_jobs() {
        let temp = tempfile::tempdir().unwrap();
        let mut config = test_config(temp.path().to_path_buf());
        let job_id = config.jobs[0].uuid.unwrap();
        let service = test_service("worker");
        let service_id = service.uuid.unwrap();
        config.services.push(service);
        let mut snapshot = StateSnapshot::new(vec![
            (job_id, "sleepy".to_string()),
            (service_id, "worker".to_string()),
        ]);
        snapshot.upsert(running_job_state(service_id, "worker"));
        let api = test_api(config, snapshot);

        let status = build_status_response(&api).await;

        assert_eq!(status.jobs.len(), 1);
        assert_eq!(status.jobs[0].name, "sleepy");
        assert_eq!(status.services.len(), 1);
        assert_eq!(status.services[0].name, "worker");
        assert!(matches!(status.services[0].status, JobStatus::Running));
    }

    #[tokio::test]
    async fn api_status_returns_absolute_log_paths() {
        let temp = tempfile::tempdir().unwrap();
        let config = test_config(temp.path().to_path_buf());
        let job_id = config.jobs[0].uuid.unwrap();
        let mut snapshot = StateSnapshot::new(vec![(job_id, "sleepy".to_string())]);
        let mut state = running_job_state(job_id, "sleepy");
        state.log_path = Some(PathBuf::from(".sundiald/logs/sleepy.log"));
        snapshot.upsert(state);
        let api = test_api(config, snapshot);

        let status = build_status_response(&api).await;

        let log_path = status.jobs[0].log_path.as_ref().unwrap();
        assert!(log_path.is_absolute());
        assert!(log_path.ends_with(".sundiald/logs/sleepy.log"));
    }

    #[tokio::test]
    async fn api_status_resolves_bare_log_paths_against_configured_log_dir() {
        let temp = tempfile::tempdir().unwrap();
        let config = test_config(temp.path().to_path_buf());
        let expected_log_dir = config.log_dir.clone();
        let job_id = config.jobs[0].uuid.unwrap();
        let mut snapshot = StateSnapshot::new(vec![(job_id, "sleepy".to_string())]);
        let mut state = running_job_state(job_id, "sleepy");
        state.log_path = Some(PathBuf::from("sleepy.log"));
        snapshot.upsert(state);
        let api = test_api(config, snapshot);

        let status = build_status_response(&api).await;

        assert_eq!(
            status.jobs[0].log_path.as_deref(),
            Some(expected_log_dir.join("sleepy.log").as_path())
        );
    }

    #[tokio::test]
    async fn api_exposes_job_history_endpoint_by_uuid() {
        let temp = tempfile::tempdir().unwrap();
        let config = test_config(temp.path().to_path_buf());
        let job = config.jobs[0].clone();
        let job_id = job.uuid.unwrap();
        let snapshot = StateSnapshot::new(vec![(job_id, "sleepy".to_string())]);
        let history = HistoryDb::open(temp.path()).await.unwrap();
        let started_at = Local::now();
        let run_id = history
            .record_triggered(&job, started_at, "manual", &temp.path().join("sleepy.log"))
            .await
            .unwrap();
        history
            .record_finished(
                run_id,
                started_at,
                started_at + chrono::Duration::milliseconds(15),
                Some(0),
                "succeeded",
                None,
                None,
            )
            .await
            .unwrap();
        let mut api = test_api(config, snapshot);
        api.history = history;
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let handle = tokio::spawn(run_api_on_listener(listener, api));

        let history: HistoryResponse = reqwest::Client::new()
            .get(format!("http://{addr}/jobs/{job_id}/history?limit=5"))
            .send()
            .await
            .unwrap()
            .error_for_status()
            .unwrap()
            .json()
            .await
            .unwrap();

        assert_eq!(history.uuid, job_id);
        assert_eq!(history.runs.len(), 1);
        assert_eq!(history.runs[0].status.as_deref(), Some("succeeded"));
        handle.abort();
    }

    #[tokio::test]
    async fn api_exposes_latest_log_endpoint() {
        let temp = tempfile::tempdir().unwrap();
        let config = test_config(temp.path().to_path_buf());
        let job_id = config.jobs[0].uuid.unwrap();
        let log_path = temp
            .path()
            .join("logs")
            .join("sleepy")
            .join("20260711120000.stdout.log");
        let stderr_log_path = log_path.with_file_name("20260711120000.stderr.log");
        tokio::fs::create_dir_all(log_path.parent().unwrap())
            .await
            .unwrap();
        tokio::fs::write(&log_path, "one\ntwo\nthree\n")
            .await
            .unwrap();
        tokio::fs::write(&stderr_log_path, "warn\nerror\n")
            .await
            .unwrap();
        let mut snapshot = StateSnapshot::new(vec![(job_id, "sleepy".to_string())]);
        let mut state = running_job_state(job_id, "sleepy");
        state.log_path = Some(log_path.clone());
        snapshot.upsert(state);
        let api = test_api(config, snapshot);
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let handle = tokio::spawn(run_api_on_listener(listener, api));

        let log: LogResponse = reqwest::Client::new()
            .get(format!("http://{addr}/jobs/{job_id}/logs/latest?tail=2"))
            .send()
            .await
            .unwrap()
            .error_for_status()
            .unwrap()
            .json()
            .await
            .unwrap();

        assert_eq!(log.uuid, job_id);
        assert!(log.log_path.is_absolute());
        assert_eq!(log.content, "... 1 earlier line(s) omitted\ntwo\nthree");
        assert_eq!(
            log.stderr_log_path.as_deref(),
            Some(stderr_log_path.as_path())
        );
        assert_eq!(log.stderr_content.as_deref(), Some("warn\nerror"));
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
            JobControl::Signal {
                kind: SignalKind::Kill,
                expected: false
            }
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
            JobControl::Signal {
                kind: SignalKind::Term,
                expected: false
            }
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
            JobControl::Signal {
                kind: SignalKind::Term,
                expected: false
            }
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
    trigger:
      schedule: "0 0 * * * *"
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
    trigger:
      schedule: "0 0 * * * *"
  - name: second
    command: "true"
    trigger:
      schedule: "0 0 * * * *"
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
    trigger:
      schedule: "0 0 * * * *"
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
    trigger:
      schedule: "0 0 * * * *"
  - name: original
    command: "true"
    trigger:
      schedule: "0 0 * * * *"
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
