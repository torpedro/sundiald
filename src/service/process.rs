use std::{
    collections::HashMap,
    path::{Path, PathBuf},
    process::Stdio,
    sync::Arc,
};

use anyhow::{Context, Result};
use chrono::Local;
use tokio::{
    fs::OpenOptions,
    io::{AsyncReadExt, AsyncWriteExt},
    process::Command,
    sync::{Mutex, mpsc},
    task::JoinHandle,
    time,
};
use uuid::Uuid;

use super::{alert::write_alert, history::HistoryDb};
use crate::{
    config::{AlertConfig, JobConfig, ServiceConfig},
    state::{JobState, JobStatus, StateSnapshot},
};

#[derive(Debug)]
pub(crate) enum JobControl {
    Signal { kind: SignalKind, expected: bool },
}

#[derive(Debug, Clone)]
pub(crate) enum RunTrigger {
    Schedule,
    Dependency { upstream: String },
    Manual,
    ServiceSchedule,
    ServiceManual,
}

impl RunTrigger {
    pub(crate) fn kind(&self) -> &'static str {
        match self {
            Self::Schedule => "schedule",
            Self::Dependency { .. } => "dependency",
            Self::Manual => "manual",
            Self::ServiceSchedule => "service_schedule",
            Self::ServiceManual => "service_manual",
        }
    }

    fn log_suffix(&self) -> String {
        match self {
            Self::Schedule => String::new(),
            Self::Dependency { upstream } => format!(" trigger=dependency upstream={upstream}"),
            Self::Manual => " trigger=manual".to_string(),
            Self::ServiceSchedule => " trigger=service_schedule".to_string(),
            Self::ServiceManual => " trigger=service_manual".to_string(),
        }
    }
}

#[derive(Debug, Clone, Copy)]
enum ProcessKind {
    Job,
    Service,
}

#[derive(Debug, Clone)]
pub(crate) struct JobCompletion {
    pub(crate) name: String,
    pub(crate) success: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SignalKind {
    Term,
    Kill,
}

impl SignalKind {
    pub(crate) fn name(self) -> &'static str {
        match self {
            Self::Term => "SIGTERM",
            Self::Kill => "SIGKILL",
        }
    }

    #[cfg(unix)]
    fn libc_signal(self) -> libc::c_int {
        match self {
            Self::Term => libc::SIGTERM,
            Self::Kill => libc::SIGKILL,
        }
    }
}

pub(crate) struct RunningJob {
    pub(crate) handle: JoinHandle<()>,
    pub(crate) control_tx: mpsc::UnboundedSender<JobControl>,
}

pub(crate) fn spawn_tracked_job(
    job: JobConfig,
    log_dir: PathBuf,
    service_log: PathBuf,
    alert: AlertConfig,
    state_dir: PathBuf,
    history: HistoryDb,
    state: Arc<Mutex<StateSnapshot>>,
    running_controls: Arc<Mutex<HashMap<Uuid, mpsc::UnboundedSender<JobControl>>>>,
    completion_tx: mpsc::UnboundedSender<JobCompletion>,
    emit_stdout: bool,
    trigger: RunTrigger,
) -> RunningJob {
    let uuid = job
        .uuid
        .expect("job uuid must be assigned before scheduling (see load_and_ensure_ids)");
    let (control_tx, control_rx) = mpsc::unbounded_channel();
    let handle_control_tx = control_tx.clone();
    let handle = tokio::spawn(async move {
        running_controls
            .lock()
            .await
            .insert(uuid, handle_control_tx);
        let completion = run_job(
            job,
            log_dir,
            service_log,
            alert,
            state_dir,
            history,
            state,
            control_rx,
            emit_stdout,
            trigger,
            ProcessKind::Job,
        )
        .await;
        let _ = completion_tx.send(completion);
        running_controls.lock().await.remove(&uuid);
    });

    RunningJob { handle, control_tx }
}

pub(crate) fn spawn_tracked_service(
    service: ServiceConfig,
    log_dir: PathBuf,
    service_log: PathBuf,
    alert: AlertConfig,
    state_dir: PathBuf,
    history: HistoryDb,
    state: Arc<Mutex<StateSnapshot>>,
    running_controls: Arc<Mutex<HashMap<Uuid, mpsc::UnboundedSender<JobControl>>>>,
    emit_stdout: bool,
    trigger: RunTrigger,
) -> RunningJob {
    let uuid = service
        .uuid
        .expect("service uuid must be assigned before scheduling (see load_and_ensure_ids)");
    let job = service.as_job_config();
    let (control_tx, control_rx) = mpsc::unbounded_channel();
    let handle_control_tx = control_tx.clone();
    let handle = tokio::spawn(async move {
        running_controls
            .lock()
            .await
            .insert(uuid, handle_control_tx);
        let _ = run_job(
            job,
            log_dir,
            service_log,
            alert,
            state_dir,
            history,
            state,
            control_rx,
            emit_stdout,
            trigger,
            ProcessKind::Service,
        )
        .await;
        running_controls.lock().await.remove(&uuid);
    });

    RunningJob { handle, control_tx }
}

async fn run_job(
    job: JobConfig,
    log_dir: PathBuf,
    service_log: PathBuf,
    alert: AlertConfig,
    state_dir: PathBuf,
    history: HistoryDb,
    state: Arc<Mutex<StateSnapshot>>,
    control_rx: mpsc::UnboundedReceiver<JobControl>,
    emit_stdout: bool,
    trigger: RunTrigger,
    process_kind: ProcessKind,
) -> JobCompletion {
    match run_job_inner(
        job.clone(),
        log_dir,
        service_log,
        alert.clone(),
        state_dir,
        history,
        state,
        control_rx,
        emit_stdout,
        trigger,
        process_kind,
    )
    .await
    {
        Ok(completion) => completion,
        Err(error) => {
            write_alert(
                &alert,
                &job.name,
                &format!("job runner internal error: {error:#}"),
            )
            .await;
            JobCompletion {
                name: job.name,
                success: false,
            }
        }
    }
}

/// Persists `state` for `job_name`, logging (rather than failing) if the
/// write itself errors. This runs after the child has already been spawned;
/// propagating the error here would abandon the child mid-function before
/// its stdout/stderr are drained and before the wait/signal loop starts,
/// leaving an orphaned, unkillable process behind.
async fn persist_state(state_dir: &Path, state: &Mutex<StateSnapshot>, job_state: JobState) {
    let job_name = job_state.name.clone();
    let snapshot = {
        let mut snapshot = state.lock().await;
        snapshot.upsert(job_state);
        snapshot.clone()
    };
    if let Err(error) = snapshot.save(state_dir).await {
        eprintln!("failed to persist state for job '{job_name}': {error:#}");
    }
}

async fn record_history_finished(
    history: &HistoryDb,
    run_id: Option<i64>,
    job_name: &str,
    started_at: chrono::DateTime<Local>,
    finished_at: chrono::DateTime<Local>,
    exit_code: Option<i32>,
    status: &str,
    terminated_by_signal: Option<&str>,
    error: Option<&str>,
) {
    let Some(run_id) = run_id else {
        return;
    };
    if let Err(error) = history
        .record_finished(
            run_id,
            started_at,
            finished_at,
            exit_code,
            status,
            terminated_by_signal,
            error,
        )
        .await
    {
        eprintln!("failed to record run finish for job '{job_name}': {error:#}");
    }
}

async fn run_job_inner(
    job: JobConfig,
    log_dir: PathBuf,
    service_log: PathBuf,
    alert: AlertConfig,
    state_dir: PathBuf,
    history: HistoryDb,
    state: Arc<Mutex<StateSnapshot>>,
    mut control_rx: mpsc::UnboundedReceiver<JobControl>,
    emit_stdout: bool,
    trigger: RunTrigger,
    process_kind: ProcessKind,
) -> Result<JobCompletion> {
    let uuid = job
        .uuid
        .expect("job uuid must be assigned before running (see load_and_ensure_ids)");
    let started_at = Local::now();
    let job_log_dir = log_dir.join(super::sanitize_name(&job.name));
    let log_stem = format!(
        "{}-{}",
        started_at.format("%Y%m%d%H%M%S%.6f"),
        Uuid::new_v4()
    );
    let stdout_log_path = job_log_dir.join(format!("{log_stem}.stdout.log"));
    let stderr_log_path = job_log_dir.join(format!("{log_stem}.stderr.log"));
    let history_run_id = match history
        .record_triggered(&job, started_at, trigger.kind(), &stdout_log_path)
        .await
    {
        Ok(run_id) => Some(run_id),
        Err(error) => {
            eprintln!(
                "failed to record run trigger for job '{}': {error:#}",
                job.name
            );
            None
        }
    };

    let mut command = Command::new("sh");
    command
        .arg("-c")
        .arg(&job.command)
        .envs(&job.env)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true);
    // Run the shell in its own process group so terminate/kill can signal it
    // and everything it spawned (e.g. `sleep` inside `sh -c "sleep 10"`) at
    // once. Without this, signaling just the shell's pid can kill `sh`
    // while leaving its children running as orphans.
    #[cfg(unix)]
    command.process_group(0);
    let child = command
        .spawn()
        .with_context(|| format!("failed to start job '{}'", job.name));

    let mut child = match child {
        Ok(child) => child,
        Err(error) => {
            let message = error.to_string();
            let finished_at = Local::now();
            persist_state(
                &state_dir,
                &state,
                JobState {
                    uuid,
                    name: job.name.clone(),
                    status: JobStatus::StartFailed,
                    pid: None,
                    started_at: Some(started_at),
                    finished_at: Some(finished_at),
                    exit_code: None,
                    log_path: Some(stdout_log_path),
                    last_error: Some(message.clone()),
                    terminated_by_signal: None,
                },
            )
            .await;
            record_history_finished(
                &history,
                history_run_id,
                &job.name,
                started_at,
                finished_at,
                None,
                "start_failed",
                None,
                Some(&message),
            )
            .await;
            write_alert(&alert, &job.name, &message).await;
            return Ok(JobCompletion {
                name: job.name,
                success: false,
            });
        }
    };

    let pid = child.id();
    persist_state(
        &state_dir,
        &state,
        JobState {
            uuid,
            name: job.name.clone(),
            status: JobStatus::Running,
            pid,
            started_at: Some(started_at),
            finished_at: None,
            exit_code: None,
            log_path: Some(stdout_log_path.clone()),
            last_error: None,
            terminated_by_signal: None,
        },
    )
    .await;
    super::log_service_event(
        &service_log,
        format!(
            "{} job_started job={} pid={} stdout_log_path={} stderr_log_path={}{}",
            started_at.to_rfc3339(),
            job.name,
            pid.map(|pid| pid.to_string())
                .unwrap_or_else(|| "unknown".to_string()),
            stdout_log_path.display(),
            stderr_log_path.display(),
            trigger.log_suffix()
        ),
        emit_stdout,
    )
    .await;

    let mut internal_errors = Vec::new();
    let stdout_task = match child.stdout.take() {
        Some(stdout) => Some((
            "stdout",
            tokio::spawn(copy_stream(stdout, stdout_log_path.clone())),
        )),
        None => {
            internal_errors.push("child stdout was not captured".to_string());
            None
        }
    };
    let stderr_task = match child.stderr.take() {
        Some(stderr) => Some((
            "stderr",
            tokio::spawn(copy_stream_lazy(stderr, stderr_log_path.clone())),
        )),
        None => {
            internal_errors.push("child stderr was not captured".to_string());
            None
        }
    };

    let alert_threshold = job.alert_threshold();
    let mut overrun_alerted = false;
    let mut terminated_by = None;
    let mut expected_stop = false;
    let status_result = loop {
        let overrun_sleep = async {
            match alert_threshold {
                Some(duration) if !overrun_alerted => time::sleep(duration).await,
                _ => std::future::pending::<()>().await,
            }
        };

        tokio::select! {
            status = child.wait() => break status,
            command = control_rx.recv() => {
                if let Some(JobControl::Signal { kind: signal, expected }) = command {
                    terminated_by = Some(signal);
                    expected_stop |= expected;
                    if let Some(pid) = pid {
                        match send_signal(pid, signal) {
                            Ok(()) => {
                                super::log_service_event(
                                    &service_log,
                                    format!(
                                        "{} job_signal_sent job={} pid={} signal={}{}",
                                        Local::now().to_rfc3339(),
                                        job.name,
                                        pid,
                                        signal.name(),
                                        trigger.log_suffix()
                                    ),
                                    emit_stdout,
                                )
                                .await;
                            }
                            Err(error) if process_is_missing(&error) => {}
                            Err(error) => internal_errors.push(format!(
                                "failed to send {} to process group {pid}: {error:#}",
                                signal.name()
                            )),
                        }
                    }
                } else {
                    break child.wait().await;
                }
            }
            _ = overrun_sleep => {
                overrun_alerted = true;
                write_alert(
                    &alert,
                    &job.name,
                    &format!(
                        "still running after {}",
                        job.alert_if_running_for_longer_than
                            .as_deref()
                            .unwrap_or("the configured threshold")
                    ),
                )
                .await;
            }
        }
    };
    let status = match status_result {
        Ok(status) => Some(status),
        Err(error) => {
            internal_errors.push(format!("failed to wait for child process: {error}"));
            if let Some(pid) = pid
                && let Err(error) = send_signal(pid, SignalKind::Kill)
                && !process_is_missing(&error)
            {
                internal_errors.push(format!(
                    "failed to kill process group {pid} after wait error: {error:#}"
                ));
            }
            match time::timeout(std::time::Duration::from_secs(5), child.wait()).await {
                Ok(Ok(status)) => Some(status),
                Ok(Err(error)) => {
                    internal_errors.push(format!("failed to reap child process: {error}"));
                    None
                }
                Err(_) => {
                    internal_errors.push(
                        "timed out reaping child process after forced termination".to_string(),
                    );
                    None
                }
            }
        }
    };

    for stream_task in [stdout_task, stderr_task].into_iter().flatten() {
        let (stream, task) = stream_task;
        match task.await {
            Ok(Ok(())) => {}
            Ok(Err(error)) => {
                internal_errors.push(format!("failed to capture child {stream}: {error:#}"));
            }
            Err(error) => {
                internal_errors.push(format!("child {stream} capture task failed: {error}"));
            }
        }
    }

    let finished_at = Local::now();
    let exit_code = status.as_ref().and_then(|status| status.code());
    let process_success = status.as_ref().is_some_and(|status| status.success());
    let internal_error = (!internal_errors.is_empty()).then(|| internal_errors.join("; "));
    let unexpected_service_exit = matches!(process_kind, ProcessKind::Service) && !expected_stop;
    let expected_service_stop = matches!(process_kind, ProcessKind::Service) && expected_stop;
    let status_kind = if internal_error.is_some() {
        JobStatus::Failed
    } else if expected_service_stop {
        JobStatus::Succeeded
    } else if unexpected_service_exit {
        JobStatus::Failed
    } else if process_success {
        JobStatus::Succeeded
    } else {
        JobStatus::Failed
    };
    let final_success = matches!(status_kind, JobStatus::Succeeded);
    let history_status = status_kind.to_string();
    let history_signal = terminated_by.map(|signal| signal.name());
    let last_error = if let Some(error) = internal_error {
        Some(format!("job runner internal error: {error}"))
    } else if expected_service_stop {
        None
    } else if unexpected_service_exit && process_success {
        Some("service exited unexpectedly".to_string())
    } else if process_success {
        None
    } else if let Some(signal) = history_signal {
        Some(format!("terminated by {signal}"))
    } else {
        Some(format!("non-zero exit status {:?}", exit_code))
    };

    persist_state(
        &state_dir,
        &state,
        JobState {
            uuid,
            name: job.name.clone(),
            status: status_kind,
            pid: None,
            started_at: Some(started_at),
            finished_at: Some(finished_at),
            exit_code,
            log_path: Some(stdout_log_path.clone()),
            last_error: last_error.clone(),
            terminated_by_signal: terminated_by.map(|signal| signal.name().to_string()),
        },
    )
    .await;
    record_history_finished(
        &history,
        history_run_id,
        &job.name,
        started_at,
        finished_at,
        exit_code,
        &history_status,
        history_signal,
        last_error.as_deref(),
    )
    .await;
    super::log_service_event(
        &service_log,
        format!(
            "{} job_finished job={} exit_code={} success={} stdout_log_path={} stderr_log_path={}{}{}",
            finished_at.to_rfc3339(),
            job.name,
            exit_code
                .map(|code| code.to_string())
                .unwrap_or_else(|| "signal".to_string()),
            final_success,
            stdout_log_path.display(),
            stderr_log_path.display(),
            terminated_by
                .map(|signal| format!(" terminated=true signal={}", signal.name()))
                .unwrap_or_default(),
            trigger.log_suffix()
        ),
        emit_stdout,
    )
    .await;

    if let Some(error) = last_error {
        write_alert(&alert, &job.name, &error).await;
    }

    Ok(JobCompletion {
        name: job.name,
        success: final_success,
    })
}

#[cfg(unix)]
fn process_is_missing(error: &anyhow::Error) -> bool {
    error.chain().any(|cause| {
        cause
            .downcast_ref::<std::io::Error>()
            .and_then(std::io::Error::raw_os_error)
            == Some(libc::ESRCH)
    })
}

#[cfg(not(unix))]
fn process_is_missing(_error: &anyhow::Error) -> bool {
    false
}

#[cfg(unix)]
fn send_signal(pid: u32, signal: SignalKind) -> Result<()> {
    // Negative pid targets the whole process group (see process_group(0) at
    // spawn time), so this reaches the shell and anything it spawned, not
    // just the shell itself.
    let result = unsafe { libc::kill(-(pid as libc::pid_t), signal.libc_signal()) };
    if result == 0 {
        Ok(())
    } else {
        Err(std::io::Error::last_os_error())
            .with_context(|| format!("failed to send {} to process group {pid}", signal.name()))
    }
}

#[cfg(not(unix))]
fn send_signal(_pid: u32, signal: SignalKind) -> Result<()> {
    anyhow::bail!("{} is not supported on this platform", signal.name())
}

async fn copy_stream(
    mut stream: impl tokio::io::AsyncRead + Unpin,
    log_path: PathBuf,
) -> Result<()> {
    if let Some(parent) = log_path.parent() {
        tokio::fs::create_dir_all(parent).await?;
    }
    let mut file = OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(&log_path)
        .await
        .with_context(|| format!("failed to open log {}", log_path.display()))?;

    tokio::io::copy(&mut stream, &mut file).await?;
    file.flush().await?;
    Ok(())
}

async fn copy_stream_lazy(
    mut stream: impl tokio::io::AsyncRead + Unpin,
    log_path: PathBuf,
) -> Result<()> {
    let mut first_chunk = [0_u8; 8192];
    let count = stream.read(&mut first_chunk).await?;
    if count == 0 {
        return Ok(());
    }
    if let Some(parent) = log_path.parent() {
        tokio::fs::create_dir_all(parent).await?;
    }
    let mut file = OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(&log_path)
        .await
        .with_context(|| format!("failed to open log {}", log_path.display()))?;
    file.write_all(&first_chunk[..count]).await?;
    tokio::io::copy(&mut stream, &mut file).await?;
    file.flush().await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{AlertConfig, JobTrigger};

    #[tokio::test]
    async fn run_job_inner_sends_exactly_one_alert_when_running_past_threshold() {
        let temp = tempfile::tempdir().unwrap();
        let alert = AlertConfig {
            log: temp.path().join("alerts.log"),
            event_dir: temp.path().join("alerts"),
            retention_days: 0,
            command: None,
            pushover: None,
        };
        let job = JobConfig {
            uuid: Some(Uuid::new_v4()),
            name: "slow".to_string(),
            command: "sleep 1".to_string(),
            trigger: JobTrigger::Manual,
            // Fires the overrun check almost immediately, well before the
            // 1-second job finishes, so this test stays fast.
            alert_if_running_for_longer_than: Some("0s".to_string()),
            group: None,
            env: HashMap::new(),
            source_path: None,
        };
        let history = HistoryDb::open(temp.path()).await.unwrap();
        let state = Arc::new(Mutex::new(StateSnapshot::new(Vec::new())));
        let (_control_tx, control_rx) = mpsc::unbounded_channel();

        run_job_inner(
            job,
            temp.path().join("logs"),
            temp.path().join("sundiald.log"),
            alert.clone(),
            temp.path().to_path_buf(),
            history,
            state,
            control_rx,
            false,
            RunTrigger::Manual,
            ProcessKind::Job,
        )
        .await
        .unwrap();

        let mut entries = tokio::fs::read_dir(&alert.event_dir).await.unwrap();
        let mut count = 0;
        while entries.next_entry().await.unwrap().is_some() {
            count += 1;
        }
        assert_eq!(
            count, 1,
            "expected exactly one overrun alert event, got {count}"
        );
    }

    #[tokio::test]
    async fn run_job_inner_uses_shell_env_expansion_for_command_strings() {
        let temp = tempfile::tempdir().unwrap();
        let alert = AlertConfig {
            log: temp.path().join("alerts.log"),
            event_dir: temp.path().join("alerts"),
            retention_days: 0,
            command: None,
            pushover: None,
        };
        let job_id = Uuid::new_v4();
        let job = JobConfig {
            uuid: Some(job_id),
            name: "env".to_string(),
            command: "SUNDIALD_TEST_VALUE=expanded; printf '%s\\n' \"$SUNDIALD_TEST_VALUE\""
                .to_string(),
            trigger: JobTrigger::Manual,
            alert_if_running_for_longer_than: None,
            group: None,
            env: HashMap::new(),
            source_path: None,
        };
        let history = HistoryDb::open(temp.path()).await.unwrap();
        let log_dir = temp.path().join("logs");
        tokio::fs::create_dir_all(&log_dir).await.unwrap();
        let state = Arc::new(Mutex::new(StateSnapshot::new(Vec::new())));
        let (_control_tx, control_rx) = mpsc::unbounded_channel();

        run_job_inner(
            job,
            log_dir.clone(),
            temp.path().join("sundiald.log"),
            alert,
            temp.path().to_path_buf(),
            history,
            Arc::clone(&state),
            control_rx,
            false,
            RunTrigger::Manual,
            ProcessKind::Job,
        )
        .await
        .unwrap();

        let snapshot = state.lock().await;
        let log_path = snapshot
            .jobs
            .iter()
            .find(|job| job.uuid == job_id)
            .and_then(|job| job.log_path.as_ref())
            .expect("completed job should have a log path");
        let log = tokio::fs::read_to_string(log_path).await.unwrap();
        let stderr_path = log_path.with_file_name(
            log_path
                .file_name()
                .unwrap()
                .to_string_lossy()
                .replace(".stdout.log", ".stderr.log"),
        );

        assert_eq!(log, "expanded\n");
        assert_eq!(log_path.parent(), Some(log_dir.join("env").as_path()));
        assert!(!stderr_path.exists());
    }

    #[tokio::test]
    async fn run_job_inner_passes_group_env_to_command() {
        let temp = tempfile::tempdir().unwrap();
        let alert = AlertConfig {
            log: temp.path().join("alerts.log"),
            event_dir: temp.path().join("alerts"),
            retention_days: 0,
            command: None,
            pushover: None,
        };
        let job_id = Uuid::new_v4();
        let mut env = HashMap::new();
        env.insert("SUNDIALD_GROUP_VALUE".to_string(), "from-group".to_string());
        let job = JobConfig {
            uuid: Some(job_id),
            name: "env-group".to_string(),
            command: "printf '%s\\n' \"$SUNDIALD_GROUP_VALUE\"".to_string(),
            trigger: JobTrigger::Manual,
            alert_if_running_for_longer_than: None,
            group: Some("ops".to_string()),
            env,
            source_path: None,
        };
        let history = HistoryDb::open(temp.path()).await.unwrap();
        let log_dir = temp.path().join("logs");
        tokio::fs::create_dir_all(&log_dir).await.unwrap();
        let state = Arc::new(Mutex::new(StateSnapshot::new(Vec::new())));
        let (_control_tx, control_rx) = mpsc::unbounded_channel();

        run_job_inner(
            job,
            log_dir,
            temp.path().join("sundiald.log"),
            alert,
            temp.path().to_path_buf(),
            history,
            Arc::clone(&state),
            control_rx,
            false,
            RunTrigger::Manual,
            ProcessKind::Job,
        )
        .await
        .unwrap();

        let snapshot = state.lock().await;
        let log_path = snapshot
            .jobs
            .iter()
            .find(|job| job.uuid == job_id)
            .and_then(|job| job.log_path.as_ref())
            .expect("completed job should have a log path");
        let log = tokio::fs::read_to_string(log_path).await.unwrap();

        assert_eq!(log, "from-group\n");
    }

    #[tokio::test]
    async fn run_job_inner_writes_stderr_to_separate_lazy_file() {
        let temp = tempfile::tempdir().unwrap();
        let alert = AlertConfig {
            log: temp.path().join("alerts.log"),
            event_dir: temp.path().join("alerts"),
            retention_days: 0,
            command: None,
            pushover: None,
        };
        let job_id = Uuid::new_v4();
        let job = JobConfig {
            uuid: Some(job_id),
            name: "streams".to_string(),
            command: "printf 'out\\n'; printf 'err\\n' >&2".to_string(),
            trigger: JobTrigger::Manual,
            alert_if_running_for_longer_than: None,
            group: None,
            env: HashMap::new(),
            source_path: None,
        };
        let history = HistoryDb::open(temp.path()).await.unwrap();
        let log_dir = temp.path().join("logs");
        tokio::fs::create_dir_all(&log_dir).await.unwrap();
        let state = Arc::new(Mutex::new(StateSnapshot::new(Vec::new())));
        let (_control_tx, control_rx) = mpsc::unbounded_channel();

        run_job_inner(
            job,
            log_dir,
            temp.path().join("sundiald.log"),
            alert,
            temp.path().to_path_buf(),
            history,
            Arc::clone(&state),
            control_rx,
            false,
            RunTrigger::Manual,
            ProcessKind::Job,
        )
        .await
        .unwrap();

        let snapshot = state.lock().await;
        let stdout_path = snapshot
            .jobs
            .iter()
            .find(|job| job.uuid == job_id)
            .and_then(|job| job.log_path.as_ref())
            .expect("completed job should have a stdout log path");
        let stderr_path = stdout_path.with_file_name(
            stdout_path
                .file_name()
                .unwrap()
                .to_string_lossy()
                .replace(".stdout.log", ".stderr.log"),
        );

        assert_eq!(
            tokio::fs::read_to_string(stdout_path).await.unwrap(),
            "out\n"
        );
        assert_eq!(
            tokio::fs::read_to_string(stderr_path).await.unwrap(),
            "err\n"
        );
    }

    #[tokio::test]
    async fn run_job_inner_preserves_binary_and_partial_stream_output() {
        let temp = tempfile::tempdir().unwrap();
        let alert = AlertConfig {
            log: temp.path().join("alerts.log"),
            event_dir: temp.path().join("alerts"),
            retention_days: 0,
            command: None,
            pushover: None,
        };
        let job_id = Uuid::new_v4();
        let job = JobConfig {
            uuid: Some(job_id),
            name: "binary-streams".to_string(),
            command: "printf '\\377\\000A'; printf '\\376\\000B' >&2".to_string(),
            trigger: JobTrigger::Manual,
            alert_if_running_for_longer_than: None,
            group: None,
            env: HashMap::new(),
            source_path: None,
        };
        let history = HistoryDb::open(temp.path()).await.unwrap();
        let state = Arc::new(Mutex::new(StateSnapshot::new(Vec::new())));
        let (_control_tx, control_rx) = mpsc::unbounded_channel();

        run_job_inner(
            job,
            temp.path().join("logs"),
            temp.path().join("sundiald.log"),
            alert,
            temp.path().to_path_buf(),
            history,
            Arc::clone(&state),
            control_rx,
            false,
            RunTrigger::Manual,
            ProcessKind::Job,
        )
        .await
        .unwrap();

        let snapshot = state.lock().await;
        let stdout_path = snapshot.jobs[0].log_path.as_ref().unwrap();
        let stderr_path = stdout_path.with_file_name(
            stdout_path
                .file_name()
                .unwrap()
                .to_string_lossy()
                .replace(".stdout.log", ".stderr.log"),
        );
        assert_eq!(tokio::fs::read(stdout_path).await.unwrap(), [255, 0, b'A']);
        assert_eq!(tokio::fs::read(stderr_path).await.unwrap(), [254, 0, b'B']);
    }

    #[tokio::test]
    async fn repeated_runs_use_distinct_log_files() {
        let temp = tempfile::tempdir().unwrap();
        let job_id = Uuid::new_v4();
        let job = JobConfig {
            uuid: Some(job_id),
            name: "quick".to_string(),
            command: "printf run".to_string(),
            trigger: JobTrigger::Manual,
            alert_if_running_for_longer_than: None,
            group: None,
            env: HashMap::new(),
            source_path: None,
        };
        let alert = AlertConfig {
            log: temp.path().join("alerts.log"),
            event_dir: temp.path().join("alerts"),
            retention_days: 0,
            command: None,
            pushover: None,
        };
        let history = HistoryDb::open(temp.path()).await.unwrap();
        let state = Arc::new(Mutex::new(StateSnapshot::new(Vec::new())));

        for _ in 0..2 {
            let (_control_tx, control_rx) = mpsc::unbounded_channel();
            run_job_inner(
                job.clone(),
                temp.path().join("logs"),
                temp.path().join("sundiald.log"),
                alert.clone(),
                temp.path().to_path_buf(),
                history.clone(),
                Arc::clone(&state),
                control_rx,
                false,
                RunTrigger::Manual,
                ProcessKind::Job,
            )
            .await
            .unwrap();
        }

        let runs = history.runs_for_job(job_id, 2).await.unwrap();
        assert_eq!(runs.len(), 2);
        assert_ne!(runs[0].log_path, runs[1].log_path);
        assert!(
            runs.iter()
                .all(|run| run.log_path.as_ref().unwrap().exists())
        );
    }

    #[tokio::test]
    async fn log_capture_failure_finalizes_state_and_history_as_failed() {
        let temp = tempfile::tempdir().unwrap();
        let alert = AlertConfig {
            log: temp.path().join("alerts.log"),
            event_dir: temp.path().join("alerts"),
            retention_days: 0,
            command: None,
            pushover: None,
        };
        let job_id = Uuid::new_v4();
        let job = JobConfig {
            uuid: Some(job_id),
            name: "broken-log".to_string(),
            command: "printf 'output\\n'".to_string(),
            trigger: JobTrigger::Manual,
            alert_if_running_for_longer_than: None,
            group: None,
            env: HashMap::new(),
            source_path: None,
        };
        let history = HistoryDb::open(temp.path()).await.unwrap();
        let log_dir = temp.path().join("not-a-directory");
        tokio::fs::write(&log_dir, "blocking file").await.unwrap();
        let state = Arc::new(Mutex::new(StateSnapshot::new(Vec::new())));
        let (_control_tx, control_rx) = mpsc::unbounded_channel();

        let completion = run_job_inner(
            job,
            log_dir,
            temp.path().join("sundiald.log"),
            alert.clone(),
            temp.path().to_path_buf(),
            history.clone(),
            Arc::clone(&state),
            control_rx,
            false,
            RunTrigger::Manual,
            ProcessKind::Job,
        )
        .await
        .unwrap();

        assert!(!completion.success);
        let snapshot = state.lock().await;
        let job_state = snapshot
            .jobs
            .iter()
            .find(|job| job.uuid == job_id)
            .expect("failed job state should be persisted");
        assert!(matches!(job_state.status, JobStatus::Failed));
        assert!(
            job_state
                .last_error
                .as_deref()
                .is_some_and(|error| error.contains("failed to capture child stdout"))
        );
        drop(snapshot);

        let runs = history.runs_for_job(job_id, 1).await.unwrap();
        assert_eq!(runs[0].status.as_deref(), Some("failed"));
        assert!(runs[0].finished_at.is_some());
        assert!(alert.event_dir.exists());
    }

    #[tokio::test]
    async fn service_exit_with_zero_status_is_failed_and_alerted_when_unexpected() {
        let temp = tempfile::tempdir().unwrap();
        let alert = AlertConfig {
            log: temp.path().join("alerts.log"),
            event_dir: temp.path().join("alerts"),
            retention_days: 0,
            command: None,
            pushover: None,
        };
        let job_id = Uuid::new_v4();
        let job = JobConfig {
            uuid: Some(job_id),
            name: "service".to_string(),
            command: "true".to_string(),
            trigger: JobTrigger::Manual,
            alert_if_running_for_longer_than: None,
            group: None,
            env: HashMap::new(),
            source_path: None,
        };
        let history = HistoryDb::open(temp.path()).await.unwrap();
        let state = Arc::new(Mutex::new(StateSnapshot::new(Vec::new())));
        let (_control_tx, control_rx) = mpsc::unbounded_channel();

        run_job_inner(
            job,
            temp.path().join("logs"),
            temp.path().join("sundiald.log"),
            alert.clone(),
            temp.path().to_path_buf(),
            history,
            Arc::clone(&state),
            control_rx,
            false,
            RunTrigger::ServiceManual,
            ProcessKind::Service,
        )
        .await
        .unwrap();

        let snapshot = state.lock().await;
        let service_state = snapshot
            .jobs
            .iter()
            .find(|job| job.uuid == job_id)
            .expect("service state should be persisted");
        assert!(matches!(service_state.status, JobStatus::Failed));
        assert_eq!(
            service_state.last_error.as_deref(),
            Some("service exited unexpectedly")
        );
        let mut entries = tokio::fs::read_dir(&alert.event_dir).await.unwrap();
        assert!(entries.next_entry().await.unwrap().is_some());
    }

    #[tokio::test]
    async fn expected_service_sigterm_records_success_without_alerting() {
        let temp = tempfile::tempdir().unwrap();
        let alert = AlertConfig {
            log: temp.path().join("alerts.log"),
            event_dir: temp.path().join("alerts"),
            retention_days: 0,
            command: None,
            pushover: None,
        };
        let job_id = Uuid::new_v4();
        let job = JobConfig {
            uuid: Some(job_id),
            name: "service-stop".to_string(),
            command: "sleep 5".to_string(),
            trigger: JobTrigger::Manual,
            alert_if_running_for_longer_than: None,
            group: None,
            env: HashMap::new(),
            source_path: None,
        };
        let history = HistoryDb::open(temp.path()).await.unwrap();
        let state = Arc::new(Mutex::new(StateSnapshot::new(Vec::new())));
        let (control_tx, control_rx) = mpsc::unbounded_channel();
        tokio::spawn(async move {
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            let _ = control_tx.send(JobControl::Signal {
                kind: SignalKind::Term,
                expected: true,
            });
        });

        run_job_inner(
            job,
            temp.path().join("logs"),
            temp.path().join("sundiald.log"),
            alert.clone(),
            temp.path().to_path_buf(),
            history,
            Arc::clone(&state),
            control_rx,
            false,
            RunTrigger::ServiceManual,
            ProcessKind::Service,
        )
        .await
        .unwrap();

        let snapshot = state.lock().await;
        let service_state = snapshot
            .jobs
            .iter()
            .find(|job| job.uuid == job_id)
            .expect("service state should be persisted");
        assert!(matches!(service_state.status, JobStatus::Succeeded));
        assert!(service_state.last_error.is_none());
        assert_eq!(
            service_state.terminated_by_signal.as_deref(),
            Some("SIGTERM")
        );
        assert!(!alert.event_dir.exists());
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn run_job_inner_accepts_sigkill_after_ignored_sigterm() {
        let temp = tempfile::tempdir().unwrap();
        let alert = AlertConfig {
            log: temp.path().join("alerts.log"),
            event_dir: temp.path().join("alerts"),
            retention_days: 0,
            command: None,
            pushover: None,
        };
        let job_id = Uuid::new_v4();
        let job = JobConfig {
            uuid: Some(job_id),
            name: "stubborn-service".to_string(),
            command: "trap '' TERM; while true; do sleep 1; done".to_string(),
            trigger: JobTrigger::Manual,
            alert_if_running_for_longer_than: None,
            group: None,
            env: HashMap::new(),
            source_path: None,
        };
        let history = HistoryDb::open(temp.path()).await.unwrap();
        let state = Arc::new(Mutex::new(StateSnapshot::new(Vec::new())));
        let (control_tx, control_rx) = mpsc::unbounded_channel();
        tokio::spawn(async move {
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            let _ = control_tx.send(JobControl::Signal {
                kind: SignalKind::Term,
                expected: true,
            });
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            let _ = control_tx.send(JobControl::Signal {
                kind: SignalKind::Kill,
                expected: true,
            });
        });

        tokio::time::timeout(
            std::time::Duration::from_secs(5),
            run_job_inner(
                job,
                temp.path().join("logs"),
                temp.path().join("sundiald.log"),
                alert.clone(),
                temp.path().to_path_buf(),
                history,
                Arc::clone(&state),
                control_rx,
                false,
                RunTrigger::ServiceManual,
                ProcessKind::Service,
            ),
        )
        .await
        .expect("stubborn service should be killed")
        .unwrap();

        let snapshot = state.lock().await;
        let service_state = snapshot
            .jobs
            .iter()
            .find(|job| job.uuid == job_id)
            .expect("service state should be persisted");
        assert!(matches!(service_state.status, JobStatus::Succeeded));
        assert_eq!(
            service_state.terminated_by_signal.as_deref(),
            Some("SIGKILL")
        );
        assert!(!alert.event_dir.exists());
    }
}
