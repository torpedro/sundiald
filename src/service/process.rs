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
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    process::Command,
    sync::{Mutex, mpsc},
    task::JoinHandle,
    time,
};
use uuid::Uuid;

use super::{alert::write_alert, history::HistoryDb};
use crate::{
    config::{AlertConfig, JobConfig},
    state::{JobState, JobStatus, StateSnapshot},
};

#[derive(Debug)]
pub(crate) enum JobControl {
    Signal(SignalKind),
}

#[derive(Debug, Clone)]
pub(crate) enum RunTrigger {
    Schedule,
    Dependency { upstream: String },
    Manual,
}

impl RunTrigger {
    pub(crate) fn kind(&self) -> &'static str {
        match self {
            Self::Schedule => "schedule",
            Self::Dependency { .. } => "dependency",
            Self::Manual => "manual",
        }
    }

    fn log_suffix(&self) -> String {
        match self {
            Self::Schedule => String::new(),
            Self::Dependency { upstream } => format!(" trigger=dependency upstream={upstream}"),
            Self::Manual => " trigger=manual".to_string(),
        }
    }
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
        )
        .await;
        let _ = completion_tx.send(completion);
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
    let mut snapshot = state.lock().await;
    snapshot.upsert(job_state);
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
) -> Result<JobCompletion> {
    let uuid = job
        .uuid
        .expect("job uuid must be assigned before running (see load_and_ensure_ids)");
    let started_at = Local::now();
    let job_log_dir = log_dir.join(super::sanitize_name(&job.name));
    let log_stem = started_at.format("%Y%m%d%H%M%S").to_string();
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

    let stdout = child
        .stdout
        .take()
        .context("child stdout was not captured")?;
    let stderr = child
        .stderr
        .take()
        .context("child stderr was not captured")?;
    let stdout_task = tokio::spawn(copy_stream(stdout, stdout_log_path.clone()));
    let stderr_task = tokio::spawn(copy_stream_lazy(stderr, stderr_log_path.clone()));

    let alert_threshold = job.alert_threshold();
    let mut overrun_alerted = false;
    let mut terminated_by = None;
    let status = loop {
        let overrun_sleep = async {
            match alert_threshold {
                Some(duration) if !overrun_alerted => time::sleep(duration).await,
                _ => std::future::pending::<()>().await,
            }
        };

        tokio::select! {
            status = child.wait() => break status?,
            command = control_rx.recv() => {
                if let Some(JobControl::Signal(signal)) = command {
                    terminated_by = Some(signal);
                    if let Some(pid) = pid {
                        send_signal(pid, signal)?;
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
                }
                break child.wait().await?;
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
    let _ = stdout_task.await;
    let _ = stderr_task.await;

    let finished_at = Local::now();
    let exit_code = status.code();
    let success = status.success();
    let status_kind = if success {
        JobStatus::Succeeded
    } else {
        JobStatus::Failed
    };
    let history_status = status_kind.to_string();
    let history_signal = terminated_by.map(|signal| signal.name());
    let last_error = if success {
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
            success,
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
        success,
    })
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

async fn copy_stream(stream: impl tokio::io::AsyncRead + Unpin, log_path: PathBuf) -> Result<()> {
    let mut reader = BufReader::new(stream).lines();
    if let Some(parent) = log_path.parent() {
        tokio::fs::create_dir_all(parent).await?;
    }
    let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(&log_path)
        .await
        .with_context(|| format!("failed to open log {}", log_path.display()))?;

    while let Some(line) = reader.next_line().await? {
        file.write_all(format!("{line}\n").as_bytes()).await?;
    }

    Ok(())
}

async fn copy_stream_lazy(
    stream: impl tokio::io::AsyncRead + Unpin,
    log_path: PathBuf,
) -> Result<()> {
    let mut reader = BufReader::new(stream).lines();
    let Some(first_line) = reader.next_line().await? else {
        return Ok(());
    };
    if let Some(parent) = log_path.parent() {
        tokio::fs::create_dir_all(parent).await?;
    }
    let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(&log_path)
        .await
        .with_context(|| format!("failed to open log {}", log_path.display()))?;
    file.write_all(format!("{first_line}\n").as_bytes()).await?;

    while let Some(line) = reader.next_line().await? {
        file.write_all(format!("{line}\n").as_bytes()).await?;
    }

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
}
