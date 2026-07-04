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

use super::alert::write_alert;
use crate::{
    config::{AlertConfig, JobConfig},
    state::{JobState, JobStatus, StateSnapshot},
};

#[derive(Debug)]
pub(crate) enum JobControl {
    Signal(SignalKind),
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
    state: Arc<Mutex<StateSnapshot>>,
    running_controls: Arc<Mutex<HashMap<Uuid, mpsc::UnboundedSender<JobControl>>>>,
    emit_stdout: bool,
    manual: bool,
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
        run_job(
            job,
            log_dir,
            service_log,
            alert,
            state_dir,
            state,
            control_rx,
            emit_stdout,
            manual,
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
    state: Arc<Mutex<StateSnapshot>>,
    control_rx: mpsc::UnboundedReceiver<JobControl>,
    emit_stdout: bool,
    manual: bool,
) {
    if let Err(error) = run_job_inner(
        job.clone(),
        log_dir,
        service_log,
        alert.clone(),
        state_dir,
        state,
        control_rx,
        emit_stdout,
        manual,
    )
    .await
    {
        write_alert(
            &alert,
            &job.name,
            &format!("job runner internal error: {error:#}"),
        )
        .await;
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

async fn run_job_inner(
    job: JobConfig,
    log_dir: PathBuf,
    service_log: PathBuf,
    alert: AlertConfig,
    state_dir: PathBuf,
    state: Arc<Mutex<StateSnapshot>>,
    mut control_rx: mpsc::UnboundedReceiver<JobControl>,
    emit_stdout: bool,
    manual: bool,
) -> Result<()> {
    let uuid = job
        .uuid
        .expect("job uuid must be assigned before running (see load_and_ensure_ids)");
    let started_at = Local::now();
    let log_path = log_dir.join(format!(
        "{}-{}.log",
        super::sanitize_name(&job.name),
        started_at.format("%Y%m%d%H%M%S")
    ));

    let mut command = Command::new("sh");
    command
        .arg("-c")
        .arg(&job.command)
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
            persist_state(
                &state_dir,
                &state,
                JobState {
                    uuid,
                    name: job.name.clone(),
                    status: JobStatus::StartFailed,
                    pid: None,
                    started_at: Some(started_at),
                    finished_at: Some(Local::now()),
                    exit_code: None,
                    log_path: Some(log_path),
                    last_error: Some(message.clone()),
                    terminated_by_signal: None,
                },
            )
            .await;
            write_alert(&alert, &job.name, &message).await;
            return Ok(());
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
            log_path: Some(log_path.clone()),
            last_error: None,
            terminated_by_signal: None,
        },
    )
    .await;
    super::log_service_event(
        &service_log,
        format!(
            "{} job_started job={} pid={} log_path={}{}",
            started_at.to_rfc3339(),
            job.name,
            pid.map(|pid| pid.to_string())
                .unwrap_or_else(|| "unknown".to_string()),
            log_path.display(),
            if manual { " manual=true" } else { "" }
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
    let stdout_log_path = log_path.clone();
    let stderr_log_path = log_path.clone();
    let stdout_task = tokio::spawn(copy_stream("stdout", stdout, stdout_log_path));
    let stderr_task = tokio::spawn(copy_stream("stderr", stderr, stderr_log_path));

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
                                if manual { " manual=true" } else { "" }
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
    let last_error = if success {
        None
    } else if let Some(signal) = terminated_by {
        Some(format!("terminated by {}", signal.name()))
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
            log_path: Some(log_path.clone()),
            last_error: last_error.clone(),
            terminated_by_signal: terminated_by.map(|signal| signal.name().to_string()),
        },
    )
    .await;
    super::log_service_event(
        &service_log,
        format!(
            "{} job_finished job={} exit_code={} success={} log_path={}{}{}",
            finished_at.to_rfc3339(),
            job.name,
            exit_code
                .map(|code| code.to_string())
                .unwrap_or_else(|| "signal".to_string()),
            success,
            log_path.display(),
            terminated_by
                .map(|signal| format!(" terminated=true signal={}", signal.name()))
                .unwrap_or_default(),
            if manual { " manual=true" } else { "" }
        ),
        emit_stdout,
    )
    .await;

    if let Some(error) = last_error {
        write_alert(&alert, &job.name, &error).await;
    }

    Ok(())
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
    label: &'static str,
    stream: impl tokio::io::AsyncRead + Unpin,
    log_path: PathBuf,
) -> Result<()> {
    let mut reader = BufReader::new(stream).lines();
    let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(&log_path)
        .await
        .with_context(|| format!("failed to open log {}", log_path.display()))?;

    while let Some(line) = reader.next_line().await? {
        file.write_all(format!("[{}] {line}\n", label).as_bytes())
            .await?;
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{AlertConfig, Schedule};

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
            schedule: Schedule {
                manual_only: true,
                seconds: vec!["0".to_string()],
                minutes: vec!["*".to_string()],
                hours: vec!["*".to_string()],
                days_of_week: vec!["*".to_string()],
                days_of_month: vec!["*".to_string()],
                months: vec!["*".to_string()],
            },
            // Fires the overrun check almost immediately, well before the
            // 1-second job finishes, so this test stays fast.
            alert_if_running_for_longer_than: Some("0s".to_string()),
        };
        let state = Arc::new(Mutex::new(StateSnapshot::new(Vec::new())));
        let (_control_tx, control_rx) = mpsc::unbounded_channel();

        run_job_inner(
            job,
            temp.path().join("logs"),
            temp.path().join("sundiald.log"),
            alert.clone(),
            temp.path().to_path_buf(),
            state,
            control_rx,
            false,
            false,
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
}
