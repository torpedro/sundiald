mod alert;
mod api;
mod cleanup;
mod history;
mod orphan;
mod process;

use std::{
    collections::{HashMap, HashSet},
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};

use anyhow::{Context, Result};
use chrono::{DateTime, Local, Timelike};
use tokio::{
    fs::{self, OpenOptions},
    io::AsyncWriteExt,
    sync::{Mutex, RwLock, mpsc},
    time,
};
use uuid::Uuid;

pub use api::{JobStatusResponse, StatusResponse};

use api::ApiState;
use cleanup::cleanup_old_files;
use history::HistoryDb;
use orphan::alert_orphaned_process_groups;
use process::{JobControl, RunningJob, SignalKind, spawn_tracked_job};

use crate::{config::SundialdConfig, state::StateSnapshot};

fn current_second(now: DateTime<Local>) -> DateTime<Local> {
    now.with_nanosecond(0).unwrap_or(now)
}

/// Strips a job name down to filesystem-safe characters, for use in log and
/// alert-event file names. Shared by `process` (job log files) and `alert`
/// (alert event files).
pub(crate) fn sanitize_name(name: &str) -> String {
    name.chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() || ch == '-' || ch == '_' {
                ch
            } else {
                '_'
            }
        })
        .collect()
}

/// Appends `line` to `service_log` and, if `emit_stdout`, also prints it.
/// Shared by `process` (job lifecycle events) and this module's own manual-run
/// bookkeeping (ignored/rejected events).
pub(crate) async fn log_service_event(service_log: &PathBuf, line: String, emit_stdout: bool) {
    if emit_stdout {
        println!("{line}");
    }
    let result = async {
        if let Some(parent) = service_log.parent() {
            fs::create_dir_all(parent).await?;
        }
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(service_log)
            .await
            .with_context(|| format!("failed to open service log {}", service_log.display()))?;
        file.write_all(format!("{line}\n").as_bytes()).await?;
        Ok::<_, anyhow::Error>(())
    }
    .await;
    if let Err(error) = result {
        eprintln!("{error:#}");
    }
}

pub async fn run(mut config: SundialdConfig, config_path: PathBuf) -> Result<()> {
    let runtime_base =
        std::env::current_dir().context("failed to resolve sundiald working directory")?;
    config.absolutize_runtime_paths(&runtime_base);
    let config_path = absolutize_path(&runtime_base, &config_path);

    fs::create_dir_all(&config.state_dir).await?;
    fs::create_dir_all(&config.log_dir).await?;
    fs::create_dir_all(&config.alert.event_dir).await?;
    if let Some(parent) = config.service_log.parent() {
        fs::create_dir_all(parent).await?;
    }
    if let Some(parent) = config.alert.log.parent() {
        fs::create_dir_all(parent).await?;
    }

    let api_bind = config.api_bind;
    cleanup_old_files(&config.log_dir, config.log_retention_days).await;
    cleanup_old_files(&config.alert.event_dir, config.alert.retention_days).await;
    let history = HistoryDb::open(&config.state_dir).await?;

    let loaded_snapshot = StateSnapshot::load(&config.state_dir).await?;
    if let Some(snapshot) = &loaded_snapshot {
        alert_orphaned_process_groups(&config.alert, snapshot).await;
    }
    let snapshot = loaded_snapshot
        .map(|snapshot| snapshot.reconcile(job_identities(&config)))
        .unwrap_or_else(|| StateSnapshot::new(job_identities(&config)));
    let state = Arc::new(Mutex::new(snapshot));
    state.lock().await.save(&config.state_dir).await?;

    let job_count = config.jobs.len();
    let config = Arc::new(RwLock::new(config));
    let pending_manual = Arc::new(Mutex::new(HashSet::new()));
    let running_controls = Arc::new(Mutex::new(HashMap::new()));
    let (manual_tx, mut manual_rx) = mpsc::unbounded_channel();
    let api_state = ApiState {
        config: Arc::clone(&config),
        config_path,
        state: Arc::clone(&state),
        pending_manual: Arc::clone(&pending_manual),
        manual_tx,
        running_controls: Arc::clone(&running_controls),
    };
    let api_handle = tokio::spawn(api::run_api(api_bind, api_state));

    let mut running: HashMap<Uuid, RunningJob> = HashMap::new();
    let mut fired_seconds: HashSet<(Uuid, DateTime<Local>)> = HashSet::new();
    let mut tick = time::interval(Duration::from_secs(1));
    let mut cleanup_tick = time::interval(Duration::from_secs(3600));

    eprintln!("sundiald service started with {job_count} job(s)");
    eprintln!("sundiald api listening on http://{api_bind}");

    loop {
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {
                eprintln!("sundiald service stopping");
                break;
            }
            Some(job_name) = manual_rx.recv() => {
                retain_running(&mut running);
                let current_config = config.read().await.clone();
                handle_manual_request(&current_config, &mut running, history.clone(), Arc::clone(&state), Arc::clone(&pending_manual), Arc::clone(&running_controls), job_name).await?;
            }
            _ = tick.tick() => {
                retain_running(&mut running);
                let current_config = config.read().await.clone();
                let second = current_second(Local::now());
                fired_seconds.retain(|(_, fired_at)| *fired_at >= second - chrono::Duration::hours(24));

                for job in &current_config.jobs {
                    let Some(uuid) = job.uuid else { continue };
                    if running.contains_key(&uuid) {
                        continue;
                    }
                    if !job.schedule.matches(second) {
                        continue;
                    }
                    if !fired_seconds.insert((uuid, second)) {
                        continue;
                    }

                    let running_job = spawn_tracked_job(
                        job.clone(),
                        current_config.log_dir.clone(),
                        current_config.service_log.clone(),
                        current_config.alert.clone(),
                        current_config.state_dir.clone(),
                        history.clone(),
                        Arc::clone(&state),
                        Arc::clone(&running_controls),
                        true,
                        false,
                    );
                    running.insert(uuid, running_job);
                }
            }
            _ = cleanup_tick.tick() => {
                let current_config = config.read().await.clone();
                cleanup_old_files(&current_config.log_dir, current_config.log_retention_days).await;
                cleanup_old_files(&current_config.alert.event_dir, current_config.alert.retention_days).await;
            }
        }
    }

    for (_, running_job) in running {
        let _ = running_job
            .control_tx
            .send(JobControl::Signal(SignalKind::Term));
        running_job.handle.abort();
    }
    api_handle.abort();

    Ok(())
}

/// `(uuid, name)` pairs for every configured job, for `StateSnapshot::new`/
/// `reconcile`. Panics if a job lacks an uuid, which should be impossible: the
/// only ways a `SundialdConfig` reaches `run`/`reload` are `load_and_ensure_ids`,
/// which guarantees every job has one.
fn job_identities(config: &SundialdConfig) -> Vec<(Uuid, String)> {
    config
        .jobs
        .iter()
        .map(|job| {
            (
                job.uuid
                    .expect("job uuid must be assigned before serving (see load_and_ensure_ids)"),
                job.name.clone(),
            )
        })
        .collect()
}

async fn handle_manual_request(
    config: &SundialdConfig,
    running: &mut HashMap<Uuid, RunningJob>,
    history: HistoryDb,
    state: Arc<Mutex<StateSnapshot>>,
    pending_manual: Arc<Mutex<HashSet<String>>>,
    running_controls: Arc<Mutex<HashMap<Uuid, mpsc::UnboundedSender<JobControl>>>>,
    job_name: String,
) -> Result<()> {
    let Some(job) = config.jobs.iter().find(|job| job.name == job_name).cloned() else {
        pending_manual.lock().await.remove(&job_name);
        log_service_event(
            &config.service_log,
            format!(
                "{} manual_request_ignored job={} reason=unknown_job",
                Local::now().to_rfc3339(),
                job_name
            ),
            true,
        )
        .await;
        return Ok(());
    };

    let Some(uuid) = job.uuid else {
        pending_manual.lock().await.remove(&job_name);
        log_service_event(
            &config.service_log,
            format!(
                "{} manual_request_ignored job={} reason=missing_id",
                Local::now().to_rfc3339(),
                job_name
            ),
            true,
        )
        .await;
        return Ok(());
    };

    if running.contains_key(&uuid) {
        pending_manual.lock().await.remove(&job_name);
        log_service_event(
            &config.service_log,
            format!(
                "{} manual_request_rejected job={} reason=already_running",
                Local::now().to_rfc3339(),
                job.name
            ),
            true,
        )
        .await;
        return Ok(());
    }

    let mut running_job = spawn_tracked_job(
        job,
        config.log_dir.clone(),
        config.service_log.clone(),
        config.alert.clone(),
        config.state_dir.clone(),
        history,
        state,
        running_controls,
        true,
        true,
    );
    let pending = Arc::clone(&pending_manual);
    let original_handle = running_job.handle;
    running_job.handle = tokio::spawn(async move {
        let _ = original_handle.await;
        pending.lock().await.remove(&job_name);
    });
    running.insert(uuid, running_job);
    Ok(())
}

fn retain_running(running: &mut HashMap<Uuid, RunningJob>) {
    running.retain(|_, running_job| !running_job.handle.is_finished());
}

fn absolutize_path(base: &Path, path: &Path) -> PathBuf {
    if path.is_absolute() {
        path.to_path_buf()
    } else {
        base.join(path)
    }
}
