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

#[cfg(test)]
pub use api::TriggerStatusResponse;
pub use api::{
    HistoryResponse, JobStatusResponse, LogResponse, ServiceStatusResponse, StatusResponse,
};

use api::ApiState;
use cleanup::cleanup_old_files;
use history::HistoryDb;
use orphan::alert_orphaned_process_groups;
use process::{
    JobCompletion, JobControl, RunTrigger, RunningJob, SignalKind, spawn_tracked_job,
    spawn_tracked_service,
};

use crate::{
    config::{AlertConfig, JobTrigger, ServiceConfig, ServiceSchedule, SundialdConfig},
    state::StateSnapshot,
};

#[derive(Debug)]
pub(crate) enum ServiceCommand {
    Start(String),
    Stop(String),
    Kill(String),
}

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
    let (service_tx, mut service_rx) = mpsc::unbounded_channel();
    let (completion_tx, mut completion_rx) = mpsc::unbounded_channel();
    let api_state = ApiState {
        config: Arc::clone(&config),
        config_path,
        state: Arc::clone(&state),
        pending_manual: Arc::clone(&pending_manual),
        manual_tx,
        service_tx,
        running_controls: Arc::clone(&running_controls),
        history: history.clone(),
    };
    let api_handle = tokio::spawn(api::run_api(api_bind, api_state));

    let mut running: HashMap<Uuid, RunningJob> = HashMap::new();
    let mut running_services: HashMap<Uuid, RunningJob> = HashMap::new();
    let mut fired_seconds: HashSet<(Uuid, DateTime<Local>)> = HashSet::new();
    let mut service_fired_seconds: HashSet<(Uuid, &'static str, DateTime<Local>)> = HashSet::new();
    let mut service_stop_deadlines: HashMap<Uuid, DateTime<Local>> = HashMap::new();
    let mut service_grace_alerted: HashSet<Uuid> = HashSet::new();
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
                handle_manual_request(&current_config, &mut running, history.clone(), Arc::clone(&state), Arc::clone(&pending_manual), Arc::clone(&running_controls), completion_tx.clone(), job_name).await?;
            }
            Some(command) = service_rx.recv() => {
                retain_running(&mut running_services);
                let current_config = config.read().await.clone();
                handle_service_command(&current_config, &mut running_services, &mut service_stop_deadlines, &mut service_grace_alerted, history.clone(), Arc::clone(&state), Arc::clone(&running_controls), command).await;
            }
            Some(completion) = completion_rx.recv() => {
                retain_running(&mut running);
                if completion.success {
                    let current_config = config.read().await.clone();
                    handle_job_completion(&current_config, &mut running, history.clone(), Arc::clone(&state), Arc::clone(&running_controls), completion_tx.clone(), completion).await;
                }
            }
            _ = tick.tick() => {
                retain_running(&mut running);
                retain_running(&mut running_services);
                let current_config = config.read().await.clone();
                let second = current_second(Local::now());
                fired_seconds.retain(|(_, fired_at)| *fired_at >= second - chrono::Duration::hours(24));
                service_fired_seconds.retain(|(_, _, fired_at)| *fired_at >= second - chrono::Duration::hours(24));
                let configured_service_ids = current_config
                    .services
                    .iter()
                    .filter_map(|service| service.uuid)
                    .collect::<HashSet<_>>();
                for (uuid, running_service) in &running_services {
                    if configured_service_ids.contains(uuid) {
                        continue;
                    }
                    let _ = running_service.control_tx.send(JobControl::Signal {
                        kind: SignalKind::Term,
                        expected: true,
                    });
                }

                for job in &current_config.jobs {
                    let Some(uuid) = job.uuid else { continue };
                    if running.contains_key(&uuid) {
                        continue;
                    }
                    match &job.trigger {
                        JobTrigger::Schedule(schedule) => {
                            if !schedule.matches(second) {
                                continue;
                            }
                            if !fired_seconds.insert((uuid, second)) {
                                continue;
                            }
                        }
                        JobTrigger::After(_) | JobTrigger::Manual => continue,
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
                        completion_tx.clone(),
                        true,
                        RunTrigger::Schedule,
                    );
                    running.insert(uuid, running_job);
                }

                for service in &current_config.services {
                    let Some(uuid) = service.uuid else { continue };
                    let ServiceSchedule::Window { start, stop } = &service.schedule else {
                        continue;
                    };
                    if stop.matches(second) && service_fired_seconds.insert((uuid, "stop", second)) {
                        signal_service_stop(service, &mut running_services, &mut service_stop_deadlines, &mut service_grace_alerted, SignalKind::Term, second);
                    }
                    if start.matches(second)
                        && service_fired_seconds.insert((uuid, "start", second))
                        && !running_services.contains_key(&uuid)
                    {
                        let running_service = spawn_service_process(
                            service.clone(),
                            &current_config,
                            history.clone(),
                            Arc::clone(&state),
                            Arc::clone(&running_controls),
                            RunTrigger::ServiceSchedule,
                        );
                        running_services.insert(uuid, running_service);
                        service_stop_deadlines.remove(&uuid);
                        service_grace_alerted.remove(&uuid);
                    }
                }
                check_service_grace_alerts(&current_config, &running_services, &mut service_stop_deadlines, &mut service_grace_alerted, &current_config.alert).await;
            }
            _ = cleanup_tick.tick() => {
                let current_config = config.read().await.clone();
                cleanup_old_files(&current_config.log_dir, current_config.log_retention_days).await;
                cleanup_old_files(&current_config.alert.event_dir, current_config.alert.retention_days).await;
            }
        }
    }

    for (_, running_job) in running {
        let _ = running_job.control_tx.send(JobControl::Signal {
            kind: SignalKind::Term,
            expected: false,
        });
        running_job.handle.abort();
    }
    for (_, running_service) in running_services {
        let _ = running_service.control_tx.send(JobControl::Signal {
            kind: SignalKind::Term,
            expected: true,
        });
        running_service.handle.abort();
    }
    api_handle.abort();

    Ok(())
}

/// `(uuid, name)` pairs for every configured runnable, for `StateSnapshot::new`/
/// `reconcile`. Panics if a runnable lacks an uuid, which should be impossible:
/// the only ways a `SundialdConfig` reaches `run`/`reload` are
/// `load_and_ensure_ids`, which guarantees every job and service has one.
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
        .chain(config.services.iter().map(|service| {
            (
                service.uuid.expect(
                    "service uuid must be assigned before serving (see load_and_ensure_ids)",
                ),
                service.name.clone(),
            )
        }))
        .collect()
}

async fn handle_manual_request(
    config: &SundialdConfig,
    running: &mut HashMap<Uuid, RunningJob>,
    history: HistoryDb,
    state: Arc<Mutex<StateSnapshot>>,
    pending_manual: Arc<Mutex<HashSet<String>>>,
    running_controls: Arc<Mutex<HashMap<Uuid, mpsc::UnboundedSender<JobControl>>>>,
    completion_tx: mpsc::UnboundedSender<JobCompletion>,
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
        completion_tx,
        true,
        RunTrigger::Manual,
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

async fn handle_job_completion(
    config: &SundialdConfig,
    running: &mut HashMap<Uuid, RunningJob>,
    history: HistoryDb,
    state: Arc<Mutex<StateSnapshot>>,
    running_controls: Arc<Mutex<HashMap<Uuid, mpsc::UnboundedSender<JobControl>>>>,
    completion_tx: mpsc::UnboundedSender<JobCompletion>,
    completion: JobCompletion,
) {
    for job in config.jobs.iter().filter(|job| {
        matches!(
            &job.trigger,
            JobTrigger::After(upstream) if upstream == &completion.name
        )
    }) {
        let Some(uuid) = job.uuid else {
            continue;
        };
        if running.contains_key(&uuid) {
            continue;
        }
        let running_job = spawn_tracked_job(
            job.clone(),
            config.log_dir.clone(),
            config.service_log.clone(),
            config.alert.clone(),
            config.state_dir.clone(),
            history.clone(),
            Arc::clone(&state),
            Arc::clone(&running_controls),
            completion_tx.clone(),
            true,
            RunTrigger::Dependency {
                upstream: completion.name.clone(),
            },
        );
        running.insert(uuid, running_job);
    }
}

async fn handle_service_command(
    config: &SundialdConfig,
    running_services: &mut HashMap<Uuid, RunningJob>,
    service_stop_deadlines: &mut HashMap<Uuid, DateTime<Local>>,
    service_grace_alerted: &mut HashSet<Uuid>,
    history: HistoryDb,
    state: Arc<Mutex<StateSnapshot>>,
    running_controls: Arc<Mutex<HashMap<Uuid, mpsc::UnboundedSender<JobControl>>>>,
    command: ServiceCommand,
) {
    let (service_name, action) = match command {
        ServiceCommand::Start(name) => (name, "start"),
        ServiceCommand::Stop(name) => (name, "stop"),
        ServiceCommand::Kill(name) => (name, "kill"),
    };
    let Some(service) = find_service(config, &service_name).cloned() else {
        log_service_event(
            &config.service_log,
            format!(
                "{} service_request_ignored service={} action={} reason=unknown_service",
                Local::now().to_rfc3339(),
                service_name,
                action
            ),
            true,
        )
        .await;
        return;
    };
    let Some(uuid) = service.uuid else {
        return;
    };

    match action {
        "start" => {
            if running_services.contains_key(&uuid) {
                log_service_event(
                    &config.service_log,
                    format!(
                        "{} service_start_rejected service={} reason=already_running",
                        Local::now().to_rfc3339(),
                        service.name
                    ),
                    true,
                )
                .await;
                return;
            }
            let running_service = spawn_service_process(
                service,
                config,
                history,
                state,
                running_controls,
                RunTrigger::ServiceManual,
            );
            running_services.insert(uuid, running_service);
            service_stop_deadlines.remove(&uuid);
            service_grace_alerted.remove(&uuid);
        }
        "stop" => {
            signal_service_stop(
                &service,
                running_services,
                service_stop_deadlines,
                service_grace_alerted,
                SignalKind::Term,
                Local::now(),
            );
        }
        "kill" => {
            signal_service_stop(
                &service,
                running_services,
                service_stop_deadlines,
                service_grace_alerted,
                SignalKind::Kill,
                Local::now(),
            );
        }
        _ => {}
    }
}

fn find_service<'a>(config: &'a SundialdConfig, service: &str) -> Option<&'a ServiceConfig> {
    config.services.iter().find(|candidate| {
        candidate.name == service
            || candidate.uuid.map(|uuid| uuid.to_string()) == Some(service.to_string())
    })
}

fn spawn_service_process(
    service: ServiceConfig,
    config: &SundialdConfig,
    history: HistoryDb,
    state: Arc<Mutex<StateSnapshot>>,
    running_controls: Arc<Mutex<HashMap<Uuid, mpsc::UnboundedSender<JobControl>>>>,
    trigger: RunTrigger,
) -> RunningJob {
    spawn_tracked_service(
        service,
        config.log_dir.clone(),
        config.service_log.clone(),
        config.alert.clone(),
        config.state_dir.clone(),
        history,
        state,
        running_controls,
        true,
        trigger,
    )
}

fn signal_service_stop(
    service: &ServiceConfig,
    running_services: &mut HashMap<Uuid, RunningJob>,
    service_stop_deadlines: &mut HashMap<Uuid, DateTime<Local>>,
    service_grace_alerted: &mut HashSet<Uuid>,
    signal: SignalKind,
    now: DateTime<Local>,
) {
    let Some(uuid) = service.uuid else {
        return;
    };
    let Some(running_service) = running_services.get(&uuid) else {
        return;
    };
    let _ = running_service.control_tx.send(JobControl::Signal {
        kind: signal,
        expected: true,
    });
    let grace = chrono::Duration::from_std(service.stop_grace())
        .unwrap_or_else(|_| chrono::Duration::seconds(30));
    service_stop_deadlines.insert(uuid, now + grace);
    service_grace_alerted.remove(&uuid);
}

async fn check_service_grace_alerts(
    config: &SundialdConfig,
    running_services: &HashMap<Uuid, RunningJob>,
    service_stop_deadlines: &mut HashMap<Uuid, DateTime<Local>>,
    service_grace_alerted: &mut HashSet<Uuid>,
    alert: &AlertConfig,
) {
    let now = Local::now();
    for service in &config.services {
        let Some(uuid) = service.uuid else {
            continue;
        };
        if !running_services.contains_key(&uuid) {
            service_stop_deadlines.remove(&uuid);
            service_grace_alerted.remove(&uuid);
            continue;
        }
        if let ServiceSchedule::Window { start: _, stop: _ } = &service.schedule {
            if !service_is_inside_runtime(service, now) {
                let grace = chrono::Duration::from_std(service.stop_grace())
                    .unwrap_or_else(|_| chrono::Duration::seconds(30));
                service_stop_deadlines.entry(uuid).or_insert(now + grace);
            }
        }
        if service_stop_deadlines
            .get(&uuid)
            .is_some_and(|deadline| now >= *deadline)
            && service_grace_alerted.insert(uuid)
        {
            alert::write_alert(
                alert,
                &service.name,
                "service is still running outside its configured runtime",
            )
            .await;
        }
    }
}

fn service_is_inside_runtime(service: &ServiceConfig, now: DateTime<Local>) -> bool {
    let ServiceSchedule::Window { start, stop } = &service.schedule else {
        return true;
    };
    let mut starts = start.next_runs(now - chrono::Duration::days(366 * 5), 10_000);
    let mut stops = stop.next_runs(now - chrono::Duration::days(366 * 5), 10_000);
    starts.retain(|time| *time <= now);
    stops.retain(|time| *time <= now);
    match (starts.last(), stops.last()) {
        (Some(start), Some(stop)) => start > stop,
        (Some(_), None) => true,
        _ => false,
    }
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{AlertConfig, JobConfig};

    fn manual_job(name: &str) -> JobConfig {
        JobConfig {
            uuid: Some(Uuid::new_v4()),
            name: name.to_string(),
            command: "true".to_string(),
            trigger: JobTrigger::Manual,
            alert_if_running_for_longer_than: None,
            group: None,
            env: HashMap::new(),
            source_path: None,
        }
    }

    #[tokio::test]
    async fn successful_completion_triggers_downstream_job() {
        let temp = tempfile::tempdir().unwrap();
        let log_dir = temp.path().join("logs");
        tokio::fs::create_dir_all(&log_dir).await.unwrap();
        let upstream = manual_job("build");
        let mut downstream = manual_job("deploy");
        downstream.trigger = JobTrigger::After("build".to_string());
        let config = SundialdConfig {
            state_dir: temp.path().to_path_buf(),
            log_dir,
            service_log: temp.path().join("sundiald.log"),
            api_bind: "127.0.0.1:0".parse().unwrap(),
            log_retention_days: 14,
            alert: AlertConfig::default(),
            env: HashMap::new(),
            job_files: Vec::new(),
            jobs: vec![upstream.clone(), downstream.clone()],
            services: Vec::new(),
        };
        let history = HistoryDb::open(temp.path()).await.unwrap();
        let state = Arc::new(Mutex::new(StateSnapshot::new(job_identities(&config))));
        let running_controls = Arc::new(Mutex::new(HashMap::new()));
        let (completion_tx, mut completion_rx) = mpsc::unbounded_channel();
        let mut running = HashMap::new();

        handle_job_completion(
            &config,
            &mut running,
            history,
            Arc::clone(&state),
            running_controls,
            completion_tx,
            JobCompletion {
                name: upstream.name,
                success: true,
            },
        )
        .await;

        assert!(running.contains_key(&downstream.uuid.unwrap()));
        let completion = completion_rx.recv().await.unwrap();
        assert_eq!(completion.name, "deploy");
        assert!(completion.success);
    }
}
