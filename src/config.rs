mod duration;
mod schedule;
mod uuid_patch;

use std::{
    collections::{HashMap, HashSet},
    env, fs,
    net::SocketAddr,
    path::{Path, PathBuf},
};

use anyhow::{Context, Result, bail};
use serde::{Deserialize, Deserializer, Serialize};
use uuid::Uuid;

pub use schedule::Schedule;

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SundialdConfig {
    #[serde(default = "default_state_dir")]
    pub state_dir: PathBuf,
    #[serde(default = "default_log_dir")]
    pub log_dir: PathBuf,
    #[serde(default = "default_service_log")]
    pub service_log: PathBuf,
    #[serde(default = "default_api_bind")]
    pub api_bind: SocketAddr,
    #[serde(default)]
    pub api_token: Option<String>,
    #[serde(default)]
    pub missed_run_policy: MissedRunPolicy,
    #[serde(default = "default_log_retention_days")]
    pub log_retention_days: u32,
    #[serde(default = "default_shutdown_grace_period")]
    pub shutdown_grace_period: String,
    #[serde(default)]
    pub alert: AlertConfig,
    #[serde(default)]
    pub env: HashMap<String, String>,
    #[serde(default)]
    pub job_files: Vec<JobFileConfig>,
    #[serde(default)]
    pub jobs: Vec<JobConfig>,
    #[serde(default)]
    pub services: Vec<ServiceConfig>,
}

#[derive(Debug, Clone, Copy, Default, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum MissedRunPolicy {
    #[default]
    Skip,
    RunOnce,
}

impl std::fmt::Display for MissedRunPolicy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Skip => write!(f, "skip"),
            Self::RunOnce => write!(f, "run_once"),
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct JobFileConfig {
    pub name: String,
    pub path: PathBuf,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct JobFileContents {
    #[serde(default)]
    pub env: HashMap<String, String>,
    #[serde(default)]
    pub jobs: Vec<JobConfig>,
    #[serde(default)]
    pub services: Vec<ServiceConfig>,
}

impl JobFileContents {
    fn into_parts(self) -> (HashMap<String, String>, Vec<JobConfig>, Vec<ServiceConfig>) {
        (self.env, self.jobs, self.services)
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AlertConfig {
    #[serde(default = "default_alert_log")]
    pub log: PathBuf,
    #[serde(default = "default_alert_event_dir")]
    pub event_dir: PathBuf,
    #[serde(default = "default_alert_retention_days")]
    pub retention_days: u32,
    #[serde(default)]
    pub command: Option<AlertCommandConfig>,
    #[serde(default)]
    pub pushover: Option<PushoverConfig>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AlertCommandConfig {
    pub program: String,
    #[serde(default)]
    pub args: Vec<String>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct PushoverConfig {
    pub token: String,
    pub user: String,
    #[serde(default)]
    pub device: Option<String>,
    #[serde(default)]
    pub title: Option<String>,
    #[serde(default)]
    pub priority: Option<i8>,
    #[serde(default)]
    pub sound: Option<String>,
    #[serde(default)]
    pub url: Option<String>,
    #[serde(default)]
    pub url_title: Option<String>,
    #[serde(default)]
    pub ttl: Option<u32>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct JobConfig {
    /// Stable identity used to track a job across renames. Absent for a
    /// hand-written or freshly generated config; `load_and_ensure_ids` fills
    /// in and persists a fresh one for any job missing it.
    #[serde(default)]
    pub uuid: Option<Uuid>,
    pub name: String,
    pub command: String,
    pub trigger: JobTrigger,
    /// If a run is still active this long after it started, sundiald fires a
    /// "still running" alert (once per run) through the normal alert
    /// channels. A duration like `"45s"`, `"10m"`, `"2h"`, `"1d"`, or a
    /// compound value like `"1h30m"`.
    #[serde(default)]
    pub alert_if_running_for_longer_than: Option<String>,
    /// Display grouping for jobs loaded from a named external job file.
    #[serde(skip)]
    pub group: Option<String>,
    /// Environment inherited from the named external job file group.
    #[serde(skip)]
    pub env: HashMap<String, String>,
    /// YAML file this job came from, used to persist generated UUIDs back to
    /// the right source file without re-serializing unrelated config.
    #[serde(skip)]
    pub source_path: Option<PathBuf>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ServiceConfig {
    #[serde(default)]
    pub uuid: Option<Uuid>,
    pub name: String,
    pub command: String,
    pub schedule: ServiceSchedule,
    #[serde(default)]
    pub stop_grace_period: Option<String>,
    #[serde(skip)]
    pub group: Option<String>,
    #[serde(skip)]
    pub env: HashMap<String, String>,
    #[serde(skip)]
    pub source_path: Option<PathBuf>,
}

#[derive(Debug, Clone)]
pub enum ServiceSchedule {
    Permanent,
    Window { start: Schedule, stop: Schedule },
}

impl<'de> Deserialize<'de> for ServiceSchedule {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        use serde::de::Error;

        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Window {
            start: Schedule,
            stop: Schedule,
        }

        let value = serde_yaml::Value::deserialize(deserializer)?;
        match value {
            serde_yaml::Value::String(value) if value == "permanent" => Ok(Self::Permanent),
            serde_yaml::Value::Mapping(_) => {
                let window: Window = serde_yaml::from_value(value)
                    .map_err(|error| D::Error::custom(error.to_string()))?;
                Ok(Self::Window {
                    start: window.start,
                    stop: window.stop,
                })
            }
            _ => Err(D::Error::custom(
                "service schedule must be 'permanent' or a map containing start and stop",
            )),
        }
    }
}

impl ServiceConfig {
    pub fn stop_grace(&self) -> std::time::Duration {
        self.stop_grace_period
            .as_deref()
            .and_then(|value| duration::parse_duration(value).ok())
            .unwrap_or_else(|| std::time::Duration::from_secs(30))
    }

    pub fn as_job_config(&self) -> JobConfig {
        JobConfig {
            uuid: self.uuid,
            name: self.name.clone(),
            command: self.command.clone(),
            trigger: JobTrigger::Manual,
            alert_if_running_for_longer_than: None,
            group: self.group.clone(),
            env: self.env.clone(),
            source_path: self.source_path.clone(),
        }
    }
}

#[derive(Debug, Clone)]
pub enum JobTrigger {
    Schedule(Schedule),
    After(String),
    Manual,
}

impl JobTrigger {
    pub fn kind(&self) -> &'static str {
        match self {
            Self::Schedule(_) => "schedule",
            Self::After(_) => "dependency",
            Self::Manual => "manual",
        }
    }

    pub fn after(&self) -> Option<&str> {
        match self {
            Self::After(job) => Some(job),
            _ => None,
        }
    }

    pub fn schedule(&self) -> Option<&Schedule> {
        match self {
            Self::Schedule(schedule) => Some(schedule),
            _ => None,
        }
    }
}

impl<'de> Deserialize<'de> for JobTrigger {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        use serde::de::Error;

        let value = serde_yaml::Value::deserialize(deserializer)?;
        match value {
            serde_yaml::Value::String(value) if value == "manual" => Ok(Self::Manual),
            serde_yaml::Value::Mapping(mapping) => {
                let mut schedule = None;
                let mut after = None;
                for (key, value) in mapping {
                    let Some(key) = key.as_str() else {
                        return Err(D::Error::custom("trigger keys must be strings"));
                    };
                    match key {
                        "schedule" => {
                            if schedule.is_some() {
                                return Err(D::Error::custom("trigger.schedule is duplicated"));
                            }
                            schedule = Some(
                                serde_yaml::from_value(value)
                                    .map_err(|error| D::Error::custom(error.to_string()))?,
                            );
                        }
                        "after" => {
                            if after.is_some() {
                                return Err(D::Error::custom("trigger.after is duplicated"));
                            }
                            after = Some(
                                serde_yaml::from_value(value)
                                    .map_err(|error| D::Error::custom(error.to_string()))?,
                            );
                        }
                        other => {
                            return Err(D::Error::custom(format!("unknown trigger key '{other}'")));
                        }
                    }
                }
                match (schedule, after) {
                    (Some(schedule), None) => Ok(Self::Schedule(schedule)),
                    (None, Some(after)) => Ok(Self::After(after)),
                    (None, None) => Err(D::Error::custom(
                        "trigger must contain exactly one of schedule or after, or be 'manual'",
                    )),
                    (Some(_), Some(_)) => Err(D::Error::custom(
                        "trigger must contain exactly one of schedule or after",
                    )),
                }
            }
            _ => Err(D::Error::custom(
                "trigger must be 'manual' or a map containing schedule or after",
            )),
        }
    }
}

impl JobConfig {
    /// Parses `alert_if_running_for_longer_than`, if set. `validate()`
    /// already guarantees this parses successfully for any config that's
    /// been loaded, so a parse failure here (only reachable if this is
    /// called on a config that was never validated) is treated as "no
    /// threshold" rather than propagating an error into the scheduler.
    pub fn alert_threshold(&self) -> Option<std::time::Duration> {
        self.alert_if_running_for_longer_than
            .as_deref()
            .and_then(|value| duration::parse_duration(value).ok())
    }
}

impl SundialdConfig {
    pub fn load(path: &PathBuf) -> Result<Self> {
        let raw = fs::read_to_string(path)
            .with_context(|| format!("failed to read config {}", path.display()))?;
        let mut config: Self = serde_yaml::from_str(&raw)
            .with_context(|| format!("failed to parse YAML config {}", path.display()))?;
        config.load_job_files(path)?;
        config.validate()?;
        Ok(config)
    }

    fn load_job_files(&mut self, config_path: &Path) -> Result<()> {
        for job in &mut self.jobs {
            job.env = self.env.clone();
            job.source_path = Some(config_path.to_path_buf());
        }
        for service in &mut self.services {
            service.env = self.env.clone();
            service.source_path = Some(config_path.to_path_buf());
        }

        let config_dir = config_path.parent().unwrap_or_else(|| Path::new("."));
        for job_file in &self.job_files {
            let path = resolve_path(config_dir, &job_file.path);
            let raw = fs::read_to_string(&path)
                .with_context(|| format!("failed to read job file {}", path.display()))?;
            let contents: JobFileContents = serde_yaml::from_str(&raw)
                .with_context(|| format!("failed to parse job file {}", path.display()))?;
            let (env, mut jobs, mut services) = contents.into_parts();
            validate_env(
                &env,
                &format!("job file '{}' ({})", job_file.name, path.display()),
            )?;
            for job in &mut jobs {
                job.group = Some(job_file.name.clone());
                job.env = env.clone();
                job.source_path = Some(path.clone());
            }
            self.jobs.extend(jobs);
            for service in &mut services {
                service.group = Some(job_file.name.clone());
                service.env = env.clone();
                service.source_path = Some(path.clone());
            }
            self.services.extend(services);
        }
        Ok(())
    }

    pub fn validate(&self) -> Result<()> {
        duration::parse_duration(&self.shutdown_grace_period)
            .context("invalid shutdown_grace_period")?;

        if let Some(token) = &self.api_token
            && token.trim().is_empty()
        {
            bail!("api_token cannot be empty");
        }
        if !self.api_bind.ip().is_loopback() && self.api_token.is_none() {
            bail!("api_token is required when api_bind is not a loopback address");
        }

        if let Some(command) = &self.alert.command {
            if command.program.trim().is_empty() {
                bail!("alert.command.program cannot be empty");
            }
        }
        if let Some(pushover) = &self.alert.pushover {
            if pushover.token.trim().is_empty() {
                bail!("alert.pushover.token cannot be empty");
            }
            if pushover.user.trim().is_empty() {
                bail!("alert.pushover.user cannot be empty");
            }
            if let Some(priority) = pushover.priority {
                if !(-2..=2).contains(&priority) {
                    bail!("alert.pushover.priority must be between -2 and 2");
                }
            }
        }

        validate_env(&self.env, "root config")?;

        let mut job_file_names = HashSet::new();
        for job_file in &self.job_files {
            if job_file.name.trim().is_empty() {
                bail!("job_files.name cannot be empty");
            }
            if !job_file_names.insert(job_file.name.clone()) {
                bail!("duplicate job_files name '{}'", job_file.name);
            }
            if job_file.path.as_os_str().is_empty() {
                bail!("job_files '{}' path cannot be empty", job_file.name);
            }
        }

        let mut names = HashSet::new();
        let mut job_names = HashSet::new();
        let mut uuids = HashSet::new();
        for job in &self.jobs {
            let job_context = job_context(job);
            if job.name.trim().is_empty() {
                bail!("job name cannot be empty ({job_context})");
            }
            if !names.insert(job.name.clone()) {
                bail!("duplicate job name '{}' ({job_context})", job.name);
            }
            job_names.insert(job.name.clone());
            if let Some(uuid) = job.uuid {
                if !uuids.insert(uuid) {
                    bail!("duplicate job uuid '{uuid}' ({job_context})");
                }
            }
            if job.command.trim().is_empty() {
                bail!("command cannot be empty ({job_context})");
            }
            if let Some(duration) = &job.alert_if_running_for_longer_than {
                duration::parse_duration(duration).with_context(|| {
                    format!("invalid alert_if_running_for_longer_than ({job_context})")
                })?;
            }
            if let JobTrigger::Schedule(schedule) = &job.trigger {
                schedule
                    .validate()
                    .with_context(|| format!("invalid schedule ({job_context})"))?;
            }
        }

        for service in &self.services {
            let service_context = service_context(service);
            if service.name.trim().is_empty() {
                bail!("service name cannot be empty ({service_context})");
            }
            if !names.insert(service.name.clone()) {
                bail!(
                    "duplicate service name '{}' ({service_context})",
                    service.name
                );
            }
            if let Some(uuid) = service.uuid {
                if !uuids.insert(uuid) {
                    bail!("duplicate service uuid '{uuid}' ({service_context})");
                }
            }
            if service.command.trim().is_empty() {
                bail!("service command cannot be empty ({service_context})");
            }
            if let Some(duration) = &service.stop_grace_period {
                duration::parse_duration(duration)
                    .with_context(|| format!("invalid stop_grace_period ({service_context})"))?;
            }
            match &service.schedule {
                ServiceSchedule::Permanent => {}
                ServiceSchedule::Window { start, stop } => {
                    start.validate().with_context(|| {
                        format!("invalid service start schedule ({service_context})")
                    })?;
                    stop.validate().with_context(|| {
                        format!("invalid service stop schedule ({service_context})")
                    })?;
                }
            }
        }

        for job in &self.jobs {
            let job_context = job_context(job);
            if let JobTrigger::After(upstream) = &job.trigger {
                if upstream.trim().is_empty() {
                    bail!("trigger.after cannot be empty ({job_context})");
                }
                if !job_names.contains(upstream) {
                    bail!("unknown trigger.after job '{upstream}' ({job_context})");
                }
            }
        }
        validate_dependency_cycles(&self.jobs)?;
        Ok(())
    }

    pub fn absolutize_runtime_paths(&mut self, base: &Path) {
        self.state_dir = resolve_path(base, &self.state_dir);
        self.log_dir = resolve_path(base, &self.log_dir);
        self.service_log = resolve_path(base, &self.service_log);
        self.alert.log = resolve_path(base, &self.alert.log);
        self.alert.event_dir = resolve_path(base, &self.alert.event_dir);
    }

    pub fn shutdown_grace(&self) -> std::time::Duration {
        duration::parse_duration(&self.shutdown_grace_period)
            .unwrap_or_else(|_| std::time::Duration::from_secs(30))
    }

    /// Like `load`, but also assigns a fresh UUID to any job missing one and
    /// persists it back to the YAML file that defined that job before
    /// returning — a minimal, targeted text patch that inserts `uuid: <uuid>` lines rather than
    /// re-serializing the whole file, so hand-written comments and
    /// formatting elsewhere in the config survive untouched.
    pub fn load_and_ensure_ids(path: &PathBuf) -> Result<Self> {
        let mut config = Self::load(path)?;

        let missing: Vec<(PathBuf, String, Uuid)> = config
            .jobs
            .iter()
            .filter(|job| job.uuid.is_none())
            .map(|job| {
                (
                    job.source_path.clone().unwrap_or_else(|| path.clone()),
                    job.name.clone(),
                    Uuid::new_v4(),
                )
            })
            .chain(
                config
                    .services
                    .iter()
                    .filter(|service| service.uuid.is_none())
                    .map(|service| {
                        (
                            service.source_path.clone().unwrap_or_else(|| path.clone()),
                            service.name.clone(),
                            Uuid::new_v4(),
                        )
                    }),
            )
            .collect();
        if missing.is_empty() {
            return Ok(config);
        }

        let mut missing_by_path: HashMap<PathBuf, Vec<(String, Uuid)>> = HashMap::new();
        for (source_path, name, uuid) in &missing {
            missing_by_path
                .entry(source_path.clone())
                .or_default()
                .push((name.clone(), *uuid));
        }

        for (source_path, missing_jobs) in missing_by_path {
            let raw = fs::read_to_string(&source_path)
                .with_context(|| format!("failed to read config {}", source_path.display()))?;
            let patched =
                uuid_patch::insert_missing_job_uuids(&raw, &missing_jobs).with_context(|| {
                    format!(
                        "failed to persist generated job uuids into {}",
                        source_path.display()
                    )
                })?;
            fs::write(&source_path, &patched).with_context(|| {
                format!("failed to persist job uuids to {}", source_path.display())
            })?;
        }

        let assigned: HashMap<&str, Uuid> = missing
            .iter()
            .map(|(_, name, uuid)| (name.as_str(), *uuid))
            .collect();
        for job in &mut config.jobs {
            if job.uuid.is_none() {
                job.uuid = assigned.get(job.name.as_str()).copied();
            }
        }
        for service in &mut config.services {
            if service.uuid.is_none() {
                service.uuid = assigned.get(service.name.as_str()).copied();
            }
        }

        Ok(config)
    }
}

fn validate_dependency_cycles(jobs: &[JobConfig]) -> Result<()> {
    let dependencies = jobs
        .iter()
        .filter_map(|job| {
            job.trigger
                .after()
                .map(|upstream| (job.name.clone(), upstream.to_string()))
        })
        .collect::<HashMap<_, _>>();
    let mut colors = HashMap::<String, u8>::new();
    let mut path = Vec::new();

    for job in jobs {
        visit_dependency(&job.name, &dependencies, &mut colors, &mut path)?;
    }
    Ok(())
}

fn visit_dependency(
    name: &str,
    dependencies: &HashMap<String, String>,
    colors: &mut HashMap<String, u8>,
    path: &mut Vec<String>,
) -> Result<()> {
    match colors.get(name).copied() {
        Some(2) => return Ok(()),
        Some(1) => {
            let start = path.iter().position(|entry| entry == name).unwrap_or(0);
            let mut cycle = path[start..].to_vec();
            cycle.push(name.to_string());
            bail!("dependency cycle detected: {}", cycle.join(" -> "));
        }
        _ => {}
    }

    colors.insert(name.to_string(), 1);
    path.push(name.to_string());
    if let Some(upstream) = dependencies.get(name) {
        visit_dependency(upstream, dependencies, colors, path)?;
    }
    path.pop();
    colors.insert(name.to_string(), 2);
    Ok(())
}

fn validate_env(env: &HashMap<String, String>, context: &str) -> Result<()> {
    for key in env.keys() {
        if key.trim().is_empty() {
            bail!("{context} env contains an empty key");
        }
        if key.contains('=') {
            bail!("{context} env key '{key}' cannot contain '='");
        }
    }
    Ok(())
}

fn resolve_path(base: &Path, path: &Path) -> PathBuf {
    if path.is_absolute() {
        path.to_path_buf()
    } else {
        base.join(path)
    }
}

fn job_context(job: &JobConfig) -> String {
    let mut context = format!("job '{}'", job.name);
    if let Some(group) = &job.group {
        context.push_str(&format!(", group '{group}'"));
    }
    if let Some(source_path) = &job.source_path {
        context.push_str(&format!(", file {}", source_path.display()));
    }
    context
}

fn service_context(service: &ServiceConfig) -> String {
    let mut context = format!("service '{}'", service.name);
    if let Some(group) = &service.group {
        context.push_str(&format!(", group '{group}'"));
    }
    if let Some(source_path) = &service.source_path {
        context.push_str(&format!(", file {}", source_path.display()));
    }
    context
}

fn default_state_dir() -> PathBuf {
    default_user_state_dir()
}

fn default_log_dir() -> PathBuf {
    default_user_state_dir().join("logs")
}

fn default_service_log() -> PathBuf {
    default_user_state_dir().join("sundiald.log")
}

fn default_api_bind() -> SocketAddr {
    "127.0.0.1:8787"
        .parse()
        .expect("default api bind address is valid")
}

fn default_alert_log() -> PathBuf {
    default_user_state_dir().join("alerts.log")
}

fn default_alert_event_dir() -> PathBuf {
    default_user_state_dir().join("alerts")
}

fn default_log_retention_days() -> u32 {
    14
}

fn default_shutdown_grace_period() -> String {
    "30s".to_string()
}

fn default_alert_retention_days() -> u32 {
    90
}

impl Default for AlertConfig {
    fn default() -> Self {
        Self {
            log: default_alert_log(),
            event_dir: default_alert_event_dir(),
            retention_days: default_alert_retention_days(),
            command: None,
            pushover: None,
        }
    }
}

fn default_user_state_dir() -> PathBuf {
    env::var_os("HOME")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("~"))
        .join(".local/state/sundiald")
}

pub fn sample_config() -> String {
    format!(
        r#"state_dir: {state_dir}
log_dir: {log_dir}
service_log: {service_log}
api_bind: 127.0.0.1:8787
# Required when api_bind is not a loopback address. CLI commands send it as a bearer token.
# api_token: "replace-with-a-long-random-secret"
# What to do when the daemon misses scheduled seconds while suspended or busy:
# `skip` ignores them; `run_once` runs each affected job once after recovery.
missed_run_policy: skip
# Delete job log files older than this many days. Set to 0 to keep logs forever.
log_retention_days: 14
# Time to wait for running processes to exit after SIGTERM during daemon shutdown.
shutdown_grace_period: "30s"
alert:
  log: {alert_log}
  event_dir: {alert_event_dir}
  # Delete alert event JSON files older than this many days. Set to 0 to keep forever.
  retention_days: 90
  # Optional command run when a job fails. No environment variables are used.
  # Placeholders available in args: {{job}}, {{message}}, {{alert_file}}
  # command:
  #   program: /usr/local/bin/sundiald-alert
  #   args: ["--event", "{{alert_file}}"]
  # Optional Pushover output. Credentials are read from this config file.
  # pushover:
  #   token: "your-pushover-application-token"
  #   user: "your-pushover-user-or-group-key"
  #   title: "sundiald"
  #   priority: 0
# Environment variables inherited by inline jobs and services in this file.
env:
  APP_ENV: production
# Optional named files containing additional job and service definitions.
# Each file is a YAML map with optional `env`, `jobs`, and `services` lists.
# job_files:
#   - name: maintenance
#     path: maintenance.yaml
jobs:
  - name: heartbeat
    uuid: a63d6b30-d69d-4e08-946e-1ad554d0d541
    command: "echo sundiald is alive"
    trigger:
      schedule: "0 */1 * * * mon-sun"
  - name: long-lived
    uuid: 87b8069d-2fd9-487e-852a-066314cb1f77
    command: "echo sleeping; sleep 30; echo awake"
    # Fire an alert if this job is still running after 20 seconds.
    alert_if_running_for_longer_than: "20s"
    trigger:
      schedule: "30 */5 * * * mon-sun"
  - name: fails
    uuid: 14036dee-250c-4625-a3d6-21a068f82a4a
    command: "echo this job fails; exit 42"
    trigger: manual
services:
  - name: web
    uuid: 3e6012cb-d80f-4645-9b1f-15b943b35a83
    command: "python3 -m http.server 8080"
    schedule: permanent
  - name: office-worker
    uuid: 8bb33865-08a4-47ef-bdbf-028108c99c42
    command: "bin/worker"
    stop_grace_period: "45s"
    schedule:
      start: "0 0 9 * * mon-fri"
      stop: "0 0 17 * * mon-fri"
"#,
        state_dir = default_state_dir().display(),
        log_dir = default_log_dir().display(),
        service_log = default_service_log().display(),
        alert_log = default_alert_log().display(),
        alert_event_dir = default_alert_event_dir().display(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn config_accepts_pushover_alert_output() {
        let config: SundialdConfig = serde_yaml::from_str(
            r#"
alert:
  pushover:
    token: app-token
    user: user-key
    title: sundiald
    priority: 1
jobs:
  - name: failing-job
    command: "false"
    trigger:
      schedule: "0 0 * * * *"
"#,
        )
        .unwrap();

        assert!(config.validate().is_ok());
        assert_eq!(
            config.alert.pushover.unwrap().token,
            "app-token".to_string()
        );
    }

    #[test]
    fn config_rejects_invalid_pushover_priority() {
        let config: SundialdConfig = serde_yaml::from_str(
            r#"
alert:
  pushover:
    token: app-token
    user: user-key
    priority: 9
"#,
        )
        .unwrap();

        assert!(config.validate().is_err());
    }

    #[test]
    fn config_rejects_invalid_alert_if_running_for_longer_than() {
        let config: SundialdConfig = serde_yaml::from_str(
            r#"
jobs:
  - name: slow
    command: "true"
    alert_if_running_for_longer_than: "not-a-duration"
    trigger:
      schedule: "0 0 * * * *"
"#,
        )
        .unwrap();

        assert!(config.validate().is_err());
    }

    #[test]
    fn config_rejects_invalid_shutdown_grace_period() {
        let config: SundialdConfig = serde_yaml::from_str(
            r#"
shutdown_grace_period: "not-a-duration"
jobs:
  - name: ok
    command: "true"
    trigger: manual
"#,
        )
        .unwrap();

        assert!(config.validate().is_err());
    }

    #[test]
    fn config_defaults_shutdown_grace_period_to_thirty_seconds() {
        let config: SundialdConfig = serde_yaml::from_str(
            r#"
jobs:
  - name: ok
    command: "true"
    trigger: manual
"#,
        )
        .unwrap();

        assert!(config.validate().is_ok());
        assert_eq!(config.shutdown_grace(), std::time::Duration::from_secs(30));
    }

    #[test]
    fn config_accepts_valid_alert_if_running_for_longer_than() {
        let config: SundialdConfig = serde_yaml::from_str(
            r#"
jobs:
  - name: slow
    command: "true"
    alert_if_running_for_longer_than: "10m"
    trigger:
      schedule: "0 0 * * * *"
"#,
        )
        .unwrap();

        assert!(config.validate().is_ok());
        assert_eq!(
            config.jobs[0].alert_threshold(),
            Some(std::time::Duration::from_secs(600))
        );
    }

    #[test]
    fn config_accepts_completion_trigger_for_known_upstream() {
        let config: SundialdConfig = serde_yaml::from_str(
            r#"
jobs:
  - name: build
    command: "true"
    trigger: manual
  - name: deploy
    command: "true"
    trigger:
      after: build
"#,
        )
        .unwrap();

        assert!(config.validate().is_ok());
    }

    #[test]
    fn config_rejects_dependency_cycles() {
        let config: SundialdConfig = serde_yaml::from_str(
            r#"
jobs:
  - name: first
    command: "true"
    trigger: { after: second }
  - name: second
    command: "true"
    trigger: { after: first }
"#,
        )
        .unwrap();

        let error = config.validate().unwrap_err();
        assert!(error.to_string().contains("first -> second -> first"));
    }

    #[test]
    fn config_rejects_self_dependency() {
        let config: SundialdConfig = serde_yaml::from_str(
            r#"
jobs:
  - name: loop
    command: "true"
    trigger: { after: loop }
"#,
        )
        .unwrap();

        assert!(
            config
                .validate()
                .unwrap_err()
                .to_string()
                .contains("loop -> loop")
        );
    }

    #[test]
    fn config_rejects_unknown_fields_at_every_config_level() {
        for yaml in [
            "api_bnid: 127.0.0.1:8787\n",
            "alert:\n  retntion_days: 7\n",
            "jobs:\n  - name: job\n    command: 'true'\n    trigger: manual\n    commnad: 'false'\n",
            "services:\n  - name: service\n    command: 'true'\n    schedule: permanent\n    stop_grace: 5s\n",
        ] {
            assert!(
                serde_yaml::from_str::<SundialdConfig>(yaml).is_err(),
                "{yaml}"
            );
        }
    }

    #[test]
    fn non_loopback_api_requires_a_nonempty_token() {
        let config: SundialdConfig = serde_yaml::from_str("api_bind: 0.0.0.0:8787\n").unwrap();
        assert!(
            config
                .validate()
                .unwrap_err()
                .to_string()
                .contains("api_token is required")
        );

        let empty: SundialdConfig =
            serde_yaml::from_str("api_bind: 127.0.0.1:8787\napi_token: ''\n").unwrap();
        assert!(
            empty
                .validate()
                .unwrap_err()
                .to_string()
                .contains("cannot be empty")
        );

        let configured: SundialdConfig =
            serde_yaml::from_str("api_bind: 0.0.0.0:8787\napi_token: secret\n").unwrap();
        assert!(configured.validate().is_ok());
    }

    #[test]
    fn missed_run_policy_defaults_to_skip_and_accepts_run_once() {
        let default: SundialdConfig = serde_yaml::from_str("jobs: []\n").unwrap();
        let run_once: SundialdConfig =
            serde_yaml::from_str("missed_run_policy: run_once\n").unwrap();

        assert_eq!(default.missed_run_policy, MissedRunPolicy::Skip);
        assert_eq!(run_once.missed_run_policy, MissedRunPolicy::RunOnce);
    }

    #[test]
    fn config_rejects_completion_trigger_for_unknown_upstream() {
        let config: SundialdConfig = serde_yaml::from_str(
            r#"
jobs:
  - name: deploy
    command: "true"
    trigger:
      after: build
"#,
        )
        .unwrap();

        assert!(config.validate().is_err());
    }

    #[test]
    fn config_rejects_completion_trigger_for_service_name() {
        let config: SundialdConfig = serde_yaml::from_str(
            r#"
jobs:
  - name: deploy
    command: "true"
    trigger:
      after: worker
services:
  - name: worker
    command: "sleep 60"
    schedule: permanent
"#,
        )
        .unwrap();

        assert!(config.validate().is_err());
    }

    #[test]
    fn config_accepts_services_with_permanent_and_window_schedules() {
        let mut config: SundialdConfig = serde_yaml::from_str(
            r#"
env:
  APP_ENV: production
services:
  - name: api
    command: "bin/api"
    schedule: permanent
  - name: worker
    command: "bin/worker"
    stop_grace_period: 45s
    schedule:
      start: "0 0 9 * * *"
      stop: "0 0 17 * * *"
"#,
        )
        .unwrap();

        let config_path = PathBuf::from("sundiald.yaml");
        for service in &mut config.services {
            service.env = config.env.clone();
            service.source_path = Some(config_path.clone());
        }

        assert!(config.validate().is_ok());
        assert_eq!(config.services.len(), 2);
        assert!(matches!(
            config.services[0].schedule,
            ServiceSchedule::Permanent
        ));
        assert!(matches!(
            config.services[1].schedule,
            ServiceSchedule::Window { .. }
        ));
        assert_eq!(
            config.services[0].env.get("APP_ENV").map(String::as_str),
            Some("production")
        );
        assert_eq!(
            config.services[1].stop_grace(),
            std::time::Duration::from_secs(45)
        );
    }

    #[test]
    fn config_rejects_legacy_schedule_field() {
        let config = serde_yaml::from_str::<SundialdConfig>(
            r#"
jobs:
  - name: old
    command: "true"
    schedule:
      seconds: ["0"]
      minutes: ["0"]
      hours: ["*"]
"#,
        );

        assert!(config.is_err());
    }

    #[test]
    fn config_rejects_legacy_expanded_trigger_schedule() {
        let config = serde_yaml::from_str::<SundialdConfig>(
            r#"
jobs:
  - name: old
    command: "true"
    trigger:
      schedule:
        seconds: ["0"]
        minutes: ["0"]
        hours: ["*"]
"#,
        );

        assert!(config.is_err());
    }

    #[test]
    fn config_loads_named_external_job_files_relative_to_config() {
        let temp = tempfile::tempdir().unwrap();
        let jobs_dir = temp.path().join("jobs");
        std::fs::create_dir(&jobs_dir).unwrap();
        std::fs::write(
            jobs_dir.join("maintenance.yaml"),
            r#"
jobs:
  - name: cleanup
    command: "true"
    trigger:
      schedule: "0 0 * * * *"
"#,
        )
        .unwrap();
        let config_path = temp.path().join("config.yaml");
        std::fs::write(
            &config_path,
            r#"
job_files:
  - name: maintenance
    path: jobs/maintenance.yaml
"#,
        )
        .unwrap();

        let config = SundialdConfig::load(&config_path).unwrap();

        assert_eq!(config.jobs.len(), 1);
        assert_eq!(config.jobs[0].name, "cleanup");
        assert_eq!(config.jobs[0].group.as_deref(), Some("maintenance"));
        assert_eq!(
            config.jobs[0].source_path.as_deref(),
            Some(jobs_dir.join("maintenance.yaml").as_path())
        );
    }

    #[test]
    fn config_rejects_bare_list_external_job_file() {
        let temp = tempfile::tempdir().unwrap();
        let external_path = temp.path().join("ops.yaml");
        std::fs::write(
            &external_path,
            r#"
- name: cleanup
  command: "true"
  trigger: manual
"#,
        )
        .unwrap();
        let config_path = temp.path().join("config.yaml");
        std::fs::write(
            &config_path,
            r#"
job_files:
  - name: ops
    path: ops.yaml
"#,
        )
        .unwrap();

        let error = SundialdConfig::load(&config_path).unwrap_err();

        assert!(format!("{error:#}").contains("failed to parse job file"));
    }

    #[test]
    fn config_applies_external_jobs_section_env_to_loaded_jobs() {
        let temp = tempfile::tempdir().unwrap();
        let external_path = temp.path().join("ops.yaml");
        std::fs::write(
            &external_path,
            r#"
env:
  APP_ENV: production
  REPORT_ROOT: /srv/reports
jobs:
  - name: cleanup
    command: "true"
    trigger: manual
"#,
        )
        .unwrap();
        let config_path = temp.path().join("config.yaml");
        std::fs::write(
            &config_path,
            r#"
job_files:
  - name: ops
    path: ops.yaml
"#,
        )
        .unwrap();

        let config = SundialdConfig::load(&config_path).unwrap();

        assert_eq!(
            config.jobs[0].env.get("APP_ENV").map(String::as_str),
            Some("production")
        );
        assert_eq!(
            config.jobs[0].env.get("REPORT_ROOT").map(String::as_str),
            Some("/srv/reports")
        );
    }

    #[test]
    fn config_applies_root_env_to_inline_jobs() {
        let temp = tempfile::tempdir().unwrap();
        let config_path = temp.path().join("config.yaml");
        std::fs::write(
            &config_path,
            r#"
env:
  APP_ENV: development
jobs:
  - name: heartbeat
    command: "true"
    trigger: manual
"#,
        )
        .unwrap();

        let config = SundialdConfig::load(&config_path).unwrap();

        assert_eq!(
            config.jobs[0].env.get("APP_ENV").map(String::as_str),
            Some("development")
        );
    }

    #[test]
    fn config_rejects_env_on_job_file_reference() {
        let config = serde_yaml::from_str::<SundialdConfig>(
            r#"
job_files:
  - name: ops
    path: ops.yaml
    env:
      APP_ENV: production
"#,
        );

        assert!(config.is_err());
    }

    #[test]
    fn load_and_ensure_ids_patches_the_external_job_file_that_defined_the_job() {
        let temp = tempfile::tempdir().unwrap();
        let external_path = temp.path().join("ops.yaml");
        std::fs::write(
            &external_path,
            r#"
jobs:
  - name: rotate-logs
    # keep this comment next to the job definition
    command: "true"
    trigger:
      schedule: "0 0 * * * *"
"#,
        )
        .unwrap();
        let config_path = temp.path().join("config.yaml");
        std::fs::write(
            &config_path,
            r#"
job_files:
  - name: ops
    path: ops.yaml
"#,
        )
        .unwrap();

        let config = SundialdConfig::load_and_ensure_ids(&config_path).unwrap();
        let patched_external = std::fs::read_to_string(&external_path).unwrap();
        let root_config = std::fs::read_to_string(&config_path).unwrap();

        assert!(config.jobs[0].uuid.is_some());
        assert!(patched_external.contains("    uuid: "));
        assert!(patched_external.contains("    # keep this comment"));
        assert!(!root_config.contains("uuid:"));
    }

    #[test]
    fn absolutize_runtime_paths_resolves_paths_against_the_service_cwd() {
        let mut config: SundialdConfig = serde_yaml::from_str(
            r#"
state_dir: state
log_dir: logs
service_log: service.log
alert:
  log: alerts.log
  event_dir: alerts
"#,
        )
        .unwrap();
        let base = std::path::Path::new("/srv/sundiald");

        config.absolutize_runtime_paths(base);

        assert_eq!(config.state_dir, base.join("state"));
        assert_eq!(config.log_dir, base.join("logs"));
        assert_eq!(config.service_log, base.join("service.log"));
        assert_eq!(config.alert.log, base.join("alerts.log"));
        assert_eq!(config.alert.event_dir, base.join("alerts"));
    }

    #[test]
    fn sample_config_matches_example_file() {
        // Keeps sample_config(), the README snippet, and examples/sundiald.yaml
        // from silently drifting apart; update all three together. The
        // generated sample uses the current user's HOME, while the checked-in
        // example uses a portable placeholder.
        let example_path =
            std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("examples/sundiald.yaml");
        let example = std::fs::read_to_string(example_path).unwrap();
        let generated = sample_config();
        let generated = env::var_os("HOME")
            .map(|home| generated.replace(&home.to_string_lossy().to_string(), "/home/you"))
            .unwrap_or(generated);

        assert_eq!(generated, example);
    }

    #[test]
    fn default_runtime_paths_use_home_local_state() {
        let state_dir = default_state_dir();

        assert!(state_dir.ends_with(".local/state/sundiald"));
        assert_eq!(default_log_dir(), state_dir.join("logs"));
        assert_eq!(default_service_log(), state_dir.join("sundiald.log"));
        assert_eq!(default_alert_log(), state_dir.join("alerts.log"));
        assert_eq!(default_alert_event_dir(), state_dir.join("alerts"));
    }

    #[test]
    fn external_jobs_example_uses_supported_format() {
        let example_path =
            std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("examples/maintenance.yaml");
        let example = std::fs::read_to_string(example_path).unwrap();
        let contents: JobFileContents = serde_yaml::from_str(&example).unwrap();
        let (env, jobs, services) = contents.into_parts();
        let config = SundialdConfig {
            state_dir: default_state_dir(),
            log_dir: default_log_dir(),
            service_log: default_service_log(),
            api_bind: default_api_bind(),
            api_token: None,
            missed_run_policy: MissedRunPolicy::Skip,
            log_retention_days: default_log_retention_days(),
            shutdown_grace_period: default_shutdown_grace_period(),
            alert: AlertConfig::default(),
            env: HashMap::new(),
            job_files: Vec::new(),
            jobs,
            services,
        };

        assert_eq!(env.get("APP_ENV").map(String::as_str), Some("production"));
        assert!(config.validate().is_ok());
    }
}
