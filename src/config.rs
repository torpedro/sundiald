mod duration;
mod schedule;
mod uuid_patch;

use std::{
    collections::{HashMap, HashSet},
    fs,
    net::SocketAddr,
    path::PathBuf,
};

use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

pub use schedule::Schedule;

#[derive(Debug, Clone, Deserialize)]
pub struct SundialdConfig {
    #[serde(default = "default_state_dir")]
    pub state_dir: PathBuf,
    #[serde(default = "default_log_dir")]
    pub log_dir: PathBuf,
    #[serde(default = "default_service_log")]
    pub service_log: PathBuf,
    #[serde(default = "default_api_bind")]
    pub api_bind: SocketAddr,
    #[serde(default = "default_log_retention_days")]
    pub log_retention_days: u32,
    #[serde(default)]
    pub alert: AlertConfig,
    #[serde(default)]
    pub jobs: Vec<JobConfig>,
}

#[derive(Debug, Clone, Deserialize)]
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
pub struct AlertCommandConfig {
    pub program: String,
    #[serde(default)]
    pub args: Vec<String>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
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
pub struct JobConfig {
    /// Stable identity used to track a job across renames. Absent for a
    /// hand-written or freshly generated config; `load_and_ensure_ids` fills
    /// in and persists a fresh one for any job missing it.
    #[serde(default)]
    pub uuid: Option<Uuid>,
    pub name: String,
    pub command: String,
    pub schedule: Schedule,
    /// If a run is still active this long after it started, sundiald fires a
    /// "still running" alert (once per run) through the normal alert
    /// channels. A duration like `"45s"`, `"10m"`, `"2h"`, `"1d"`, or a
    /// compound value like `"1h30m"`.
    #[serde(default)]
    pub alert_if_running_for_longer_than: Option<String>,
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
        let config: Self = serde_yaml::from_str(&raw)
            .with_context(|| format!("failed to parse YAML config {}", path.display()))?;
        config.validate()?;
        Ok(config)
    }

    pub fn validate(&self) -> Result<()> {
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

        let mut names = HashSet::new();
        let mut uuids = HashSet::new();
        for job in &self.jobs {
            if job.name.trim().is_empty() {
                bail!("job name cannot be empty");
            }
            if !names.insert(job.name.clone()) {
                bail!("duplicate job name '{}'", job.name);
            }
            if let Some(uuid) = job.uuid {
                if !uuids.insert(uuid) {
                    bail!("duplicate job uuid '{uuid}' (job '{}')", job.name);
                }
            }
            if job.command.trim().is_empty() {
                bail!("job '{}' command cannot be empty", job.name);
            }
            if let Some(duration) = &job.alert_if_running_for_longer_than {
                duration::parse_duration(duration).with_context(|| {
                    format!(
                        "invalid alert_if_running_for_longer_than for job '{}'",
                        job.name
                    )
                })?;
            }
            job.schedule
                .validate()
                .with_context(|| format!("invalid schedule for job '{}'", job.name))?;
        }
        Ok(())
    }

    /// Like `load`, but also assigns a fresh UUID to any job missing one and
    /// persists it back to `path` before returning — a minimal, targeted
    /// text patch that inserts `uuid: <uuid>` lines rather than
    /// re-serializing the whole file, so hand-written comments and
    /// formatting elsewhere in the config survive untouched.
    pub fn load_and_ensure_ids(path: &PathBuf) -> Result<Self> {
        let mut config = Self::load(path)?;

        let missing: Vec<(String, Uuid)> = config
            .jobs
            .iter()
            .filter(|job| job.uuid.is_none())
            .map(|job| (job.name.clone(), Uuid::new_v4()))
            .collect();
        if missing.is_empty() {
            return Ok(config);
        }

        let raw = fs::read_to_string(path)
            .with_context(|| format!("failed to read config {}", path.display()))?;
        let patched = uuid_patch::insert_missing_job_uuids(&raw, &missing).with_context(|| {
            format!(
                "failed to persist generated job uuids into {}",
                path.display()
            )
        })?;
        fs::write(path, &patched)
            .with_context(|| format!("failed to persist job uuids to {}", path.display()))?;

        let assigned: HashMap<&str, Uuid> = missing
            .iter()
            .map(|(name, uuid)| (name.as_str(), *uuid))
            .collect();
        for job in &mut config.jobs {
            if job.uuid.is_none() {
                job.uuid = assigned.get(job.name.as_str()).copied();
            }
        }

        Ok(config)
    }
}

fn default_state_dir() -> PathBuf {
    PathBuf::from(".sundiald")
}

fn default_log_dir() -> PathBuf {
    PathBuf::from(".sundiald/logs")
}

fn default_service_log() -> PathBuf {
    PathBuf::from(".sundiald/sundiald.log")
}

fn default_api_bind() -> SocketAddr {
    "127.0.0.1:8787"
        .parse()
        .expect("default api bind address is valid")
}

fn default_alert_log() -> PathBuf {
    PathBuf::from(".sundiald/alerts.log")
}

fn default_alert_event_dir() -> PathBuf {
    PathBuf::from(".sundiald/alerts")
}

fn default_log_retention_days() -> u32 {
    14
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

pub fn sample_config() -> &'static str {
    r#"state_dir: .sundiald
log_dir: .sundiald/logs
service_log: .sundiald/sundiald.log
api_bind: 127.0.0.1:8787
# Delete job log files older than this many days. Set to 0 to keep logs forever.
log_retention_days: 14
alert:
  log: .sundiald/alerts.log
  event_dir: .sundiald/alerts
  # Delete alert event JSON files older than this many days. Set to 0 to keep forever.
  retention_days: 90
  # Optional command run when a job fails. No environment variables are used.
  # Placeholders available in args: {job}, {message}, {alert_file}
  # command:
  #   program: /usr/local/bin/sundiald-alert
  #   args: ["--event", "{alert_file}"]
  # Optional Pushover output. Credentials are read from this config file.
  # pushover:
  #   token: "your-pushover-application-token"
  #   user: "your-pushover-user-or-group-key"
  #   title: "sundiald"
  #   priority: 0
jobs:
  - name: heartbeat
    uuid: a63d6b30-d69d-4e08-946e-1ad554d0d541
    command: "echo sundiald is alive"
    schedule:
      seconds: ["0"]
      minutes: ["*/1"]
      hours: ["*"]
      days_of_week: ["mon", "tue", "wed", "thu", "fri", "sat", "sun"]
      days_of_month: ["*"]
      months: ["*"]
  - name: long-lived
    uuid: 87b8069d-2fd9-487e-852a-066314cb1f77
    command: "echo sleeping; sleep 30; echo awake"
    # Fire an alert if this job is still running after 20 seconds.
    alert_if_running_for_longer_than: "20s"
    schedule:
      seconds: ["30"]
      minutes: ["*/5"]
      hours: ["*"]
      days_of_week: ["mon", "tue", "wed", "thu", "fri", "sat", "sun"]
      days_of_month: ["*"]
      months: ["*"]
  - name: fails
    uuid: 14036dee-250c-4625-a3d6-21a068f82a4a
    command: "echo this job fails; exit 42"
    schedule:
      manual_only: true
"#
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
    schedule:
      minutes: ["0"]
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
    schedule:
      minutes: ["0"]
"#,
        )
        .unwrap();

        assert!(config.validate().is_err());
    }

    #[test]
    fn config_accepts_valid_alert_if_running_for_longer_than() {
        let config: SundialdConfig = serde_yaml::from_str(
            r#"
jobs:
  - name: slow
    command: "true"
    alert_if_running_for_longer_than: "10m"
    schedule:
      minutes: ["0"]
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
    fn sample_config_matches_example_file() {
        // Keeps sample_config(), the README snippet, and examples/sundiald.yaml
        // from silently drifting apart; update all three together.
        let example_path =
            std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("examples/sundiald.yaml");
        let example = std::fs::read_to_string(example_path).unwrap();
        assert_eq!(sample_config(), example);
    }
}
