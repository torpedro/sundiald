use anyhow::{Context, Result};
use chrono::{DateTime, Local};
use serde::Serialize;
use tokio::{
    fs::{self, OpenOptions},
    io::AsyncWriteExt,
    process::Command,
};
use uuid::Uuid;

use crate::config::{AlertCommandConfig, AlertConfig, PushoverConfig};

#[derive(Debug, Serialize)]
struct AlertEvent<'a> {
    job: &'a str,
    message: &'a str,
    created_at: DateTime<Local>,
}

/// Writes the durable alert record (log line + JSON event file) for a job
/// failure, then best-effort forwards it to the optional command/Pushover
/// notification channels. Notification-channel failures are logged to
/// stderr but do not return an error: they are not the same kind of failure
/// as the job itself failing, and propagating them up causes the caller to
/// write a second, confusing "internal error" alert for what is really just
/// a delivery problem.
pub(crate) async fn write_alert(alert: &AlertConfig, job_name: &str, message: &str) {
    if let Err(error) = write_alert_inner(alert, job_name, message).await {
        eprintln!("failed to record alert for job '{job_name}': {error:#}");
    }
}

async fn write_alert_inner(alert: &AlertConfig, job_name: &str, message: &str) -> Result<()> {
    fs::create_dir_all(&alert.event_dir).await?;
    let created_at = Local::now();
    let alert_file = alert.event_dir.join(format!(
        "{}-{}-{}.json",
        created_at.format("%Y%m%d%H%M%S"),
        super::sanitize_name(job_name),
        Uuid::new_v4()
    ));
    let event = AlertEvent {
        job: job_name,
        message,
        created_at,
    };
    fs::write(&alert_file, serde_json::to_vec_pretty(&event)?)
        .await
        .with_context(|| format!("failed to write alert event {}", alert_file.display()))?;

    let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(&alert.log)
        .await
        .with_context(|| format!("failed to open alert log {}", alert.log.display()))?;
    let line = format!(
        "{} job={} alert={} alert_file={}\n",
        created_at.to_rfc3339(),
        job_name,
        message,
        alert_file.display()
    );
    file.write_all(line.as_bytes()).await?;
    println!("{line}");

    if let Some(alert_command) = &alert.command {
        if let Err(error) = run_alert_command(alert_command, job_name, message, &alert_file).await {
            eprintln!("failed to run alert command for job '{job_name}': {error:#}");
        }
    }
    if let Some(pushover) = &alert.pushover {
        if let Err(error) = send_pushover_alert(pushover, job_name, message, &alert_file).await {
            eprintln!("failed to send Pushover alert for job '{job_name}': {error:#}");
        }
    }
    Ok(())
}

async fn run_alert_command(
    alert_command: &AlertCommandConfig,
    job_name: &str,
    message: &str,
    alert_file: &std::path::Path,
) -> Result<()> {
    let args = alert_command
        .args
        .iter()
        .map(|arg| {
            arg.replace("{job}", job_name)
                .replace("{message}", message)
                .replace("{alert_file}", &alert_file.display().to_string())
        })
        .collect::<Vec<_>>();

    let status = Command::new(&alert_command.program)
        .args(args)
        .status()
        .await
        .with_context(|| format!("failed to run alert command {}", alert_command.program))?;
    if !status.success() {
        eprintln!("alert command exited with status {status}");
    }
    Ok(())
}

async fn send_pushover_alert(
    pushover: &PushoverConfig,
    job_name: &str,
    message: &str,
    alert_file: &std::path::Path,
) -> Result<()> {
    let title = pushover
        .title
        .clone()
        .unwrap_or_else(|| format!("sundiald: {job_name} failed"));
    let body = format!("{message}\nalert_file: {}", alert_file.display());

    let mut form = vec![
        ("token", pushover.token.clone()),
        ("user", pushover.user.clone()),
        ("title", title),
        ("message", body),
    ];
    push_optional(&mut form, "device", &pushover.device);
    push_optional(&mut form, "sound", &pushover.sound);
    push_optional(&mut form, "url", &pushover.url);
    push_optional(&mut form, "url_title", &pushover.url_title);
    if let Some(priority) = pushover.priority {
        form.push(("priority", priority.to_string()));
    }
    if let Some(ttl) = pushover.ttl {
        form.push(("ttl", ttl.to_string()));
    }

    let response = reqwest::Client::new()
        .post("https://api.pushover.net/1/messages.json")
        .form(&form)
        .send()
        .await
        .context("failed to send Pushover alert")?;

    let status = response.status();
    if !status.is_success() {
        let response_body = response.text().await.unwrap_or_default();
        anyhow::bail!("Pushover alert failed with HTTP {status}: {response_body}");
    }

    Ok(())
}

fn push_optional(
    form: &mut Vec<(&'static str, String)>,
    key: &'static str,
    value: &Option<String>,
) {
    if let Some(value) = value {
        form.push((key, value.clone()));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::AlertConfig;

    #[tokio::test]
    async fn write_alert_keeps_multiple_events_for_same_job_in_same_second() {
        let temp = tempfile::tempdir().unwrap();
        let alert = AlertConfig {
            log: temp.path().join("alerts.log"),
            event_dir: temp.path().join("events"),
            retention_days: 0,
            command: None,
            pushover: None,
        };

        write_alert(&alert, "same-job", "first").await;
        write_alert(&alert, "same-job", "second").await;

        let mut entries = fs::read_dir(&alert.event_dir).await.unwrap();
        let mut count = 0;
        while entries.next_entry().await.unwrap().is_some() {
            count += 1;
        }

        assert_eq!(count, 2);
    }
}
