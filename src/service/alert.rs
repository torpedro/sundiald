use anyhow::{Context, Result};
use chrono::{DateTime, Local};
use serde::Serialize;
use tokio::{
    fs::{self, OpenOptions},
    io::AsyncWriteExt,
    process::Command,
    time::{Duration, timeout},
};
use uuid::Uuid;

use crate::config::{AlertCommandConfig, AlertConfig, FlaresConfig, PushoverConfig};

#[derive(Debug, Serialize)]
struct AlertEvent<'a> {
    job: &'a str,
    message: &'a str,
    created_at: DateTime<Local>,
}

/// Writes the durable alert record (log line + JSON event file) for a job
/// failure, then best-effort forwards it to the optional command/Pushover/Flares
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

    if let Some(alert_command) = &alert.command
        && let Err(error) = run_alert_command(alert_command, job_name, message, &alert_file).await
    {
        eprintln!("failed to run alert command for job '{job_name}': {error:#}");
    }
    if let Some(pushover) = &alert.pushover
        && let Err(error) = send_pushover_alert(pushover, job_name, message).await
    {
        eprintln!("failed to send Pushover alert for job '{job_name}': {error:#}");
    }
    if let Some(flares) = &alert.flares
        && let Err(error) = send_flares_alert(flares, job_name, message).await
    {
        eprintln!("failed to send Flares alert for job '{job_name}': {error:#}");
    }
    Ok(())
}

async fn send_flares_alert(flares: &FlaresConfig, job_name: &str, message: &str) -> Result<()> {
    let client = flares_client::ApiClient::new(&flares.url, &flares.token)
        .context("failed to build Flares HTTP client")?;
    // Flares limits title/message lengths in Unicode characters. The durable
    // local event retains the full text if a notification needs truncation.
    let title = flares
        .title
        .clone()
        .unwrap_or_else(|| format!("sundiald: {job_name}"));
    let request = flares_client::Alert {
        title: title.chars().take(250).collect(),
        message: format!("{job_name}: {message}")
            .chars()
            .take(1024)
            .collect(),
        severity: flares.severity,
        group_key: None,
    };
    let result = client
        .alert(request, Some(Uuid::new_v4().to_string()))
        .await
        .context("failed to send Flares alert")?;
    match result.notification.status {
        flares_client::NotificationStatus::Sent | flares_client::NotificationStatus::Pending => {
            Ok(())
        }
        status => anyhow::bail!(
            "Flares delivery {} returned status {}",
            result.delivery_id,
            status.as_str()
        ),
    }
}

async fn run_alert_command(
    alert_command: &AlertCommandConfig,
    job_name: &str,
    message: &str,
    alert_file: &std::path::Path,
) -> Result<()> {
    run_alert_command_with_timeout(
        alert_command,
        job_name,
        message,
        alert_file,
        Duration::from_secs(30),
    )
    .await
}

async fn run_alert_command_with_timeout(
    alert_command: &AlertCommandConfig,
    job_name: &str,
    message: &str,
    alert_file: &std::path::Path,
    limit: Duration,
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

    let mut child = Command::new(&alert_command.program)
        .args(args)
        .kill_on_drop(true)
        .spawn()
        .with_context(|| format!("failed to run alert command {}", alert_command.program))?;
    let status = match timeout(limit, child.wait()).await {
        Ok(status) => status.with_context(|| {
            format!("failed to wait for alert command {}", alert_command.program)
        })?,
        Err(_) => {
            let _ = child.kill().await;
            let _ = child.wait().await;
            anyhow::bail!(
                "alert command {} timed out after {}s",
                alert_command.program,
                limit.as_secs_f64()
            );
        }
    };
    if !status.success() {
        eprintln!("alert command exited with status {status}");
    }
    Ok(())
}

async fn send_pushover_alert(
    pushover: &PushoverConfig,
    job_name: &str,
    message: &str,
) -> Result<()> {
    let title = pushover
        .title
        .clone()
        .unwrap_or_else(|| format!("sundiald: {job_name} failed"));
    let body = pushover_message_body(job_name, message);

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

    let client = reqwest::Client::builder()
        .connect_timeout(Duration::from_secs(5))
        .timeout(Duration::from_secs(15))
        .build()
        .context("failed to build Pushover HTTP client")?;
    let response = client
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

fn pushover_message_body(job_name: &str, message: &str) -> String {
    format!("{job_name}: {message}")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::AlertConfig;

    async fn flares_server(
        status: axum::http::StatusCode,
        response: serde_json::Value,
    ) -> (
        FlaresConfig,
        tokio::sync::mpsc::UnboundedReceiver<(axum::http::HeaderMap, serde_json::Value)>,
        tokio::task::JoinHandle<()>,
    ) {
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        let app = axum::Router::new().route(
            "/v1/alerts",
            axum::routing::post(
                move |headers: axum::http::HeaderMap,
                      axum::Json(body): axum::Json<serde_json::Value>| {
                    let tx = tx.clone();
                    let response = response.clone();
                    async move {
                        tx.send((headers, body)).unwrap();
                        (status, axum::Json(response))
                    }
                },
            ),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let config = FlaresConfig {
            url: format!("http://{}", listener.local_addr().unwrap()),
            token: "test-token".into(),
            title: None,
            severity: flares_client::Severity::Warning,
        };
        let server = tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        (config, rx, server)
    }

    #[tokio::test]
    async fn flares_sends_authenticated_alerts_and_accepts_pending_delivery() {
        for status in ["sent", "pending"] {
            let (mut config, mut requests, server) = flares_server(
                axum::http::StatusCode::OK,
                serde_json::json!({"delivery_id": 42, "notification": {"status": status, "error": null}}),
            ).await;
            send_flares_alert(&config, "backup", "exited with status 42")
                .await
                .unwrap();
            let (headers, body) = requests.recv().await.unwrap();
            assert_eq!(headers["authorization"], "Bearer test-token");
            let first_key = headers["idempotency-key"].to_str().unwrap();
            assert!(Uuid::parse_str(first_key).is_ok());
            assert_eq!(body["title"], "sundiald: backup");
            assert_eq!(body["message"], "backup: exited with status 42");
            assert_eq!(body["severity"], "warning");
            assert!(body["group_key"].is_null());

            config.title = Some("Custom title".into());
            config.severity = flares_client::Severity::Critical;
            send_flares_alert(&config, "backup", &"é".repeat(1500))
                .await
                .unwrap();
            let (headers, body) = requests.recv().await.unwrap();
            assert_ne!(headers["idempotency-key"], first_key);
            assert_eq!(body["title"], "Custom title");
            assert_eq!(body["severity"], "critical");
            assert_eq!(body["message"].as_str().unwrap().chars().count(), 1024);
            config.title = None;
            send_flares_alert(&config, &"é".repeat(300), "failed")
                .await
                .unwrap();
            let (_, body) = requests.recv().await.unwrap();
            assert_eq!(body["title"].as_str().unwrap().chars().count(), 250);
            server.abort();
        }
    }

    #[tokio::test]
    async fn flares_reports_delivery_and_http_failures_without_duplicate_events() {
        for (http_status, status) in [
            (axum::http::StatusCode::OK, "failed"),
            (axum::http::StatusCode::OK, "unknown"),
            (axum::http::StatusCode::OK, "not_attempted"),
            (axum::http::StatusCode::UNAUTHORIZED, "failed"),
            (axum::http::StatusCode::INTERNAL_SERVER_ERROR, "failed"),
            (axum::http::StatusCode::OK, "invalid-status"),
        ] {
            let (config, mut requests, server) = flares_server(http_status,
                serde_json::json!({"delivery_id": 42, "notification": {"status": status, "error": "private-response"}})
            ).await;
            let error = send_flares_alert(&config, "backup", "failed")
                .await
                .unwrap_err();
            let error = format!("{error:#}");
            assert!(!error.contains("test-token"));
            assert!(!error.contains("private-response"));
            requests.recv().await.unwrap();
            let temp = tempfile::tempdir().unwrap();
            let alert = AlertConfig {
                log: temp.path().join("alerts.log"),
                event_dir: temp.path().join("events"),
                flares: Some(config),
                ..AlertConfig::default()
            };
            write_alert_inner(&alert, "backup", "failed").await.unwrap();
            requests.recv().await.unwrap();
            assert!(requests.try_recv().is_err());
            assert_eq!(
                fs::read_to_string(&alert.log)
                    .await
                    .unwrap()
                    .lines()
                    .count(),
                1
            );
            let mut events = fs::read_dir(&alert.event_dir).await.unwrap();
            let event = events.next_entry().await.unwrap().unwrap();
            let body: serde_json::Value =
                serde_json::from_slice(&fs::read(event.path()).await.unwrap()).unwrap();
            assert_eq!(body["message"], "failed");
            assert!(events.next_entry().await.unwrap().is_none());
            server.abort();
        }
    }

    #[tokio::test]
    async fn write_alert_keeps_multiple_events_for_same_job_in_same_second() {
        let temp = tempfile::tempdir().unwrap();
        let alert = AlertConfig {
            log: temp.path().join("alerts.log"),
            event_dir: temp.path().join("events"),
            retention_days: 0,
            command: None,
            pushover: None,
            flares: None,
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

    #[test]
    fn pushover_message_starts_with_job_and_omits_alert_file() {
        let body = pushover_message_body("backup", "job exited with status 42");

        assert_eq!(body, "backup: job exited with status 42");
        assert!(!body.contains("alert_file"));
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn alert_command_is_killed_when_it_times_out() {
        let command = AlertCommandConfig {
            program: "sh".to_string(),
            args: vec!["-c".to_string(), "sleep 5".to_string()],
        };

        let error = run_alert_command_with_timeout(
            &command,
            "job",
            "message",
            std::path::Path::new("alert.json"),
            Duration::from_millis(20),
        )
        .await
        .unwrap_err();

        assert!(error.to_string().contains("timed out"));
    }
}
