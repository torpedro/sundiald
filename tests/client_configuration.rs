use std::{path::Path, process::Output, time::Duration};

use axum::{Json, Router, extract::Request, http::StatusCode};
use tokio::process::Command;

fn command(home: &Path) -> Command {
    let mut command = Command::new(env!("CARGO_BIN_EXE_sundiald"));
    command
        .env("HOME", home)
        .env("XDG_CONFIG_HOME", home.join("xdg"));
    for key in [
        "SUNDIALD_CONFIG",
        "SUNDIALD_CLIENT_CONFIG",
        "SUNDIALD_URL",
        "SUNDIALD_TOKEN",
        "SUNDIALD_TOKEN_FILE",
        "SUNDIALD_TOKEN_ENV",
    ] {
        command.env_remove(key);
    }
    command.kill_on_drop(true);
    command
}

async fn output(command: &mut Command) -> Output {
    tokio::time::timeout(Duration::from_secs(15), command.output())
        .await
        .unwrap()
        .unwrap()
}

#[tokio::test]
async fn client_diagnostics_are_independent_and_redact_tokens() {
    let temp = tempfile::tempdir().unwrap();
    let directory = temp.path().join("xdg/sundiald");
    std::fs::create_dir_all(&directory).unwrap();
    std::fs::write(directory.join("config.yaml"), "broken server config: [").unwrap();
    // Isolate this test from any client defaults installed in the host /etc.
    std::fs::write(directory.join("client.yaml"), "{}\n").unwrap();
    let result = output(command(temp.path()).arg("client-config")).await;
    assert!(result.status.success());
    assert!(String::from_utf8_lossy(&result.stdout).contains("http://127.0.0.1:8787"));
    std::fs::write(directory.join("api-token"), "secret-value\n").unwrap();
    std::fs::write(
        directory.join("client.yaml"),
        "url: https://scheduler.example.com/base\ntoken_file: api-token\n",
    )
    .unwrap();
    let result = output(command(temp.path()).arg("client-config")).await;
    assert!(result.status.success());
    let stdout = String::from_utf8_lossy(&result.stdout);
    assert!(stdout.contains("https://scheduler.example.com/base"));
    assert!(stdout.contains("token: configured"));
    assert!(!stdout.contains("secret-value"));
    assert!(!String::from_utf8_lossy(&result.stderr).contains("secret-value"));
    let result =
        output(command(temp.path()).args(["client-config", "--token", "a", "--token-file", "b"]))
            .await;
    assert!(!result.status.success());
    for args in [
        vec!["client-config"],
        vec!["ui", "--once"],
        vec!["reload"],
        vec!["run", "backup"],
    ] {
        let result = output(
            command(temp.path())
                .args(args)
                .args(["--config", "server.yaml"]),
        )
        .await;
        assert!(!result.status.success());
        assert!(String::from_utf8_lossy(&result.stderr).contains("unexpected argument '--config'"));
    }
}

#[tokio::test]
async fn every_client_command_uses_connection_settings_and_preserves_url_prefix() {
    let temp = tempfile::tempdir().unwrap();
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
    let app = Router::new().fallback(move |request: Request| {
        let tx = tx.clone();
        async move {
            let uri = request.uri().to_string();
            tx.send((
                request.method().clone(),
                uri.clone(),
                request.headers().clone(),
            ))
            .unwrap();
            let body = if uri.ends_with("/status") {
                serde_json::json!({"updated_at": chrono::Utc::now(), "jobs": [], "services": []})
            } else if uri.contains("/history") {
                serde_json::json!({"job": "backup", "uuid": "a63d6b30-d69d-4e08-946e-1ad554d0d541", "runs": []})
            } else {
                serde_json::json!({})
            };
            (StatusCode::OK, Json(body))
        }
    });
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}/scheduler/", listener.local_addr().unwrap());
    let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    for (args, method, path) in [
        (vec!["run", "backup"], "POST", "/jobs/backup/run"),
        (
            vec!["terminate", "backup"],
            "POST",
            "/jobs/backup/terminate",
        ),
        (vec!["kill", "backup"], "POST", "/jobs/backup/kill"),
        (vec!["start-service", "web"], "POST", "/services/web/start"),
        (vec!["stop-service", "web"], "POST", "/services/web/stop"),
        (vec!["kill-service", "web"], "POST", "/services/web/kill"),
        (vec!["reload"], "POST", "/reload"),
        (
            vec!["history", "backup"],
            "GET",
            "/jobs/backup/history?limit=20",
        ),
        (vec!["ui", "--once"], "GET", "/status"),
    ] {
        let result = output(
            command(temp.path())
                .args(args)
                .arg("--url")
                .arg(&url)
                .env("SUNDIALD_TOKEN", "test-token"),
        )
        .await;
        assert!(
            result.status.success(),
            "{}",
            String::from_utf8_lossy(&result.stderr)
        );
        let (actual_method, actual_path, headers) = rx.try_recv().unwrap();
        assert_eq!(actual_method, method);
        assert_eq!(actual_path, format!("/scheduler{path}"));
        assert_eq!(headers["authorization"], "Bearer test-token");
    }
    server.abort();
}

#[tokio::test]
async fn daemon_config_discovery_has_its_own_override() {
    let temp = tempfile::tempdir().unwrap();
    let directory = temp.path().join("xdg/sundiald");
    std::fs::create_dir_all(&directory).unwrap();
    std::fs::write(directory.join("config.yaml"), "api_bind: 127.0.0.1:8765\n").unwrap();
    let result = output(command(temp.path()).arg("config")).await;
    assert!(result.status.success());
    assert!(String::from_utf8_lossy(&result.stdout).contains("127.0.0.1:8765"));
    let alternate = temp.path().join("alternate.yaml");
    std::fs::write(&alternate, "api_bind: 127.0.0.1:8766\n").unwrap();
    let result = output(
        command(temp.path())
            .arg("config")
            .env("SUNDIALD_CONFIG", &alternate),
    )
    .await;
    assert!(result.status.success());
    assert!(String::from_utf8_lossy(&result.stdout).contains("127.0.0.1:8766"));
    let result = output(
        command(temp.path())
            .args(["config", "--config"])
            .arg(directory.join("config.yaml"))
            .env("SUNDIALD_CONFIG", &alternate),
    )
    .await;
    assert!(result.status.success());
    assert!(String::from_utf8_lossy(&result.stdout).contains("127.0.0.1:8765"));
}
