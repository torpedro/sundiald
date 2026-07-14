#![cfg(unix)]

use std::{
    net::TcpListener,
    process::{Child, Command, Stdio},
    time::{Duration, Instant},
};

use rusqlite::Connection;
use serde_json::Value;

fn available_address() -> std::net::SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    listener.local_addr().unwrap()
}

async fn wait_for_api(address: std::net::SocketAddr) {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        if reqwest::get(format!("http://{address}/health"))
            .await
            .is_ok_and(|response| response.status().is_success())
        {
            return;
        }
        assert!(Instant::now() < deadline, "daemon API did not become ready");
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

async fn wait_for_file(path: &std::path::Path) {
    let deadline = Instant::now() + Duration::from_secs(5);
    while !path.exists() {
        assert!(
            Instant::now() < deadline,
            "{} was not created",
            path.display()
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

async fn wait_for_exit(child: &mut Child) -> std::process::ExitStatus {
    let deadline = Instant::now() + Duration::from_secs(8);
    loop {
        if let Some(status) = child.try_wait().unwrap() {
            return status;
        }
        if Instant::now() >= deadline {
            let _ = child.kill();
            panic!("daemon did not exit after SIGTERM");
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

#[tokio::test]
async fn sigterm_gracefully_stops_running_service_and_finishes_persistence() {
    let temp = tempfile::tempdir().unwrap();
    let state_dir = temp.path().join("state");
    let log_dir = temp.path().join("logs");
    let ready_file = temp.path().join("service-ready");
    let stopped_file = temp.path().join("service-stopped");
    let config_path = temp.path().join("sundiald.yaml");
    let address = available_address();
    let service_id = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa";
    std::fs::write(
        &config_path,
        format!(
            r#"state_dir: {state_dir}
log_dir: {log_dir}
service_log: {service_log}
api_bind: {address}
shutdown_grace_period: "2s"
alert:
  log: {alert_log}
  event_dir: {alert_dir}
services:
  - name: worker
    uuid: {service_id}
    command: |
      trap 'echo stopped > {stopped_file}; exit 0' TERM
      echo ready > {ready_file}
      while true; do sleep 1; done
    schedule: permanent
"#,
            state_dir = state_dir.display(),
            log_dir = log_dir.display(),
            service_log = temp.path().join("sundiald.log").display(),
            alert_log = temp.path().join("alerts.log").display(),
            alert_dir = temp.path().join("alerts").display(),
            stopped_file = stopped_file.display(),
            ready_file = ready_file.display(),
        ),
    )
    .unwrap();

    let mut daemon = Command::new(env!("CARGO_BIN_EXE_sundiald"))
        .args(["daemon", "--config"])
        .arg(&config_path)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();

    wait_for_api(address).await;
    let response = reqwest::Client::new()
        .post(format!("http://{address}/services/worker/start"))
        .send()
        .await
        .unwrap();
    assert!(response.status().is_success());
    wait_for_file(&ready_file).await;

    assert_eq!(
        unsafe { libc::kill(daemon.id() as libc::pid_t, libc::SIGTERM) },
        0
    );
    let status = wait_for_exit(&mut daemon).await;
    assert!(status.success());
    assert!(stopped_file.exists());

    let state: Value =
        serde_json::from_slice(&std::fs::read(state_dir.join("state.json")).unwrap()).unwrap();
    let worker = state["jobs"]
        .as_array()
        .unwrap()
        .iter()
        .find(|job| job["uuid"] == service_id)
        .unwrap();
    assert_eq!(worker["status"], "succeeded");
    assert!(worker["finished_at"].is_string());

    let connection = Connection::open(state_dir.join("history.sqlite3")).unwrap();
    let (history_status, finished_at): (String, String) = connection
        .query_row(
            "SELECT status, finished_at FROM job_runs WHERE job_uuid = ?1",
            [service_id],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    assert_eq!(history_status, "succeeded");
    assert!(!finished_at.is_empty());
}

#[tokio::test]
async fn occupied_api_port_prevents_daemon_startup() {
    let temp = tempfile::tempdir().unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let config_path = temp.path().join("sundiald.yaml");
    std::fs::write(
        &config_path,
        format!(
            "state_dir: {}\nlog_dir: {}\nservice_log: {}\napi_bind: {address}\njobs: []\n",
            temp.path().join("state").display(),
            temp.path().join("logs").display(),
            temp.path().join("sundiald.log").display(),
        ),
    )
    .unwrap();

    let output = tokio::time::timeout(
        Duration::from_secs(5),
        tokio::process::Command::new(env!("CARGO_BIN_EXE_sundiald"))
            .args(["daemon", "--config"])
            .arg(&config_path)
            .output(),
    )
    .await
    .expect("daemon did not report the occupied API port")
    .unwrap();

    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("failed to bind api"),
        "stderr was: {stderr}"
    );
    assert!(!stderr.contains("service started"));
    assert!(!stderr.contains("api listening"));
}
