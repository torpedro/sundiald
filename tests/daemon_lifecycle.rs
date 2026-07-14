#![cfg(unix)]

use std::{
    ffi::CString,
    net::TcpListener,
    os::unix::ffi::OsStrExt,
    path::Path,
    process::{Child, Command, Stdio},
    time::{Duration, Instant},
};

use chrono::{Datelike, Local, Timelike};
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

fn spawn_daemon(config_path: &Path) -> Child {
    Command::new(env!("CARGO_BIN_EXE_sundiald"))
        .args(["daemon", "--config"])
        .arg(config_path)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap()
}

async fn stop_daemon(child: &mut Child) {
    assert_eq!(
        unsafe { libc::kill(child.id() as libc::pid_t, libc::SIGTERM) },
        0
    );
    assert!(wait_for_exit(child).await.success());
}

fn make_file_old(path: &Path) {
    let path = CString::new(path.as_os_str().as_bytes()).unwrap();
    let old = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs() as libc::time_t
        - 3 * 24 * 60 * 60;
    let times = [
        libc::timespec {
            tv_sec: old,
            tv_nsec: 0,
        },
        libc::timespec {
            tv_sec: old,
            tv_nsec: 0,
        },
    ];
    assert_eq!(
        unsafe { libc::utimensat(libc::AT_FDCWD, path.as_ptr(), times.as_ptr(), 0) },
        0
    );
}

async fn wait_for_finished_rows(database: &Path, expected: i64) {
    let deadline = Instant::now() + Duration::from_secs(8);
    loop {
        let count = Connection::open(database)
            .and_then(|connection| {
                connection.query_row(
                    "SELECT COUNT(*) FROM job_runs WHERE finished_at IS NOT NULL",
                    [],
                    |row| row.get::<_, i64>(0),
                )
            })
            .unwrap_or(0);
        if count == expected {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "only {count}/{expected} runs finished"
        );
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

#[tokio::test]
async fn run_once_policy_coalesces_a_schedule_tick_missed_while_suspended() {
    let temp = tempfile::tempdir().unwrap();
    let state_dir = temp.path().join("state");
    let marker = temp.path().join("missed-run-fired");
    let config_path = temp.path().join("sundiald.yaml");
    let address = available_address();
    let target = Local::now() + chrono::Duration::seconds(4);
    std::fs::write(
        &config_path,
        format!(
            r#"state_dir: {state_dir}
log_dir: {log_dir}
service_log: {service_log}
api_bind: {address}
missed_run_policy: run_once
jobs:
  - name: missed
    uuid: bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb
    command: "echo fired > {marker}"
    trigger:
      schedule: "{second} {minute} {hour} {day} {month} *"
"#,
            state_dir = state_dir.display(),
            log_dir = temp.path().join("logs").display(),
            service_log = temp.path().join("sundiald.log").display(),
            marker = marker.display(),
            second = target.second(),
            minute = target.minute(),
            hour = target.hour(),
            day = target.day(),
            month = target.month(),
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
    assert_eq!(
        unsafe { libc::kill(daemon.id() as libc::pid_t, libc::SIGSTOP) },
        0
    );
    let wait = (target - Local::now())
        .to_std()
        .unwrap_or_default()
        .saturating_add(Duration::from_secs(2));
    tokio::time::sleep(wait).await;
    assert_eq!(
        unsafe { libc::kill(daemon.id() as libc::pid_t, libc::SIGCONT) },
        0
    );

    wait_for_file(&marker).await;
    assert_eq!(
        unsafe { libc::kill(daemon.id() as libc::pid_t, libc::SIGTERM) },
        0
    );
    assert!(wait_for_exit(&mut daemon).await.success());
}

#[tokio::test]
async fn startup_prunes_nested_logs_and_reload_keeps_restart_only_settings() {
    let temp = tempfile::tempdir().unwrap();
    let state_dir = temp.path().join("state");
    let log_dir = temp.path().join("logs");
    let old_dir = log_dir.join("old-job");
    let old_log = old_dir.join("old.stdout.log");
    std::fs::create_dir_all(&old_dir).unwrap();
    std::fs::write(&old_log, "old").unwrap();
    make_file_old(&old_log);
    let config_path = temp.path().join("sundiald.yaml");
    let address = available_address();
    std::fs::write(
        &config_path,
        format!(
            "state_dir: {}\nlog_dir: {}\nservice_log: {}\napi_bind: {address}\nlog_retention_days: 1\njobs: []\n",
            state_dir.display(),
            log_dir.display(),
            temp.path().join("sundiald.log").display(),
        ),
    )
    .unwrap();

    let mut daemon = spawn_daemon(&config_path);
    wait_for_api(address).await;
    assert!(!old_log.exists());
    assert!(!old_dir.exists());

    let requested_address = available_address();
    std::fs::write(
        &config_path,
        format!(
            "state_dir: {}\nlog_dir: {}\nservice_log: {}\napi_bind: {requested_address}\njobs: []\n",
            state_dir.display(),
            log_dir.display(),
            temp.path().join("sundiald.log").display(),
        ),
    )
    .unwrap();
    let response = reqwest::Client::new()
        .post(format!("http://{address}/reload"))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), reqwest::StatusCode::BAD_REQUEST);
    assert!(
        reqwest::get(format!("http://{address}/status"))
            .await
            .unwrap()
            .status()
            .is_success()
    );

    stop_daemon(&mut daemon).await;
}

#[tokio::test]
async fn concurrent_runs_preserve_every_history_row_and_binary_log_byte() {
    let temp = tempfile::tempdir().unwrap();
    let state_dir = temp.path().join("state");
    let config_path = temp.path().join("sundiald.yaml");
    let address = available_address();
    let mut jobs = String::new();
    for index in 0..8 {
        jobs.push_str(&format!(
            "  - name: job-{index}\n    uuid: 00000000-0000-4000-8000-{index:012}\n    command: 'true'\n    trigger: manual\n"
        ));
    }
    jobs.push_str(
        "  - name: binary\n    uuid: cccccccc-cccc-4ccc-8ccc-cccccccccccc\n    command: |\n      printf '\\377\\376binary'\n    trigger: manual\n",
    );
    std::fs::write(
        &config_path,
        format!(
            "state_dir: {}\nlog_dir: {}\nservice_log: {}\napi_bind: {address}\njobs:\n{jobs}",
            state_dir.display(),
            temp.path().join("logs").display(),
            temp.path().join("sundiald.log").display(),
        ),
    )
    .unwrap();

    let mut daemon = spawn_daemon(&config_path);
    wait_for_api(address).await;
    let client = reqwest::Client::new();
    let mut requests = Vec::new();
    for name in (0..8)
        .map(|index| format!("job-{index}"))
        .chain(std::iter::once("binary".to_string()))
    {
        let client = client.clone();
        requests.push(tokio::spawn(async move {
            client
                .post(format!("http://{address}/jobs/{name}/run"))
                .send()
                .await
                .unwrap()
                .status()
        }));
    }
    for request in requests {
        assert!(request.await.unwrap().is_success());
    }

    let database = state_dir.join("history.sqlite3");
    wait_for_finished_rows(&database, 9).await;
    let log_path: String = Connection::open(&database)
        .unwrap()
        .query_row(
            "SELECT log_path FROM job_runs WHERE job_name = 'binary'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(std::fs::read(log_path).unwrap(), b"\xff\xfebinary");

    stop_daemon(&mut daemon).await;
}

#[tokio::test]
async fn restart_marks_a_crash_interrupted_run_finished() {
    let temp = tempfile::tempdir().unwrap();
    let state_dir = temp.path().join("state");
    let config_path = temp.path().join("sundiald.yaml");
    let address = available_address();
    std::fs::write(
        &config_path,
        format!(
            "state_dir: {}\nlog_dir: {}\nservice_log: {}\napi_bind: {address}\njobs:\n  - name: sleepy\n    uuid: dddddddd-dddd-4ddd-8ddd-dddddddddddd\n    command: 'sleep 60'\n    trigger: manual\n",
            state_dir.display(),
            temp.path().join("logs").display(),
            temp.path().join("sundiald.log").display(),
        ),
    )
    .unwrap();

    let mut daemon = spawn_daemon(&config_path);
    wait_for_api(address).await;
    assert!(
        reqwest::Client::new()
            .post(format!("http://{address}/jobs/sleepy/run"))
            .send()
            .await
            .unwrap()
            .status()
            .is_success()
    );

    let deadline = Instant::now() + Duration::from_secs(5);
    let child_pid = loop {
        let status: Value = reqwest::get(format!("http://{address}/status"))
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        if let Some(pid) = status["jobs"][0]["pid"].as_i64() {
            break pid as libc::pid_t;
        }
        assert!(Instant::now() < deadline, "job did not start");
        tokio::time::sleep(Duration::from_millis(25)).await;
    };
    assert_eq!(
        unsafe { libc::kill(daemon.id() as libc::pid_t, libc::SIGKILL) },
        0
    );
    assert!(!wait_for_exit(&mut daemon).await.success());
    let _ = unsafe { libc::kill(-child_pid, libc::SIGKILL) };

    let mut restarted = spawn_daemon(&config_path);
    wait_for_api(address).await;
    let connection = Connection::open(state_dir.join("history.sqlite3")).unwrap();
    let (status, finished_at): (String, Option<String>) = connection
        .query_row(
            "SELECT status, finished_at FROM job_runs WHERE job_name = 'sleepy'",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    assert_eq!(status, "interrupted");
    assert!(finished_at.is_some());

    stop_daemon(&mut restarted).await;
}
