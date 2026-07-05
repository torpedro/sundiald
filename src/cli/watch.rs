use std::{
    io::{self, IsTerminal, Write},
    thread,
};

use anyhow::Result;
use crossterm::event::{self, Event, KeyCode, KeyEvent, KeyModifiers};
use tokio::{
    sync::mpsc,
    time::{self, Duration},
};

use super::{
    client::{api_base, encode_path_segment},
    render::render_status,
};
use crate::{config::SundialdConfig, service};

pub(crate) async fn watch_status(config: SundialdConfig) -> Result<()> {
    let _terminal = WatchTerminal::enter()?;
    let mut selected = 0usize;
    let mut last_command = String::from("last command: none");
    let mut interval = time::interval(Duration::from_secs(1));
    let mut keys = spawn_key_reader();

    // The job list and names come from the live `/status` response on every
    // redraw, not from the config loaded once at startup: the server's job
    // list can change under us via `reload` (renames, additions, removals),
    // and actions must target the name the server currently knows about.
    let mut jobs = redraw_status(&config, Some(selected), Some(&last_command)).await?;

    loop {
        tokio::select! {
            _ = tokio::signal::ctrl_c() => break,
            _ = interval.tick() => {
                jobs = redraw_status(&config, Some(selected), Some(&last_command)).await?;
                selected = clamp_selected(selected, jobs.len());
            }
            key = keys.recv() => {
                let Some(key) = key else {
                    continue;
                };
                match key.code {
                    KeyCode::Char('q') | KeyCode::Esc => break,
                    KeyCode::Char('c') if key.modifiers.contains(KeyModifiers::CONTROL) => break,
                    KeyCode::Down | KeyCode::Char('j') => {
                        if !jobs.is_empty() {
                            selected = (selected + 1) % jobs.len();
                        }
                    }
                    KeyCode::Up | KeyCode::Char('k') => {
                        if !jobs.is_empty() {
                            selected = if selected == 0 {
                                jobs.len() - 1
                            } else {
                                selected - 1
                            };
                        }
                    }
                    KeyCode::Char('r') => {
                        if let Some(job) = jobs.get(selected) {
                            let job_name = job.name.clone();
                            let encoded_job_id = encode_path_segment(&job.uuid.to_string());
                            last_command = post_watch_action(
                                &config,
                                &format!("/jobs/{encoded_job_id}/run"),
                                &format!("queued manual run for {job_name}"),
                            )
                            .await;
                        }
                    }
                    KeyCode::Char('T') => {
                        if let Some(job) = jobs.get(selected) {
                            let job_name = job.name.clone();
                            let encoded_job_id = encode_path_segment(&job.uuid.to_string());
                            last_command = post_watch_action(
                                &config,
                                &format!("/jobs/{encoded_job_id}/terminate"),
                                &format!("sent SIGTERM to {job_name}"),
                            )
                            .await;
                        }
                    }
                    KeyCode::Char('K') => {
                        if let Some(job) = jobs.get(selected) {
                            let job_name = job.name.clone();
                            let encoded_job_id = encode_path_segment(&job.uuid.to_string());
                            last_command = post_watch_action(
                                &config,
                                &format!("/jobs/{encoded_job_id}/kill"),
                                &format!("sent SIGKILL to {job_name}"),
                            )
                            .await;
                        }
                    }
                    KeyCode::Char('R') => {
                        last_command =
                            post_watch_action(&config, "/reload", "config reloaded").await;
                    }
                    KeyCode::Char('s') => {
                        if let Some(job) = jobs.get(selected) {
                            last_command = render_schedule(job);
                        }
                    }
                    KeyCode::Char('h') => {
                        if let Some(job) = jobs.get(selected) {
                            last_command = read_history(&config, job).await;
                        }
                    }
                    KeyCode::Enter => {
                        if let Some(job) = jobs.get(selected) {
                            last_command = read_recent_log(&config, job).await;
                        }
                    }
                    KeyCode::Backspace => {
                        last_command.clear();
                    }
                    _ => {}
                }
                jobs = redraw_status(&config, Some(selected), Some(&last_command)).await?;
                selected = clamp_selected(selected, jobs.len());
            }
        }
    }

    Ok(())
}

fn render_schedule(job: &service::JobStatusResponse) -> String {
    if job.trigger.kind == "manual" {
        return format!("trigger: {} is manual", job.name);
    }
    if job.trigger.kind == "dependency" {
        return job
            .trigger
            .after
            .as_deref()
            .map(|upstream| format!("trigger: {} runs after {upstream}", job.name))
            .unwrap_or_else(|| format!("trigger: {} runs after an unknown job", job.name));
    }
    if job.next_runs.is_empty() {
        return format!("schedule: no upcoming runs found for {}", job.name);
    }

    let mut output = format!(
        "schedule: next {} run(s) for {}",
        job.next_runs.len(),
        job.name
    );
    for (index, run) in job.next_runs.iter().enumerate() {
        output.push_str(&format!(
            "\n{:>2}. {}",
            index + 1,
            run.format("%Y-%m-%d %H:%M:%S %:z")
        ));
    }
    output
}

async fn read_recent_log(config: &SundialdConfig, job: &service::JobStatusResponse) -> String {
    let encoded_job_id = encode_path_segment(&job.uuid.to_string());
    let response = reqwest::Client::new()
        .get(format!(
            "{}/jobs/{encoded_job_id}/logs/latest?tail=40",
            api_base(config)
        ))
        .send()
        .await;
    match response {
        Ok(response) if response.status().is_success() => {
            match response.json::<service::LogResponse>().await {
                Ok(log) => format!(
                    "log: {}\n---- stdout/stderr ----\n{}",
                    log.log_path.display(),
                    log.content
                ),
                Err(error) => format!("log: failed to parse api response: {error}"),
            }
        }
        Ok(response) => {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            format!("log: rejected: HTTP {status}: {body}")
        }
        Err(error) => format!("log: failed to reach api: {error}"),
    }
}

async fn read_history(config: &SundialdConfig, job: &service::JobStatusResponse) -> String {
    let encoded_job_id = encode_path_segment(&job.uuid.to_string());
    let response = reqwest::Client::new()
        .get(format!(
            "{}/jobs/{encoded_job_id}/history?limit=10",
            api_base(config)
        ))
        .send()
        .await;
    match response {
        Ok(response) if response.status().is_success() => {
            match response.json::<service::HistoryResponse>().await {
                Ok(history) => render_history(&history),
                Err(error) => format!("history: failed to parse api response: {error}"),
            }
        }
        Ok(response) => {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            format!("history: rejected: HTTP {status}: {body}")
        }
        Err(error) => format!("history: failed to reach api: {error}"),
    }
}

fn render_history(history: &service::HistoryResponse) -> String {
    if history.runs.is_empty() {
        return format!("history: no runs recorded for {}", history.job);
    }

    let mut output = format!(
        "history: last {} run(s) for {}",
        history.runs.len(),
        history.job
    );
    for run in &history.runs {
        let status = run.status.as_deref().unwrap_or("running");
        let exit = run
            .exit_code
            .map(|code| code.to_string())
            .unwrap_or_else(|| "-".to_string());
        let duration = run
            .duration_ms
            .map(format_duration_ms)
            .unwrap_or_else(|| "-".to_string());
        output.push_str(&format!(
            "\n{:>4}. {} {} status={} exit={} duration={}",
            run.id,
            run.triggered_at.format("%Y-%m-%d %H:%M:%S %:z"),
            run.trigger_kind,
            status,
            exit,
            duration
        ));
        if let Some(error) = &run.error {
            output.push_str(&format!(" error={error}"));
        }
    }
    output
}

fn format_duration_ms(duration_ms: i64) -> String {
    let seconds = duration_ms / 1_000;
    let milliseconds = duration_ms % 1_000;
    format!("{seconds}.{milliseconds:03}s")
}

/// Fire-and-report POST used by watch mode's key handlers: unlike the
/// non-interactive commands, a failure here shouldn't exit the process, just
/// update the status line with what happened.
async fn post_watch_action(config: &SundialdConfig, path: &str, success_message: &str) -> String {
    let response = reqwest::Client::new()
        .post(format!("{}{path}", api_base(config)))
        .send()
        .await;
    match response {
        Ok(response) if response.status().is_success() => {
            format!("last command: {success_message}")
        }
        Ok(response) => {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            format!("last command: rejected: HTTP {status}: {body}")
        }
        Err(error) => {
            format!("last command: failed to reach api: {error}")
        }
    }
}

fn clamp_selected(selected: usize, job_count: usize) -> usize {
    if job_count == 0 {
        0
    } else {
        selected.min(job_count - 1)
    }
}

fn spawn_key_reader() -> mpsc::UnboundedReceiver<KeyEvent> {
    let (tx, rx) = mpsc::unbounded_channel();
    thread::spawn(move || {
        while let Ok(event) = event::read() {
            if let Event::Key(key) = event {
                if tx.send(key).is_err() {
                    break;
                }
            }
        }
    });
    rx
}

/// Redraws the watch-mode screen and returns the job list from the
/// `/status` response that was just rendered, so the caller can act on
/// (and select among) the server's current jobs rather than a stale local
/// copy of the config.
async fn redraw_status(
    config: &SundialdConfig,
    selected: Option<usize>,
    last_command: Option<&str>,
) -> Result<Vec<service::JobStatusResponse>> {
    let (frame, jobs) = render_status(config, selected, last_command).await?;
    let mut stdout = io::stdout().lock();
    write!(stdout, "\x1B[H")?;
    for line in frame.lines() {
        write!(stdout, "\r\x1B[2K{line}\r\n")?;
    }
    write!(stdout, "\x1B[J")?;
    stdout.flush()?;
    Ok(jobs)
}

struct WatchTerminal;

impl WatchTerminal {
    fn enter() -> Result<Self> {
        if !io::stdin().is_terminal() || !io::stdout().is_terminal() {
            anyhow::bail!("ui requires an interactive terminal; use `ui --once` for plain output");
        }
        crossterm::terminal::enable_raw_mode()?;
        let mut stdout = io::stdout().lock();
        write!(stdout, "\x1B[?1049h\x1B[?25l\x1B[H")?;
        stdout.flush()?;
        Ok(Self)
    }
}

impl Drop for WatchTerminal {
    fn drop(&mut self) {
        let mut stdout = io::stdout().lock();
        let _ = write!(stdout, "\x1B[?25h\x1B[?1049l");
        let _ = stdout.flush();
        let _ = crossterm::terminal::disable_raw_mode();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;
    use uuid::Uuid;

    fn job_response() -> service::JobStatusResponse {
        service::JobStatusResponse {
            uuid: Uuid::new_v4(),
            name: "example".to_string(),
            group: None,
            status: crate::state::JobStatus::Idle,
            pid: None,
            started_at: None,
            finished_at: None,
            exit_code: None,
            log_path: None,
            last_error: None,
            terminated_by_signal: None,
            next_run: None,
            next_runs: Vec::new(),
            trigger: service::TriggerStatusResponse {
                kind: "schedule".to_string(),
                after: None,
            },
            manual_pending: false,
        }
    }

    #[test]
    fn render_schedule_lists_next_runs_for_selected_job() {
        let mut job = job_response();
        job.next_runs = vec![
            chrono::Local.with_ymd_and_hms(2026, 1, 1, 3, 0, 0).unwrap(),
            chrono::Local.with_ymd_and_hms(2026, 1, 2, 3, 0, 0).unwrap(),
        ];

        let output = render_schedule(&job);

        assert!(output.contains("schedule: next 2 run(s) for example"));
        assert!(output.contains(" 1. 2026-01-01 03:00:00"));
        assert!(output.contains(" 2. 2026-01-02 03:00:00"));
    }

    #[test]
    fn render_schedule_reports_manual_trigger_jobs() {
        let mut job = job_response();
        job.trigger.kind = "manual".to_string();

        assert_eq!(render_schedule(&job), "trigger: example is manual");
    }
}
