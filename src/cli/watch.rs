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
                            let encoded_job_name = encode_path_segment(&job_name);
                            last_command = post_watch_action(
                                &config,
                                &format!("/jobs/{encoded_job_name}/run"),
                                &format!("queued manual run for {job_name}"),
                            )
                            .await;
                        }
                    }
                    KeyCode::Char('T') => {
                        if let Some(job) = jobs.get(selected) {
                            let job_name = job.name.clone();
                            let encoded_job_name = encode_path_segment(&job_name);
                            last_command = post_watch_action(
                                &config,
                                &format!("/jobs/{encoded_job_name}/terminate"),
                                &format!("sent SIGTERM to {job_name}"),
                            )
                            .await;
                        }
                    }
                    KeyCode::Char('K') => {
                        if let Some(job) = jobs.get(selected) {
                            let job_name = job.name.clone();
                            let encoded_job_name = encode_path_segment(&job_name);
                            last_command = post_watch_action(
                                &config,
                                &format!("/jobs/{encoded_job_name}/kill"),
                                &format!("sent SIGKILL to {job_name}"),
                            )
                            .await;
                        }
                    }
                    KeyCode::Char('R') => {
                        last_command =
                            post_watch_action(&config, "/reload", "config reloaded").await;
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
            anyhow::bail!("status --watch requires an interactive terminal");
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
