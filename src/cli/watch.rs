use std::{
    io::{self, IsTerminal},
    thread,
};

use anyhow::Result;
use chrono::Local;
use crossterm::{
    event::{self, Event, KeyCode, KeyEvent, KeyModifiers},
    execute,
    terminal::{EnterAlternateScreen, LeaveAlternateScreen, disable_raw_mode, enable_raw_mode},
};
use ratatui::{
    Frame, Terminal,
    backend::CrosstermBackend,
    layout::{Constraint, Direction, Layout, Rect},
    style::{Color, Modifier, Style},
    text::{Line, Span},
    widgets::{Block, Borders, Cell, Paragraph, Row, Table, TableState, Wrap},
};
use tokio::{
    sync::mpsc,
    time::{self, Duration},
};

use super::{
    client::{api_base, encode_path_segment, fetch_status},
    render::{format_last_run, format_next_run_plain, group_jobs, high_level_status},
};
use crate::{config::SundialdConfig, service, state};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DetailMode {
    None,
    Log,
    History,
    Schedule,
}

#[derive(Debug)]
struct UiState {
    selected: usize,
    jobs: Vec<service::JobStatusResponse>,
    message: String,
    detail_mode: DetailMode,
    detail_title: String,
    detail_text: String,
    detail_scroll: u16,
}

impl UiState {
    fn new() -> Self {
        Self {
            selected: 0,
            jobs: Vec::new(),
            message: "ready".to_string(),
            detail_mode: DetailMode::None,
            detail_title: "Details".to_string(),
            detail_text:
                "Select a job, then press Enter for logs, h for history, or s for schedule."
                    .to_string(),
            detail_scroll: 0,
        }
    }

    fn selected_job(&self) -> Option<&service::JobStatusResponse> {
        self.jobs.get(self.selected)
    }

    fn selected_job_action_target(&self) -> Option<(uuid::Uuid, String)> {
        self.selected_job().map(|job| (job.uuid, job.name.clone()))
    }

    fn set_details(&mut self, mode: DetailMode, title: impl Into<String>, text: impl Into<String>) {
        self.detail_mode = mode;
        self.detail_title = title.into();
        self.detail_text = text.into();
        self.detail_scroll = 0;
    }

    fn clear_details(&mut self) {
        self.set_details(DetailMode::None, "Details", "");
    }
}

pub(crate) async fn watch_status(config: SundialdConfig) -> Result<()> {
    let mut terminal = WatchTerminal::enter()?;
    let mut state = UiState::new();
    let mut interval = time::interval(Duration::from_secs(1));
    let mut keys = spawn_key_reader();

    refresh_jobs(&config, &mut state).await;
    terminal.draw(|frame| draw_ui(frame, &config, &state))?;

    loop {
        tokio::select! {
            _ = tokio::signal::ctrl_c() => break,
            _ = interval.tick() => {
                refresh_jobs(&config, &mut state).await;
                terminal.draw(|frame| draw_ui(frame, &config, &state))?;
            }
            key = keys.recv() => {
                let Some(key) = key else {
                    continue;
                };
                if handle_key(key, &config, &mut state).await {
                    break;
                }
                state.selected = clamp_selected(state.selected, state.jobs.len());
                terminal.draw(|frame| draw_ui(frame, &config, &state))?;
            }
        }
    }

    Ok(())
}

async fn handle_key(key: KeyEvent, config: &SundialdConfig, state: &mut UiState) -> bool {
    match key.code {
        KeyCode::Char('q') | KeyCode::Esc => return true,
        KeyCode::Char('c') if key.modifiers.contains(KeyModifiers::CONTROL) => return true,
        KeyCode::Down | KeyCode::Char('j') => {
            if !state.jobs.is_empty() {
                state.selected = (state.selected + 1) % state.jobs.len();
            }
        }
        KeyCode::Up | KeyCode::Char('k') => {
            if !state.jobs.is_empty() {
                state.selected = if state.selected == 0 {
                    state.jobs.len() - 1
                } else {
                    state.selected - 1
                };
            }
        }
        KeyCode::PageDown => {
            state.detail_scroll = state.detail_scroll.saturating_add(8);
        }
        KeyCode::PageUp => {
            state.detail_scroll = state.detail_scroll.saturating_sub(8);
        }
        KeyCode::Char('r') => {
            if let Some((uuid, name)) = state.selected_job_action_target() {
                let encoded_job_id = encode_path_segment(&uuid.to_string());
                state.message = post_watch_action(
                    config,
                    &format!("/jobs/{encoded_job_id}/run"),
                    &format!("queued manual run for {name}"),
                )
                .await;
            }
        }
        KeyCode::Char('T') => {
            if let Some((uuid, name)) = state.selected_job_action_target() {
                let encoded_job_id = encode_path_segment(&uuid.to_string());
                state.message = post_watch_action(
                    config,
                    &format!("/jobs/{encoded_job_id}/terminate"),
                    &format!("sent SIGTERM to {name}"),
                )
                .await;
            }
        }
        KeyCode::Char('K') => {
            if let Some((uuid, name)) = state.selected_job_action_target() {
                let encoded_job_id = encode_path_segment(&uuid.to_string());
                state.message = post_watch_action(
                    config,
                    &format!("/jobs/{encoded_job_id}/kill"),
                    &format!("sent SIGKILL to {name}"),
                )
                .await;
            }
        }
        KeyCode::Char('R') => {
            state.message = post_watch_action(config, "/reload", "config reloaded").await;
            refresh_jobs(config, state).await;
        }
        KeyCode::Char('s') => {
            if let Some(job) = state.selected_job() {
                state.set_details(
                    DetailMode::Schedule,
                    format!("Schedule: {}", job.name),
                    render_schedule(job),
                );
            }
        }
        KeyCode::Char('h') => {
            if let Some((uuid, name)) = state.selected_job_action_target() {
                let history = read_history(config, uuid).await;
                state.set_details(DetailMode::History, format!("History: {name}"), history);
            }
        }
        KeyCode::Enter => {
            if let Some((uuid, name)) = state.selected_job_action_target() {
                let log = read_recent_log(config, uuid).await;
                state.set_details(DetailMode::Log, format!("Latest Log: {name}"), log);
            }
        }
        KeyCode::Backspace => {
            state.clear_details();
        }
        _ => {}
    }

    false
}

async fn refresh_jobs(config: &SundialdConfig, state: &mut UiState) {
    match fetch_status(config).await {
        Ok(status) => {
            state.jobs = status.jobs;
            state.selected = clamp_selected(state.selected, state.jobs.len());
        }
        Err(error) => {
            state.message = format!("status refresh failed: {error}");
        }
    }
}

fn draw_ui(frame: &mut Frame<'_>, config: &SundialdConfig, state: &UiState) {
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(3),
            Constraint::Min(8),
            Constraint::Length(10),
            Constraint::Length(3),
        ])
        .split(frame.area());

    draw_header(frame, chunks[0], config, state);
    draw_jobs(frame, chunks[1], state);
    draw_details(frame, chunks[2], state);
    draw_footer(frame, chunks[3]);
}

fn draw_header(frame: &mut Frame<'_>, area: Rect, config: &SundialdConfig, state: &UiState) {
    let title = Line::from(vec![
        Span::styled(
            "sundiald",
            Style::default()
                .fg(Color::Cyan)
                .add_modifier(Modifier::BOLD),
        ),
        Span::raw(format!("  {}  ", api_base(config))),
        Span::styled(&state.message, Style::default().fg(Color::Gray)),
    ]);
    frame.render_widget(
        Paragraph::new(title).block(Block::default().borders(Borders::ALL)),
        area,
    );
}

fn draw_jobs(frame: &mut Frame<'_>, area: Rect, state: &UiState) {
    let now = Local::now();
    let mut selected_row = None;
    let mut row_index = 0usize;
    let mut rows = Vec::new();

    for (group, jobs) in group_jobs(&state.jobs) {
        rows.push(
            Row::new(vec![
                Cell::from(group.unwrap_or_else(|| "inline".to_string())),
                Cell::from(""),
                Cell::from(""),
                Cell::from(""),
                Cell::from(""),
            ])
            .style(
                Style::default()
                    .fg(Color::Cyan)
                    .add_modifier(Modifier::BOLD),
            ),
        );
        row_index += 1;

        for (job_index, job) in jobs {
            if job_index == state.selected {
                selected_row = Some(row_index);
            }
            rows.push(Row::new(vec![
                Cell::from(format!("  {}", job.name)),
                status_cell(job),
                Cell::from(compact_trigger(job)),
                Cell::from(format_last_run(job, now)),
                Cell::from(format_next_run_plain(job, now)),
            ]));
            row_index += 1;
        }
    }

    if rows.is_empty() {
        rows.push(Row::new(vec![
            Cell::from("no jobs"),
            Cell::from(""),
            Cell::from(""),
            Cell::from(""),
            Cell::from(""),
        ]));
    }

    let table = Table::new(
        rows,
        [
            Constraint::Percentage(24),
            Constraint::Percentage(16),
            Constraint::Percentage(16),
            Constraint::Percentage(24),
            Constraint::Percentage(20),
        ],
    )
    .header(
        Row::new(["Job", "Status", "Trigger", "Last Run", "Next Run"])
            .style(Style::default().add_modifier(Modifier::BOLD)),
    )
    .block(Block::default().title("Jobs").borders(Borders::ALL))
    .row_highlight_style(
        Style::default()
            .bg(Color::DarkGray)
            .add_modifier(Modifier::BOLD),
    )
    .highlight_symbol("> ");

    let mut table_state = TableState::default();
    table_state.select(selected_row);
    frame.render_stateful_widget(table, area, &mut table_state);
}

fn draw_details(frame: &mut Frame<'_>, area: Rect, state: &UiState) {
    let title = match state.detail_mode {
        DetailMode::None => state.detail_title.clone(),
        DetailMode::Log => format!("{}  PgUp/PgDn scroll", state.detail_title),
        DetailMode::History => state.detail_title.clone(),
        DetailMode::Schedule => state.detail_title.clone(),
    };
    let details = Paragraph::new(state.detail_text.as_str())
        .block(Block::default().title(title).borders(Borders::ALL))
        .wrap(Wrap { trim: false })
        .scroll((state.detail_scroll, 0));
    frame.render_widget(details, area);
}

fn draw_footer(frame: &mut Frame<'_>, area: Rect) {
    let keys = "arrows/j/k select  Enter log  h history  s schedule  r run  T term  K kill  R reload  Backspace clear  q quit";
    frame.render_widget(
        Paragraph::new(keys).block(Block::default().title("Keys").borders(Borders::ALL)),
        area,
    );
}

fn compact_status(job: &service::JobStatusResponse) -> String {
    let mut parts = vec![match high_level_status(job) {
        "last run succeeded" => "ok".to_string(),
        "last run failed" => "failed".to_string(),
        "last run interrupted" => "interrupted".to_string(),
        "never run" => "idle".to_string(),
        status => status.to_string(),
    }];

    if job.manual_pending {
        parts.push("queued".to_string());
    }
    if matches!(job.status, state::JobStatus::Running) {
        parts.push(
            job.pid
                .map(|pid| format!("pid {pid}"))
                .unwrap_or_else(|| "pid ?".to_string()),
        );
    }
    if let Some(exit_code) = job.exit_code {
        parts.push(format!("exit {exit_code}"));
    }
    if let Some(signal) = &job.terminated_by_signal {
        parts.push(format!("signal {signal}"));
    }

    parts.join(" ")
}

fn status_cell(job: &service::JobStatusResponse) -> Cell<'static> {
    Cell::from(compact_status(job)).style(status_style(job))
}

fn status_style(job: &service::JobStatusResponse) -> Style {
    match job.status {
        state::JobStatus::Succeeded => Style::default().fg(Color::Green),
        state::JobStatus::Failed | state::JobStatus::StartFailed => Style::default().fg(Color::Red),
        state::JobStatus::Running => Style::default().fg(Color::Yellow),
        state::JobStatus::Interrupted => Style::default().fg(Color::Yellow),
        state::JobStatus::Idle => Style::default(),
    }
}

fn compact_trigger(job: &service::JobStatusResponse) -> String {
    match job.trigger.after.as_deref() {
        Some(upstream) => format!("after {upstream}"),
        None => job.trigger.kind.clone(),
    }
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

async fn read_recent_log(config: &SundialdConfig, job_uuid: uuid::Uuid) -> String {
    let encoded_job_id = encode_path_segment(&job_uuid.to_string());
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

async fn read_history(config: &SundialdConfig, job_uuid: uuid::Uuid) -> String {
    let encoded_job_id = encode_path_segment(&job_uuid.to_string());
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

type WatchBackend = CrosstermBackend<io::Stdout>;

struct WatchTerminal {
    terminal: Terminal<WatchBackend>,
}

impl WatchTerminal {
    fn enter() -> Result<Self> {
        if !io::stdin().is_terminal() || !io::stdout().is_terminal() {
            anyhow::bail!("ui requires an interactive terminal; use `ui --once` for plain output");
        }
        enable_raw_mode()?;
        let mut stdout = io::stdout();
        execute!(stdout, EnterAlternateScreen)?;
        let terminal = Terminal::new(CrosstermBackend::new(stdout))?;
        Ok(Self { terminal })
    }

    fn draw<F>(&mut self, draw: F) -> Result<()>
    where
        F: FnOnce(&mut Frame<'_>),
    {
        self.terminal.draw(draw)?;
        Ok(())
    }
}

impl Drop for WatchTerminal {
    fn drop(&mut self) {
        let _ = disable_raw_mode();
        let _ = execute!(self.terminal.backend_mut(), LeaveAlternateScreen);
        let _ = self.terminal.show_cursor();
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

    #[test]
    fn selection_clamps_after_job_removal() {
        assert_eq!(clamp_selected(3, 2), 1);
        assert_eq!(clamp_selected(3, 0), 0);
    }

    #[test]
    fn clear_details_resets_mode_and_text() {
        let mut state = UiState::new();
        state.set_details(DetailMode::Log, "Latest Log", "content");

        state.clear_details();

        assert_eq!(state.detail_mode, DetailMode::None);
        assert!(state.detail_text.is_empty());
        assert_eq!(state.detail_scroll, 0);
    }

    #[test]
    fn compact_trigger_reports_dependency_target() {
        let mut job = job_response();
        job.trigger.kind = "dependency".to_string();
        job.trigger.after = Some("build".to_string());

        assert_eq!(compact_trigger(&job), "after build");
    }

    #[test]
    fn status_cell_colors_success_and_failure() {
        let mut success = job_response();
        success.status = crate::state::JobStatus::Succeeded;
        let mut failed = job_response();
        failed.status = crate::state::JobStatus::Failed;

        assert_eq!(status_style(&success).fg, Some(Color::Green));
        assert_eq!(status_style(&failed).fg, Some(Color::Red));
    }

    #[test]
    fn grouping_preserves_job_order_and_group_labels() {
        let mut first = job_response();
        first.name = "inline".to_string();
        let mut second = job_response();
        second.name = "cleanup".to_string();
        second.group = Some("maintenance".to_string());
        let jobs = vec![first, second];

        let groups = group_jobs(&jobs);

        assert_eq!(groups.len(), 2);
        assert_eq!(groups[0].0, None);
        assert_eq!(groups[0].1[0].1.name, "inline");
        assert_eq!(groups[1].0.as_deref(), Some("maintenance"));
        assert_eq!(groups[1].1[0].1.name, "cleanup");
    }
}
