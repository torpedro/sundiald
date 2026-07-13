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

use super::client::{api_base, encode_path_segment, fetch_status};
use crate::{config::SundialdConfig, service, state};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DetailMode {
    None,
    Log,
    History,
    Schedule,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum EntryKind {
    Job,
    Service,
}

#[derive(Debug, Clone)]
struct UiEntry {
    kind: EntryKind,
    uuid: uuid::Uuid,
    name: String,
    group: Option<String>,
    status: state::JobStatus,
    pid: Option<u32>,
    started_at: Option<chrono::DateTime<Local>>,
    finished_at: Option<chrono::DateTime<Local>>,
    exit_code: Option<i32>,
    last_error: Option<String>,
    terminated_by_signal: Option<String>,
    manual_pending: bool,
    trigger_label: String,
    expected_running: bool,
    next_label: String,
    next_runs: Vec<chrono::DateTime<Local>>,
    next_start: Option<chrono::DateTime<Local>>,
    next_stop: Option<chrono::DateTime<Local>>,
}

impl UiEntry {
    fn from_job(job: service::JobStatusResponse) -> Self {
        let trigger_label = match job.trigger.after.as_deref() {
            Some(upstream) => format!("after {upstream}"),
            None => job.trigger.kind.clone(),
        };
        let next_label = match job.trigger.kind.as_str() {
            "manual" => "manual".to_string(),
            "dependency" => trigger_label.clone(),
            _ => job
                .next_run
                .map(|time| format_time_with_delta(time, Local::now()))
                .unwrap_or_else(|| "none".to_string()),
        };
        Self {
            kind: EntryKind::Job,
            uuid: job.uuid,
            name: job.name,
            group: job.group,
            status: job.status,
            pid: job.pid,
            started_at: job.started_at,
            finished_at: job.finished_at,
            exit_code: job.exit_code,
            last_error: job.last_error,
            terminated_by_signal: job.terminated_by_signal,
            manual_pending: job.manual_pending,
            trigger_label,
            expected_running: false,
            next_label,
            next_runs: job.next_runs,
            next_start: None,
            next_stop: None,
        }
    }

    fn from_service(service: service::ServiceStatusResponse) -> Self {
        let next_label = match (service.next_start, service.next_stop) {
            (Some(start), Some(stop)) => format!(
                "start {} / stop {}",
                format_timestamp(start),
                format_timestamp(stop)
            ),
            (Some(start), None) => format!("start {}", format_timestamp(start)),
            (None, Some(stop)) => format!("stop {}", format_timestamp(stop)),
            (None, None) => "manual".to_string(),
        };
        Self {
            kind: EntryKind::Service,
            uuid: service.uuid,
            name: service.name,
            group: service.group,
            status: service.status,
            pid: service.pid,
            started_at: service.started_at,
            finished_at: service.finished_at,
            exit_code: service.exit_code,
            last_error: service.last_error,
            terminated_by_signal: service.terminated_by_signal,
            manual_pending: false,
            trigger_label: service.schedule,
            expected_running: service.expected_running,
            next_label,
            next_runs: Vec::new(),
            next_start: service.next_start,
            next_stop: service.next_stop,
        }
    }

    fn label(&self) -> &'static str {
        match self.kind {
            EntryKind::Job => "job",
            EntryKind::Service => "service",
        }
    }
}

#[derive(Debug)]
struct UiState {
    selected: usize,
    entries: Vec<UiEntry>,
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
            entries: Vec::new(),
            message: "ready".to_string(),
            detail_mode: DetailMode::None,
            detail_title: "Details".to_string(),
            detail_text:
                "Select a job or service, then press Enter for logs, h for history, or s for schedule."
                    .to_string(),
            detail_scroll: 0,
        }
    }

    fn selected_entry(&self) -> Option<&UiEntry> {
        self.entries.get(self.selected)
    }

    fn selected_action_target(&self) -> Option<(EntryKind, uuid::Uuid, String)> {
        self.selected_entry()
            .map(|entry| (entry.kind, entry.uuid, entry.name.clone()))
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
                state.selected = clamp_selected(state.selected, state.entries.len());
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
            if !state.entries.is_empty() {
                state.selected = (state.selected + 1) % state.entries.len();
            }
        }
        KeyCode::Up | KeyCode::Char('k') => {
            if !state.entries.is_empty() {
                state.selected = if state.selected == 0 {
                    state.entries.len() - 1
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
            if let Some((kind, uuid, name)) = state.selected_action_target() {
                let encoded_id = encode_path_segment(&uuid.to_string());
                let (path, message) = match kind {
                    EntryKind::Job => (
                        format!("/jobs/{encoded_id}/run"),
                        format!("queued manual run for {name}"),
                    ),
                    EntryKind::Service => (
                        format!("/services/{encoded_id}/start"),
                        format!("queued service start for {name}"),
                    ),
                };
                state.message = post_watch_action(config, &path, &message).await;
            }
        }
        KeyCode::Char('T') => {
            if let Some((kind, uuid, name)) = state.selected_action_target() {
                let encoded_id = encode_path_segment(&uuid.to_string());
                let path = match kind {
                    EntryKind::Job => format!("/jobs/{encoded_id}/terminate"),
                    EntryKind::Service => format!("/services/{encoded_id}/stop"),
                };
                state.message =
                    post_watch_action(config, &path, &format!("sent SIGTERM to {name}")).await;
            }
        }
        KeyCode::Char('K') => {
            if let Some((kind, uuid, name)) = state.selected_action_target() {
                let encoded_id = encode_path_segment(&uuid.to_string());
                let path = match kind {
                    EntryKind::Job => format!("/jobs/{encoded_id}/kill"),
                    EntryKind::Service => format!("/services/{encoded_id}/kill"),
                };
                state.message =
                    post_watch_action(config, &path, &format!("sent SIGKILL to {name}")).await;
            }
        }
        KeyCode::Char('R') => {
            state.message = post_watch_action(config, "/reload", "config reloaded").await;
            refresh_jobs(config, state).await;
        }
        KeyCode::Char('s') => {
            if let Some(entry) = state.selected_entry() {
                state.set_details(
                    DetailMode::Schedule,
                    format!("Schedule: {}", entry.name),
                    render_schedule(entry),
                );
            }
        }
        KeyCode::Char('h') => {
            if let Some((kind, uuid, name)) = state.selected_action_target() {
                let history = match kind {
                    EntryKind::Job => read_history(config, uuid).await,
                    EntryKind::Service => "history: service history is recorded in the run database but has no dedicated UI endpoint yet".to_string(),
                };
                state.set_details(DetailMode::History, format!("History: {name}"), history);
            }
        }
        KeyCode::Enter => {
            if let Some((kind, uuid, name)) = state.selected_action_target() {
                let log = read_recent_log(config, kind, uuid).await;
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
            state.entries = status
                .jobs
                .into_iter()
                .map(UiEntry::from_job)
                .chain(status.services.into_iter().map(UiEntry::from_service))
                .collect();
            state.selected = clamp_selected(state.selected, state.entries.len());
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

    for (group, entries) in group_entries(&state.entries) {
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

        for (entry_index, entry) in entries {
            if entry_index == state.selected {
                selected_row = Some(row_index);
            }
            rows.push(Row::new(vec![
                Cell::from(format!("  {} {}", entry.label(), entry.name)),
                status_cell(entry),
                Cell::from(entry.trigger_label.clone()),
                Cell::from(format_last_run(entry, now)),
                Cell::from(entry.next_label.clone()),
            ]));
            row_index += 1;
        }
    }

    if rows.is_empty() {
        rows.push(Row::new(vec![
            Cell::from("no jobs or services"),
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
        Row::new(["Name", "Status", "Trigger", "Last Run", "Next"])
            .style(Style::default().add_modifier(Modifier::BOLD)),
    )
    .block(
        Block::default()
            .title("Jobs and Services")
            .borders(Borders::ALL),
    )
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
    let keys = "arrows/j/k select  Enter log  h history  s schedule  r run/start  T term/stop  K kill  R reload  Backspace clear  q quit";
    frame.render_widget(
        Paragraph::new(keys).block(Block::default().title("Keys").borders(Borders::ALL)),
        area,
    );
}

fn group_entries(entries: &[UiEntry]) -> Vec<(Option<String>, Vec<(usize, &UiEntry)>)> {
    let mut groups: Vec<(Option<String>, Vec<(usize, &UiEntry)>)> = Vec::new();
    for (index, entry) in entries.iter().enumerate() {
        let group = entry.group.clone();
        if let Some((_, group_entries)) = groups.iter_mut().find(|(existing, _)| *existing == group)
        {
            group_entries.push((index, entry));
        } else {
            groups.push((group, vec![(index, entry)]));
        }
    }
    groups
}

fn high_level_status(entry: &UiEntry) -> &'static str {
    match &entry.status {
        state::JobStatus::Running => "running",
        state::JobStatus::Failed | state::JobStatus::StartFailed => "last run failed",
        state::JobStatus::Succeeded => "last run succeeded",
        state::JobStatus::Interrupted => "last run interrupted",
        state::JobStatus::Idle => "never run",
    }
}

fn compact_status(entry: &UiEntry) -> String {
    let mut parts = vec![match high_level_status(entry) {
        "last run succeeded" => "ok".to_string(),
        "last run failed" => "failed".to_string(),
        "last run interrupted" => "interrupted".to_string(),
        "never run" => "idle".to_string(),
        status => status.to_string(),
    }];

    if entry.manual_pending {
        parts.push("queued".to_string());
    }
    if matches!(entry.status, state::JobStatus::Running) {
        parts.push(
            entry
                .pid
                .map(|pid| format!("pid {pid}"))
                .unwrap_or_else(|| "pid ?".to_string()),
        );
    }
    if let Some(exit_code) = entry.exit_code {
        parts.push(format!("exit {exit_code}"));
    }
    if let Some(signal) = &entry.terminated_by_signal {
        parts.push(format!("signal {signal}"));
    }
    if entry.last_error.is_some()
        && !matches!(
            entry.status,
            state::JobStatus::Failed | state::JobStatus::StartFailed
        )
    {
        parts.push("warning".to_string());
    }

    parts.join(" ")
}

fn status_cell(entry: &UiEntry) -> Cell<'static> {
    Cell::from(compact_status(entry)).style(status_style(entry))
}

fn status_style(entry: &UiEntry) -> Style {
    if matches!(entry.kind, EntryKind::Service) {
        let running = matches!(entry.status, state::JobStatus::Running);
        return match (running, entry.expected_running) {
            (true, true) => Style::default().fg(Color::Green),
            (false, true) | (true, false) => Style::default().fg(Color::Red),
            (false, false) => match entry.status {
                state::JobStatus::Failed | state::JobStatus::StartFailed => {
                    Style::default().fg(Color::Red)
                }
                state::JobStatus::Interrupted => Style::default().fg(Color::Yellow),
                _ => Style::default(),
            },
        };
    }

    match entry.status {
        state::JobStatus::Succeeded => Style::default().fg(Color::Green),
        state::JobStatus::Failed | state::JobStatus::StartFailed => Style::default().fg(Color::Red),
        state::JobStatus::Running => Style::default().fg(Color::Yellow),
        state::JobStatus::Interrupted => Style::default().fg(Color::Yellow),
        state::JobStatus::Idle => Style::default(),
    }
}

fn format_last_run(entry: &UiEntry, now: chrono::DateTime<Local>) -> String {
    if matches!(entry.status, state::JobStatus::Running) {
        let Some(started) = entry.started_at else {
            return "never".to_string();
        };
        let elapsed = format_chrono_duration(now.signed_duration_since(started));
        return format!("{} (running for {elapsed})", format_timestamp(started));
    }

    let Some(time) = entry.finished_at.or(entry.started_at) else {
        return "never".to_string();
    };
    let ago = format_chrono_duration(now.signed_duration_since(time));

    match (entry.started_at, entry.finished_at) {
        (Some(started), Some(finished)) => {
            let took = format_chrono_duration(finished.signed_duration_since(started));
            format!("{} ({ago} ago, took {took})", format_timestamp(time))
        }
        _ => format!("{} ({ago} ago)", format_timestamp(time)),
    }
}

fn format_timestamp(time: chrono::DateTime<Local>) -> String {
    time.format("%Y-%m-%d %H:%M:%S").to_string()
}

fn format_time_with_delta(time: chrono::DateTime<Local>, now: chrono::DateTime<Local>) -> String {
    format!(
        "{} (in {})",
        format_timestamp(time),
        format_chrono_duration(time.signed_duration_since(now))
    )
}

fn format_chrono_duration(duration: chrono::Duration) -> String {
    let milliseconds = duration.num_milliseconds().max(0);
    format_duration_ms(milliseconds)
}

fn render_schedule(entry: &UiEntry) -> String {
    if entry.kind == EntryKind::Service {
        let mut output = format!("schedule: {} is {}", entry.name, entry.trigger_label);
        if let Some(start) = entry.next_start {
            output.push_str(&format!("\nnext start: {}", format_timestamp(start)));
        }
        if let Some(stop) = entry.next_stop {
            output.push_str(&format!("\nnext stop: {}", format_timestamp(stop)));
        }
        return output;
    }
    if entry.trigger_label == "manual" {
        return format!("trigger: {} is manual", entry.name);
    }
    if entry.trigger_label.starts_with("after ") {
        return format!("trigger: {} runs {}", entry.name, entry.trigger_label);
    }
    if entry.next_runs.is_empty() {
        return format!("schedule: no upcoming runs found for {}", entry.name);
    }

    let mut output = format!(
        "schedule: next {} run(s) for {}",
        entry.next_runs.len(),
        entry.name
    );
    for (index, run) in entry.next_runs.iter().enumerate() {
        output.push_str(&format!(
            "\n{:>2}. {}",
            index + 1,
            run.format("%Y-%m-%d %H:%M:%S %:z")
        ));
    }
    output
}

async fn read_recent_log(config: &SundialdConfig, kind: EntryKind, uuid: uuid::Uuid) -> String {
    let encoded_id = encode_path_segment(&uuid.to_string());
    let path = match kind {
        EntryKind::Job => format!("/jobs/{encoded_id}/logs/latest?tail=40"),
        EntryKind::Service => format!("/services/{encoded_id}/logs/latest?tail=40"),
    };
    let response = reqwest::Client::new()
        .get(format!("{}{}", api_base(config), path))
        .send()
        .await;
    match response {
        Ok(response) if response.status().is_success() => {
            match response.json::<service::LogResponse>().await {
                Ok(log) => render_log_response(&log),
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

fn render_log_response(log: &service::LogResponse) -> String {
    let mut output = format!(
        "stdout log: {}\n---- stdout ----\n{}",
        log.log_path.display(),
        log.content
    );
    if let (Some(path), Some(content)) = (&log.stderr_log_path, &log.stderr_content) {
        output.push_str(&format!(
            "\nstderr log: {}\n---- stderr ----\n{}",
            path.display(),
            content
        ));
    }
    output
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

    fn job_entry() -> UiEntry {
        UiEntry::from_job(service::JobStatusResponse {
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
        })
    }

    fn service_entry(status: crate::state::JobStatus, expected_running: bool) -> UiEntry {
        UiEntry {
            kind: EntryKind::Service,
            uuid: Uuid::new_v4(),
            name: "worker".to_string(),
            group: None,
            status,
            pid: None,
            started_at: None,
            finished_at: None,
            exit_code: None,
            last_error: None,
            terminated_by_signal: None,
            manual_pending: false,
            trigger_label: "permanent".to_string(),
            expected_running,
            next_label: "manual".to_string(),
            next_runs: Vec::new(),
            next_start: None,
            next_stop: None,
        }
    }

    #[test]
    fn render_schedule_lists_next_runs_for_selected_job() {
        let mut job = job_entry();
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
        let mut job = job_entry();
        job.trigger_label = "manual".to_string();

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
        let mut job = job_entry();
        job.trigger_label = "after build".to_string();

        assert_eq!(render_schedule(&job), "trigger: example runs after build");
    }

    #[test]
    fn status_cell_colors_success_and_failure() {
        let mut success = job_entry();
        success.status = crate::state::JobStatus::Succeeded;
        let mut failed = job_entry();
        failed.status = crate::state::JobStatus::Failed;

        assert_eq!(status_style(&success).fg, Some(Color::Green));
        assert_eq!(status_style(&failed).fg, Some(Color::Red));
    }

    #[test]
    fn service_status_colors_expected_runtime() {
        let running_expected = service_entry(crate::state::JobStatus::Running, true);
        let idle_expected = service_entry(crate::state::JobStatus::Idle, true);
        let running_unexpected = service_entry(crate::state::JobStatus::Running, false);
        let idle_unexpected = service_entry(crate::state::JobStatus::Idle, false);

        assert_eq!(status_style(&running_expected).fg, Some(Color::Green));
        assert_eq!(status_style(&idle_expected).fg, Some(Color::Red));
        assert_eq!(status_style(&running_unexpected).fg, Some(Color::Red));
        assert_eq!(status_style(&idle_unexpected).fg, None);
    }

    #[test]
    fn grouping_preserves_job_order_and_group_labels() {
        let mut first = job_entry();
        first.name = "inline".to_string();
        let mut second = job_entry();
        second.name = "cleanup".to_string();
        second.group = Some("maintenance".to_string());
        let entries = vec![first, second];

        let groups = group_entries(&entries);

        assert_eq!(groups.len(), 2);
        assert_eq!(groups[0].0, None);
        assert_eq!(groups[0].1[0].1.name, "inline");
        assert_eq!(groups[1].0.as_deref(), Some("maintenance"));
        assert_eq!(groups[1].1[0].1.name, "cleanup");
    }
}
