use std::{
    collections::{HashMap, HashSet},
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
    widgets::{Block, Borders, Cell, Clear, Paragraph, Row, Table, TableState, Tabs, Wrap},
};
use tokio::{
    sync::mpsc,
    time::{self, Duration},
};

use super::client::{api_base, api_client, authorize, encode_path_segment, fetch_status};
use crate::{config::SundialdConfig, service, state};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DetailMode {
    Summary,
    Log,
    History,
    Schedule,
}

impl DetailMode {
    fn index(self) -> usize {
        match self {
            Self::Summary => 0,
            Self::Log => 1,
            Self::History => 2,
            Self::Schedule => 3,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum EntryKind {
    Job,
    Service,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FilterMode {
    All,
    Running,
    Failed,
    Unexpected,
}

impl FilterMode {
    fn next(self) -> Self {
        match self {
            Self::All => Self::Running,
            Self::Running => Self::Failed,
            Self::Failed => Self::Unexpected,
            Self::Unexpected => Self::All,
        }
    }

    fn label(self) -> &'static str {
        match self {
            Self::All => "all",
            Self::Running => "running",
            Self::Failed => "failed",
            Self::Unexpected => "unexpected",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Focus {
    Table,
    Details,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum InputMode {
    Normal,
    Search,
    ConfirmKill { uuid: uuid::Uuid, name: String },
    Help,
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
}

#[derive(Debug)]
struct UiState {
    selected_uuid: Option<uuid::Uuid>,
    entries: Vec<UiEntry>,
    message: String,
    message_expires_at: Option<time::Instant>,
    connected: bool,
    last_refresh: Option<chrono::DateTime<Local>>,
    status_pending: bool,
    pending_actions: HashMap<uuid::Uuid, String>,
    reload_pending: bool,
    filter: FilterMode,
    search: String,
    collapsed_groups: HashSet<(EntryKind, String)>,
    input_mode: InputMode,
    focus: Focus,
    detail_mode: DetailMode,
    detail_uuid: Option<uuid::Uuid>,
    detail_loading: bool,
    detail_error: Option<String>,
    detail_log: Option<service::LogResponse>,
    detail_history: Option<service::HistoryResponse>,
    detail_scroll: u16,
    log_follow: bool,
}

impl UiState {
    fn new() -> Self {
        Self {
            selected_uuid: None,
            entries: Vec::new(),
            message: "connecting".to_string(),
            message_expires_at: None,
            connected: false,
            last_refresh: None,
            status_pending: false,
            pending_actions: HashMap::new(),
            reload_pending: false,
            filter: FilterMode::All,
            search: String::new(),
            collapsed_groups: HashSet::new(),
            input_mode: InputMode::Normal,
            focus: Focus::Table,
            detail_mode: DetailMode::Summary,
            detail_uuid: None,
            detail_loading: false,
            detail_error: None,
            detail_log: None,
            detail_history: None,
            detail_scroll: 0,
            log_follow: false,
        }
    }

    fn selected_entry(&self) -> Option<&UiEntry> {
        let uuid = self.selected_uuid?;
        self.entries.iter().find(|entry| entry.uuid == uuid)
    }

    fn selected_action_target(&self) -> Option<(EntryKind, uuid::Uuid, String)> {
        self.selected_entry()
            .map(|entry| (entry.kind, entry.uuid, entry.name.clone()))
    }

    fn set_detail_mode(&mut self, mode: DetailMode) {
        self.detail_mode = mode;
        self.detail_uuid = self.selected_uuid;
        self.detail_loading = false;
        self.detail_error = None;
        self.detail_log = None;
        self.detail_history = None;
        self.detail_scroll = 0;
        self.focus = if mode == DetailMode::Summary {
            Focus::Table
        } else {
            Focus::Details
        };
    }

    fn clear_details(&mut self) {
        self.set_detail_mode(DetailMode::Summary);
    }

    fn entry_visible(&self, entry: &UiEntry) -> bool {
        let matches_filter = match self.filter {
            FilterMode::All => true,
            FilterMode::Running => matches!(entry.status, state::JobStatus::Running),
            FilterMode::Failed => matches!(
                entry.status,
                state::JobStatus::Failed | state::JobStatus::StartFailed
            ),
            FilterMode::Unexpected => {
                entry.kind == EntryKind::Service
                    && (matches!(entry.status, state::JobStatus::Running) != entry.expected_running)
            }
        };
        if !matches_filter {
            return false;
        }
        let query = self.search.trim().to_lowercase();
        query.is_empty()
            || entry.name.to_lowercase().contains(&query)
            || entry
                .group
                .as_deref()
                .is_some_and(|group| group.to_lowercase().contains(&query))
            || entry.trigger_label.to_lowercase().contains(&query)
    }

    fn group_collapsed(&self, entry: &UiEntry) -> bool {
        self.search.is_empty()
            && entry
                .group
                .as_ref()
                .is_some_and(|group| self.collapsed_groups.contains(&(entry.kind, group.clone())))
    }

    fn visible_ids(&self) -> Vec<uuid::Uuid> {
        self.entries
            .iter()
            .filter(|entry| self.entry_visible(entry) && !self.group_collapsed(entry))
            .map(|entry| entry.uuid)
            .collect()
    }

    fn reconcile_selection(&mut self) {
        let ids = self.visible_ids();
        if self
            .selected_uuid
            .is_none_or(|selected| !ids.contains(&selected))
        {
            self.selected_uuid = ids.first().copied();
            self.detail_uuid = self.selected_uuid;
        }
    }

    fn move_selection(&mut self, amount: isize) -> bool {
        let ids = self.visible_ids();
        if ids.is_empty() {
            self.selected_uuid = None;
            return false;
        }
        let current = self
            .selected_uuid
            .and_then(|uuid| ids.iter().position(|candidate| *candidate == uuid))
            .unwrap_or(0);
        let next = (current as isize + amount).clamp(0, ids.len() as isize - 1) as usize;
        let changed = self.selected_uuid != Some(ids[next]);
        self.selected_uuid = Some(ids[next]);
        changed
    }
}

enum UiEvent {
    Status(Result<service::StatusResponse, String>),
    Action {
        uuid: Option<uuid::Uuid>,
        result: Result<String, String>,
    },
    Log {
        uuid: uuid::Uuid,
        result: Result<service::LogResponse, String>,
    },
    History {
        uuid: uuid::Uuid,
        result: Result<service::HistoryResponse, String>,
    },
}

pub(crate) async fn watch_status(config: SundialdConfig) -> Result<()> {
    let mut terminal = WatchTerminal::enter()?;
    let mut state = UiState::new();
    let mut interval = time::interval(Duration::from_secs(1));
    let mut keys = spawn_key_reader();
    let (event_tx, mut event_rx) = mpsc::unbounded_channel();

    request_status(&config, &mut state, &event_tx);
    terminal.draw(|frame| draw_ui(frame, &config, &state))?;

    loop {
        tokio::select! {
            _ = tokio::signal::ctrl_c() => break,
            _ = interval.tick() => {
                if state.message_expires_at.is_some_and(|deadline| time::Instant::now() >= deadline) {
                    state.message = "ready".to_string();
                    state.message_expires_at = None;
                }
                request_status(&config, &mut state, &event_tx);
                if state.detail_mode == DetailMode::Log
                    && state.log_follow
                    && !state.detail_loading
                    && state.selected_entry().is_some_and(|entry| matches!(entry.status, state::JobStatus::Running))
                {
                    request_selected_detail(&config, &mut state, &event_tx);
                }
                terminal.draw(|frame| draw_ui(frame, &config, &state))?;
            }
            event = event_rx.recv() => {
                if let Some(event) = event {
                    handle_ui_event(event, &config, &mut state, &event_tx);
                    terminal.draw(|frame| draw_ui(frame, &config, &state))?;
                }
            }
            key = keys.recv() => {
                let Some(key) = key else {
                    continue;
                };
                if handle_key(key, &config, &mut state, &event_tx) {
                    break;
                }
                terminal.draw(|frame| draw_ui(frame, &config, &state))?;
            }
        }
    }

    Ok(())
}

fn handle_key(
    key: KeyEvent,
    config: &SundialdConfig,
    state: &mut UiState,
    event_tx: &mpsc::UnboundedSender<UiEvent>,
) -> bool {
    match &state.input_mode {
        InputMode::Search => return handle_search_key(key, config, state, event_tx),
        InputMode::ConfirmKill { uuid, name } => {
            let uuid = *uuid;
            let name = name.clone();
            match key.code {
                KeyCode::Char('y') | KeyCode::Char('Y') => {
                    state.input_mode = InputMode::Normal;
                    start_kill(config, state, event_tx, uuid, name);
                }
                KeyCode::Char('n') | KeyCode::Char('N') | KeyCode::Esc => {
                    state.input_mode = InputMode::Normal;
                    state.message = "kill cancelled".to_string();
                    state.message_expires_at = Some(time::Instant::now() + Duration::from_secs(3));
                }
                _ => {}
            }
            return false;
        }
        InputMode::Help => {
            state.input_mode = InputMode::Normal;
            return false;
        }
        InputMode::Normal => {}
    }

    match key.code {
        KeyCode::Char('q') | KeyCode::Esc => return true,
        KeyCode::Char('c') if key.modifiers.contains(KeyModifiers::CONTROL) => return true,
        KeyCode::Down | KeyCode::Char('j') => {
            if state.focus == Focus::Details {
                state.detail_scroll = state.detail_scroll.saturating_add(1);
            } else if state.move_selection(1) {
                selection_changed(config, state, event_tx);
            }
        }
        KeyCode::Up | KeyCode::Char('k') => {
            if state.focus == Focus::Details {
                state.detail_scroll = state.detail_scroll.saturating_sub(1);
            } else if state.move_selection(-1) {
                selection_changed(config, state, event_tx);
            }
        }
        KeyCode::PageDown => {
            if state.focus == Focus::Details {
                state.detail_scroll = state.detail_scroll.saturating_add(8);
            } else if state.move_selection(10) {
                selection_changed(config, state, event_tx);
            }
        }
        KeyCode::PageUp => {
            if state.focus == Focus::Details {
                state.detail_scroll = state.detail_scroll.saturating_sub(8);
            } else if state.move_selection(-10) {
                selection_changed(config, state, event_tx);
            }
        }
        KeyCode::Home => {
            if state.move_selection(-(state.visible_ids().len() as isize)) {
                selection_changed(config, state, event_tx);
            }
        }
        KeyCode::End => {
            if state.move_selection(state.visible_ids().len() as isize) {
                selection_changed(config, state, event_tx);
            }
        }
        KeyCode::Tab => {
            state.focus = match state.focus {
                Focus::Table if state.detail_mode != DetailMode::Summary => Focus::Details,
                Focus::Details => Focus::Table,
                Focus::Table => Focus::Table,
            };
        }
        KeyCode::Left if state.focus == Focus::Details => {
            switch_detail(config, state, event_tx, -1);
        }
        KeyCode::Right if state.focus == Focus::Details => {
            switch_detail(config, state, event_tx, 1);
        }
        KeyCode::Char('r') => {
            if let Some((kind, uuid, name)) = state.selected_action_target() {
                let entry = state
                    .selected_entry()
                    .expect("selected action has an entry");
                if matches!(entry.status, state::JobStatus::Running) {
                    state.message = format!("{name} is already running");
                    return false;
                }
                if entry.manual_pending || state.pending_actions.contains_key(&uuid) {
                    state.message = format!("{name} already has a pending action");
                    return false;
                }
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
                start_action(
                    config,
                    state,
                    event_tx,
                    Some(uuid),
                    path,
                    message,
                    "starting",
                );
            }
        }
        KeyCode::Char('T') => {
            if let Some((kind, uuid, name)) = state.selected_action_target() {
                if state
                    .selected_entry()
                    .is_none_or(|entry| !matches!(entry.status, state::JobStatus::Running))
                {
                    state.message = format!("{name} is not running");
                    return false;
                }
                let encoded_id = encode_path_segment(&uuid.to_string());
                let (path, message) = match kind {
                    EntryKind::Job => (
                        format!("/jobs/{encoded_id}/terminate"),
                        format!("sent SIGTERM to {name}"),
                    ),
                    EntryKind::Service => (
                        format!("/services/{encoded_id}/stop"),
                        format!("queued service stop for {name}"),
                    ),
                };
                start_action(
                    config,
                    state,
                    event_tx,
                    Some(uuid),
                    path,
                    message,
                    "stopping",
                );
            }
        }
        KeyCode::Char('K') => {
            if let Some((_, uuid, name)) = state.selected_action_target() {
                if state
                    .selected_entry()
                    .is_some_and(|entry| matches!(entry.status, state::JobStatus::Running))
                {
                    state.input_mode = InputMode::ConfirmKill { uuid, name };
                } else {
                    state.message = format!("{name} is not running");
                }
            }
        }
        KeyCode::Char('R') => {
            if !state.reload_pending {
                state.reload_pending = true;
                start_action(
                    config,
                    state,
                    event_tx,
                    None,
                    "/reload".to_string(),
                    "config reloaded".to_string(),
                    "reloading",
                );
            }
        }
        KeyCode::Char('s') => {
            state.set_detail_mode(DetailMode::Schedule);
        }
        KeyCode::Char('h') => {
            state.set_detail_mode(DetailMode::History);
            request_selected_detail(config, state, event_tx);
        }
        KeyCode::Enter => {
            state.set_detail_mode(DetailMode::Log);
            request_selected_detail(config, state, event_tx);
        }
        KeyCode::Char('i') => state.clear_details(),
        KeyCode::Char('F') if state.detail_mode == DetailMode::Log => {
            state.log_follow = !state.log_follow;
            state.message = if state.log_follow {
                "log follow enabled".to_string()
            } else {
                "log follow disabled".to_string()
            };
            state.message_expires_at = Some(time::Instant::now() + Duration::from_secs(3));
        }
        KeyCode::Char('/') => {
            state.input_mode = InputMode::Search;
        }
        KeyCode::Char('f') => {
            state.filter = state.filter.next();
            state.reconcile_selection();
            selection_changed(config, state, event_tx);
        }
        KeyCode::Char('g') => {
            if let Some(entry) = state.selected_entry()
                && let Some(group) = &entry.group
            {
                let key = (entry.kind, group.clone());
                if !state.collapsed_groups.remove(&key) {
                    state.collapsed_groups.insert(key);
                }
                state.reconcile_selection();
            }
        }
        KeyCode::Char('G') => {
            state.collapsed_groups.clear();
            state.reconcile_selection();
        }
        KeyCode::Char('?') => {
            state.input_mode = InputMode::Help;
        }
        KeyCode::Char('x') => {
            state.message = "ready".to_string();
            state.message_expires_at = None;
        }
        KeyCode::Backspace => {
            state.clear_details();
        }
        _ => {}
    }

    false
}

fn handle_search_key(
    key: KeyEvent,
    config: &SundialdConfig,
    state: &mut UiState,
    event_tx: &mpsc::UnboundedSender<UiEvent>,
) -> bool {
    match key.code {
        KeyCode::Esc | KeyCode::Enter => state.input_mode = InputMode::Normal,
        KeyCode::Backspace => {
            state.search.pop();
        }
        KeyCode::Char('u') if key.modifiers.contains(KeyModifiers::CONTROL) => state.search.clear(),
        KeyCode::Char(character) if !key.modifiers.contains(KeyModifiers::CONTROL) => {
            state.search.push(character)
        }
        _ => {}
    }
    state.reconcile_selection();
    selection_changed(config, state, event_tx);
    false
}

fn request_status(
    config: &SundialdConfig,
    state: &mut UiState,
    event_tx: &mpsc::UnboundedSender<UiEvent>,
) {
    if state.status_pending {
        return;
    }
    state.status_pending = true;
    let config = config.clone();
    let event_tx = event_tx.clone();
    tokio::spawn(async move {
        let result = fetch_status(&config)
            .await
            .map_err(|error| format!("{error:#}"));
        let _ = event_tx.send(UiEvent::Status(result));
    });
}

fn handle_ui_event(
    event: UiEvent,
    config: &SundialdConfig,
    state: &mut UiState,
    event_tx: &mpsc::UnboundedSender<UiEvent>,
) {
    match event {
        UiEvent::Status(Ok(status)) => {
            state.status_pending = false;
            state.connected = true;
            state.last_refresh = Some(Local::now());
            state.entries = status
                .jobs
                .into_iter()
                .map(UiEntry::from_job)
                .chain(status.services.into_iter().map(UiEntry::from_service))
                .collect();
            state.reconcile_selection();
            if state.message == "connecting" || state.message.starts_with("status refresh failed") {
                state.message = "ready".to_string();
            }
        }
        UiEvent::Status(Err(error)) => {
            state.status_pending = false;
            state.connected = false;
            state.message = format!("status refresh failed: {error}");
            state.message_expires_at = None;
        }
        UiEvent::Action { uuid, result } => {
            if let Some(uuid) = uuid {
                state.pending_actions.remove(&uuid);
            } else {
                state.reload_pending = false;
            }
            let succeeded = result.is_ok();
            state.message = result.unwrap_or_else(|error| error);
            state.message_expires_at =
                succeeded.then(|| time::Instant::now() + Duration::from_secs(5));
            request_status(config, state, event_tx);
        }
        UiEvent::Log { uuid, result } => {
            if state.detail_uuid == Some(uuid) && state.detail_mode == DetailMode::Log {
                state.detail_loading = false;
                match result {
                    Ok(log) => {
                        state.detail_log = Some(log);
                        state.detail_error = None;
                    }
                    Err(error) => state.detail_error = Some(error),
                }
            }
        }
        UiEvent::History { uuid, result } => {
            if state.detail_uuid == Some(uuid) && state.detail_mode == DetailMode::History {
                state.detail_loading = false;
                match result {
                    Ok(history) => {
                        state.detail_history = Some(history);
                        state.detail_error = None;
                    }
                    Err(error) => state.detail_error = Some(error),
                }
            }
        }
    }
}

fn switch_detail(
    config: &SundialdConfig,
    state: &mut UiState,
    event_tx: &mpsc::UnboundedSender<UiEvent>,
    amount: isize,
) {
    let index = (state.detail_mode.index() as isize + amount).clamp(0, 3) as usize;
    let mode = [
        DetailMode::Summary,
        DetailMode::Log,
        DetailMode::History,
        DetailMode::Schedule,
    ][index];
    state.set_detail_mode(mode);
    request_selected_detail(config, state, event_tx);
}

fn selection_changed(
    config: &SundialdConfig,
    state: &mut UiState,
    event_tx: &mpsc::UnboundedSender<UiEvent>,
) {
    state.detail_uuid = state.selected_uuid;
    state.detail_scroll = 0;
    state.detail_log = None;
    state.detail_history = None;
    state.detail_error = None;
    if matches!(state.detail_mode, DetailMode::Log | DetailMode::History) {
        request_selected_detail(config, state, event_tx);
    }
}

fn request_selected_detail(
    config: &SundialdConfig,
    state: &mut UiState,
    event_tx: &mpsc::UnboundedSender<UiEvent>,
) {
    let Some((kind, uuid, _)) = state.selected_action_target() else {
        return;
    };
    state.detail_uuid = Some(uuid);
    state.detail_loading = true;
    let config = config.clone();
    let event_tx = event_tx.clone();
    match state.detail_mode {
        DetailMode::Log => {
            tokio::spawn(async move {
                let result = fetch_recent_log(&config, kind, uuid).await;
                let _ = event_tx.send(UiEvent::Log { uuid, result });
            });
        }
        DetailMode::History => {
            tokio::spawn(async move {
                let result = fetch_history(&config, kind, uuid).await;
                let _ = event_tx.send(UiEvent::History { uuid, result });
            });
        }
        DetailMode::Summary | DetailMode::Schedule => state.detail_loading = false,
    }
}

fn start_action(
    config: &SundialdConfig,
    state: &mut UiState,
    event_tx: &mpsc::UnboundedSender<UiEvent>,
    uuid: Option<uuid::Uuid>,
    path: String,
    success_message: String,
    pending_label: &str,
) {
    if let Some(uuid) = uuid {
        if state.pending_actions.contains_key(&uuid) {
            return;
        }
        state
            .pending_actions
            .insert(uuid, pending_label.to_string());
    }
    state.message = format!("{pending_label}...");
    state.message_expires_at = None;
    let config = config.clone();
    let event_tx = event_tx.clone();
    tokio::spawn(async move {
        let result = post_watch_action(&config, &path, &success_message).await;
        let _ = event_tx.send(UiEvent::Action { uuid, result });
    });
}

fn start_kill(
    config: &SundialdConfig,
    state: &mut UiState,
    event_tx: &mpsc::UnboundedSender<UiEvent>,
    uuid: uuid::Uuid,
    name: String,
) {
    let Some(entry) = state.entries.iter().find(|entry| entry.uuid == uuid) else {
        return;
    };
    let encoded_id = encode_path_segment(&uuid.to_string());
    let path = match entry.kind {
        EntryKind::Job => format!("/jobs/{encoded_id}/kill"),
        EntryKind::Service => format!("/services/{encoded_id}/kill"),
    };
    start_action(
        config,
        state,
        event_tx,
        Some(uuid),
        path,
        format!("sent SIGKILL to {name}"),
        "killing",
    );
}

fn draw_ui(frame: &mut Frame<'_>, config: &SundialdConfig, state: &UiState) {
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(3),
            Constraint::Min(5),
            Constraint::Length(1),
        ])
        .split(frame.area());

    draw_header(frame, chunks[0], config, state);
    if chunks[1].height < 20 {
        if state.detail_mode == DetailMode::Summary {
            draw_jobs(frame, chunks[1], state);
        } else {
            draw_details(frame, chunks[1], state);
        }
    } else {
        let body = Layout::default()
            .direction(Direction::Vertical)
            .constraints([Constraint::Min(8), Constraint::Length(12)])
            .split(chunks[1]);
        draw_jobs(frame, body[0], state);
        draw_details(frame, body[1], state);
    }
    draw_footer(frame, chunks[2], state);

    match &state.input_mode {
        InputMode::ConfirmKill { name, .. } => draw_confirmation(frame, name),
        InputMode::Help => draw_help(frame),
        InputMode::Normal | InputMode::Search => {}
    }
}

fn draw_header(frame: &mut Frame<'_>, area: Rect, config: &SundialdConfig, state: &UiState) {
    let connection = if state.connected {
        Span::styled("● connected", Style::default().fg(Color::Green))
    } else {
        Span::styled("× disconnected", Style::default().fg(Color::Red))
    };
    let refreshed = state
        .last_refresh
        .map(|time| format!(" {}", time.format("%H:%M:%S")))
        .unwrap_or_default();
    let title = Line::from(vec![
        Span::styled(
            "sundiald",
            Style::default()
                .fg(Color::Cyan)
                .add_modifier(Modifier::BOLD),
        ),
        Span::raw("  "),
        connection,
        Span::styled(refreshed, Style::default().fg(Color::DarkGray)),
        Span::raw(if area.width >= 100 {
            format!("  {}  ", api_base(config))
        } else {
            "  ".to_string()
        }),
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

    let column_mode = if area.width >= 120 {
        5
    } else if area.width >= 85 {
        4
    } else {
        3
    };

    push_section_rows(
        &mut rows,
        &mut row_index,
        &mut selected_row,
        state,
        EntryKind::Job,
        "Jobs",
        "no jobs",
        now,
        column_mode,
    );
    push_section_rows(
        &mut rows,
        &mut row_index,
        &mut selected_row,
        state,
        EntryKind::Service,
        "Services",
        "no services",
        now,
        column_mode,
    );

    let (headers, constraints): (Vec<&str>, Vec<Constraint>) = match column_mode {
        5 => (
            vec!["Name", "Status", "Trigger", "Last Run", "Next"],
            vec![
                Constraint::Percentage(24),
                Constraint::Percentage(16),
                Constraint::Percentage(16),
                Constraint::Percentage(24),
                Constraint::Percentage(20),
            ],
        ),
        4 => (
            vec!["Name", "Status", "Last Run", "Next"],
            vec![
                Constraint::Percentage(28),
                Constraint::Percentage(22),
                Constraint::Percentage(27),
                Constraint::Percentage(23),
            ],
        ),
        _ => (
            vec!["Name", "Status", "Next"],
            vec![
                Constraint::Percentage(38),
                Constraint::Percentage(30),
                Constraint::Percentage(32),
            ],
        ),
    };
    let failed = state
        .entries
        .iter()
        .filter(|entry| {
            matches!(
                entry.status,
                state::JobStatus::Failed | state::JobStatus::StartFailed
            )
        })
        .count();
    let title = if state.search.is_empty() {
        format!(
            "Runnables · filter: {} · failed: {failed}",
            state.filter.label()
        )
    } else {
        format!(
            "Runnables · filter: {} · search: {} · failed: {failed}",
            state.filter.label(),
            state.search
        )
    };
    let table = Table::new(rows, constraints)
        .header(Row::new(headers).style(Style::default().add_modifier(Modifier::BOLD)))
        .block(Block::default().title(title).borders(Borders::ALL))
        .row_highlight_style(Style::default().bg(Color::Indexed(236)))
        .highlight_symbol("> ");

    let mut table_state = TableState::default().with_selected(selected_row);
    frame.render_stateful_widget(table, area, &mut table_state);
}

#[allow(clippy::too_many_arguments)]
fn push_section_rows(
    rows: &mut Vec<Row<'static>>,
    row_index: &mut usize,
    selected_row: &mut Option<usize>,
    state: &UiState,
    kind: EntryKind,
    title: &str,
    empty_label: &str,
    now: chrono::DateTime<Local>,
    column_mode: usize,
) {
    let visible_count = state
        .entries
        .iter()
        .filter(|entry| entry.kind == kind && state.entry_visible(entry))
        .count();
    let failed_count = state
        .entries
        .iter()
        .filter(|entry| {
            entry.kind == kind
                && state.entry_visible(entry)
                && matches!(
                    entry.status,
                    state::JobStatus::Failed | state::JobStatus::StartFailed
                )
        })
        .count();
    let section_title = if failed_count > 0 {
        format!("{title}  {visible_count}  failed {failed_count}")
    } else {
        format!("{title}  {visible_count}")
    };
    rows.push(
        Row::new(section_cells(section_title, column_mode)).style(
            Style::default()
                .fg(Color::Cyan)
                .add_modifier(Modifier::BOLD),
        ),
    );
    *row_index += 1;

    let groups = group_entries(state, kind);
    if groups.is_empty() {
        rows.push(Row::new(section_cells(
            format!("  {empty_label}"),
            column_mode,
        )));
        *row_index += 1;
        return;
    }

    for (group, entries) in groups {
        let collapsed = group.as_ref().is_some_and(|group| {
            state.collapsed_groups.contains(&(kind, group.to_string())) && state.search.is_empty()
        });
        if let Some(group) = &group {
            let marker = if collapsed { "▸" } else { "▾" };
            rows.push(
                Row::new(section_cells(
                    format!("  {marker} {group}  {}", entries.len()),
                    column_mode,
                ))
                .style(Style::default().fg(Color::Cyan)),
            );
            *row_index += 1;
        }

        if collapsed {
            continue;
        }

        for entry in entries {
            if Some(entry.uuid) == state.selected_uuid {
                *selected_row = Some(*row_index);
            }
            rows.push(Row::new(entry_cells(
                entry,
                now,
                column_mode,
                state.pending_actions.get(&entry.uuid).map(String::as_str),
            )));
            *row_index += 1;
        }
    }
}

fn section_cells(title: String, column_mode: usize) -> Vec<Cell<'static>> {
    std::iter::once(Cell::from(title))
        .chain((1..column_mode).map(|_| Cell::from("")))
        .collect()
}

fn entry_cells(
    entry: &UiEntry,
    now: chrono::DateTime<Local>,
    column_mode: usize,
    pending: Option<&str>,
) -> Vec<Cell<'static>> {
    let name = Cell::from(format!("  {}", entry.name));
    let status = pending.map_or_else(
        || status_cell(entry),
        |pending| Cell::from(format!("… {pending}")).style(Style::default().fg(Color::Yellow)),
    );
    match column_mode {
        5 => vec![
            name,
            status,
            Cell::from(entry.trigger_label.clone()),
            Cell::from(format_last_run(entry, now)),
            Cell::from(entry.next_label.clone()),
        ],
        4 => vec![
            name,
            status,
            Cell::from(format_last_run(entry, now)),
            Cell::from(entry.next_label.clone()),
        ],
        _ => vec![name, status, Cell::from(entry.next_label.clone())],
    }
}

fn draw_details(frame: &mut Frame<'_>, area: Rect, state: &UiState) {
    let name = state
        .selected_entry()
        .map(|entry| entry.name.as_str())
        .unwrap_or("No selection");
    let focus = if state.focus == Focus::Details {
        " · focused"
    } else {
        ""
    };
    let block = Block::default()
        .title(format!("Details · {name}{focus}"))
        .borders(Borders::ALL);
    let inner = block.inner(area);
    frame.render_widget(block, area);
    if inner.height == 0 {
        return;
    }
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Length(1), Constraint::Min(1)])
        .split(inner);
    let tabs = Tabs::new(["Summary", "Log", "History", "Schedule"])
        .select(state.detail_mode.index())
        .highlight_style(
            Style::default()
                .fg(Color::Cyan)
                .add_modifier(Modifier::BOLD),
        )
        .divider(" | ");
    frame.render_widget(tabs, chunks[0]);

    if state.detail_loading {
        frame.render_widget(Paragraph::new("Loading..."), chunks[1]);
        return;
    }
    if let Some(error) = &state.detail_error {
        frame.render_widget(
            Paragraph::new(error.as_str()).style(Style::default().fg(Color::Red)),
            chunks[1],
        );
        return;
    }

    match state.detail_mode {
        DetailMode::Summary => draw_summary(frame, chunks[1], state),
        DetailMode::Log => {
            let text = state
                .detail_log
                .as_ref()
                .map(render_log_response)
                .unwrap_or_else(|| "No log is available for this runnable.".to_string());
            let title = if state.log_follow {
                "Following"
            } else {
                "Snapshot"
            };
            frame.render_widget(
                Paragraph::new(text)
                    .block(Block::default().title(title))
                    .wrap(Wrap { trim: false })
                    .scroll((state.detail_scroll, 0)),
                chunks[1],
            );
        }
        DetailMode::History => draw_history_table(frame, chunks[1], state),
        DetailMode::Schedule => {
            let text = state
                .selected_entry()
                .map(render_schedule)
                .unwrap_or_default();
            frame.render_widget(
                Paragraph::new(text)
                    .wrap(Wrap { trim: false })
                    .scroll((state.detail_scroll, 0)),
                chunks[1],
            );
        }
    }
}

fn draw_summary(frame: &mut Frame<'_>, area: Rect, state: &UiState) {
    let Some(entry) = state.selected_entry() else {
        frame.render_widget(
            Paragraph::new("No runnable matches the current view."),
            area,
        );
        return;
    };
    let runtime = entry
        .started_at
        .filter(|_| matches!(entry.status, state::JobStatus::Running))
        .map(|started| format_chrono_duration(Local::now().signed_duration_since(started)))
        .unwrap_or_else(|| "-".to_string());
    let group = entry.group.as_deref().unwrap_or("-");
    let error = entry.last_error.as_deref().unwrap_or("-");
    let expected = if entry.kind == EntryKind::Service {
        if entry.expected_running {
            "running"
        } else {
            "stopped"
        }
    } else {
        "-"
    };
    let lines = vec![
        Line::from(format!("UUID: {}   Group: {group}", entry.uuid)),
        Line::from(format!(
            "Status: {}   PID: {}   Runtime: {runtime}   Expected: {expected}",
            compact_status(entry),
            entry
                .pid
                .map(|pid| pid.to_string())
                .unwrap_or_else(|| "-".to_string())
        )),
        Line::from(format!(
            "Trigger: {}   Next: {}",
            entry.trigger_label, entry.next_label
        )),
        Line::from(format!("Last error: {error}")),
    ];
    frame.render_widget(Paragraph::new(lines).wrap(Wrap { trim: false }), area);
}

fn draw_history_table(frame: &mut Frame<'_>, area: Rect, state: &UiState) {
    let Some(history) = &state.detail_history else {
        frame.render_widget(
            Paragraph::new("No history is available for this runnable."),
            area,
        );
        return;
    };
    let start = usize::from(state.detail_scroll).min(history.runs.len());
    let rows = history.runs.iter().skip(start).map(|run| {
        let status = run.status.as_deref().unwrap_or("running");
        let exit = run
            .exit_code
            .map(|code| code.to_string())
            .unwrap_or_else(|| "-".to_string());
        let duration = run
            .duration_ms
            .map(format_duration_ms)
            .unwrap_or_else(|| "-".to_string());
        Row::new(vec![
            Cell::from(run.id.to_string()),
            Cell::from(run.triggered_at.format("%Y-%m-%d %H:%M:%S").to_string()),
            Cell::from(run.trigger_kind.clone()),
            Cell::from(status.to_string()),
            Cell::from(exit),
            Cell::from(duration),
            Cell::from(run.error.clone().unwrap_or_default()),
        ])
        .style(match status {
            "succeeded" => Style::default().fg(Color::Green),
            "failed" | "start_failed" => Style::default().fg(Color::Red),
            "interrupted" => Style::default().fg(Color::Yellow),
            _ => Style::default(),
        })
    });
    let table = Table::new(
        rows,
        [
            Constraint::Length(6),
            Constraint::Length(19),
            Constraint::Length(12),
            Constraint::Length(12),
            Constraint::Length(6),
            Constraint::Length(12),
            Constraint::Min(10),
        ],
    )
    .header(
        Row::new([
            "ID", "Started", "Trigger", "Status", "Exit", "Duration", "Error",
        ])
        .style(Style::default().add_modifier(Modifier::BOLD)),
    );
    frame.render_widget(table, area);
}

fn draw_footer(frame: &mut Frame<'_>, area: Rect, state: &UiState) {
    let text = if state.input_mode == InputMode::Search {
        format!(
            "/ {}_   Enter accept   Esc close   Ctrl-U clear",
            state.search
        )
    } else if area.width < 90 {
        "? help  / find  f filter  Enter log  r start  T stop  q quit".to_string()
    } else {
        let actions = state.selected_entry().map_or("", |entry| {
            if matches!(entry.status, state::JobStatus::Running) {
                "T stop  K kill"
            } else {
                "r run/start"
            }
        });
        format!(
            "? help  / search  f filter  g group  Enter log  h history  s schedule  {actions}  R reload  x dismiss  q quit"
        )
    };
    frame.render_widget(
        Paragraph::new(text).style(Style::default().fg(Color::Gray)),
        area,
    );
}

fn group_entries(state: &UiState, kind: EntryKind) -> Vec<(Option<String>, Vec<&UiEntry>)> {
    let mut groups: Vec<(Option<String>, Vec<&UiEntry>)> = Vec::new();
    for entry in &state.entries {
        if entry.kind != kind || !state.entry_visible(entry) {
            continue;
        }
        let group = entry.group.clone();
        if let Some((_, group_entries)) = groups.iter_mut().find(|(existing, _)| *existing == group)
        {
            group_entries.push(entry);
        } else {
            groups.push((group, vec![entry]));
        }
    }
    groups
}

fn centered_rect(width: u16, height: u16, area: Rect) -> Rect {
    let vertical = Layout::vertical([
        Constraint::Fill(1),
        Constraint::Length(height.min(area.height)),
        Constraint::Fill(1),
    ])
    .split(area);
    Layout::horizontal([
        Constraint::Fill(1),
        Constraint::Length(width.min(area.width)),
        Constraint::Fill(1),
    ])
    .split(vertical[1])[1]
}

fn draw_confirmation(frame: &mut Frame<'_>, name: &str) {
    let area = centered_rect(58, 5, frame.area());
    frame.render_widget(Clear, area);
    frame.render_widget(
        Paragraph::new(format!(
            "Send SIGKILL to {name}?\n\n[y] Kill    [n/Esc] Cancel"
        ))
        .block(Block::default().title("Confirm kill").borders(Borders::ALL))
        .style(Style::default().fg(Color::Red)),
        area,
    );
}

fn draw_help(frame: &mut Frame<'_>) {
    let area = centered_rect(74, 18, frame.area());
    frame.render_widget(Clear, area);
    let help = [
        "Navigation        ↑/↓ or j/k, Home/End, PgUp/PgDn",
        "Focus             Tab switches table/details",
        "Find and filter   / search, f cycle filter, g collapse, G expand all",
        "Details           i summary, Enter log, h history, s schedule",
        "Logs              F toggles automatic follow",
        "Actions           r run/start, T terminate/stop, K force kill",
        "Configuration     R reload",
        "Messages          x dismisses the current notice or error",
        "Close details     Backspace",
        "Quit              q, Esc, Ctrl-C",
        "",
        "Press any key to close help.",
    ]
    .join("\n");
    frame.render_widget(
        Paragraph::new(help)
            .block(
                Block::default()
                    .title("Keyboard help")
                    .borders(Borders::ALL),
            )
            .wrap(Wrap { trim: false }),
        area,
    );
}

fn compact_status(entry: &UiEntry) -> String {
    let mut parts = vec![match entry.status {
        state::JobStatus::Running => "● running".to_string(),
        state::JobStatus::Succeeded => "✓ succeeded".to_string(),
        state::JobStatus::Failed | state::JobStatus::StartFailed => "× failed".to_string(),
        state::JobStatus::Interrupted => "! interrupted".to_string(),
        state::JobStatus::Idle => "○ idle".to_string(),
    }];

    if entry.kind == EntryKind::Service
        && (matches!(entry.status, state::JobStatus::Running) != entry.expected_running)
    {
        parts.push("unexpected".to_string());
    }

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
    let seconds = duration.num_seconds().max(0);
    format_duration_seconds(seconds)
}

fn format_duration_seconds(total_seconds: i64) -> String {
    let days = total_seconds / 86_400;
    let hours = (total_seconds % 86_400) / 3_600;
    let minutes = (total_seconds % 3_600) / 60;
    let seconds = total_seconds % 60;
    let mut parts = Vec::new();

    if days > 0 {
        parts.push(format!("{days}d"));
    }
    if hours > 0 {
        parts.push(format!("{hours}h"));
    }
    if minutes > 0 {
        parts.push(format!("{minutes}m"));
    }
    if seconds > 0 || parts.is_empty() {
        parts.push(format!("{seconds}s"));
    }

    parts.join(" ")
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

async fn fetch_recent_log(
    config: &SundialdConfig,
    kind: EntryKind,
    uuid: uuid::Uuid,
) -> Result<service::LogResponse, String> {
    let encoded_id = encode_path_segment(&uuid.to_string());
    let path = match kind {
        EntryKind::Job => format!("/jobs/{encoded_id}/logs/latest?tail=40"),
        EntryKind::Service => format!("/services/{encoded_id}/logs/latest?tail=40"),
    };
    let response = authorize(
        config,
        api_client().get(format!("{}{}", api_base(config), path)),
    )
    .send()
    .await;
    match response {
        Ok(response) if response.status().is_success() => response
            .json::<service::LogResponse>()
            .await
            .map_err(|error| format!("Failed to parse log response: {error}")),
        Ok(response) => {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            Err(format!("Log request rejected: HTTP {status}: {body}"))
        }
        Err(error) => Err(format!("Failed to load log: {error}")),
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

async fn fetch_history(
    config: &SundialdConfig,
    kind: EntryKind,
    uuid: uuid::Uuid,
) -> Result<service::HistoryResponse, String> {
    let encoded_id = encode_path_segment(&uuid.to_string());
    let path = match kind {
        EntryKind::Job => format!("/jobs/{encoded_id}/history?limit=50"),
        EntryKind::Service => format!("/services/{encoded_id}/history?limit=50"),
    };
    let response = authorize(
        config,
        api_client().get(format!("{}{path}", api_base(config))),
    )
    .send()
    .await;
    match response {
        Ok(response) if response.status().is_success() => response
            .json::<service::HistoryResponse>()
            .await
            .map_err(|error| format!("Failed to parse history response: {error}")),
        Ok(response) => {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            Err(format!("History request rejected: HTTP {status}: {body}"))
        }
        Err(error) => Err(format!("Failed to load history: {error}")),
    }
}

fn format_duration_ms(duration_ms: i64) -> String {
    let seconds = duration_ms / 1_000;
    let milliseconds = duration_ms % 1_000;
    format!("{seconds}.{milliseconds:03}s")
}

/// Fire-and-report POST used by watch mode's key handlers: unlike the
/// non-interactive commands, a failure here shouldn't exit the process, just
/// update the status line with what happened.
async fn post_watch_action(
    config: &SundialdConfig,
    path: &str,
    success_message: &str,
) -> Result<String, String> {
    let response = authorize(
        config,
        api_client().post(format!("{}{path}", api_base(config))),
    )
    .send()
    .await;
    match response {
        Ok(response) if response.status().is_success() => Ok(success_message.to_string()),
        Ok(response) => {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            Err(format!("Command rejected: HTTP {status}: {body}"))
        }
        Err(error) => Err(format!("Command failed: {error}")),
    }
}

fn spawn_key_reader() -> mpsc::UnboundedReceiver<KeyEvent> {
    let (tx, rx) = mpsc::unbounded_channel();
    thread::spawn(move || {
        while let Ok(event) = event::read() {
            if let Event::Key(key) = event
                && tx.send(key).is_err()
            {
                break;
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
    use ratatui::backend::TestBackend;
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

    fn render_at(width: u16, height: u16, state: &UiState) -> String {
        let backend = TestBackend::new(width, height);
        let mut terminal = Terminal::new(backend).unwrap();
        let config: SundialdConfig = serde_yaml::from_str("jobs: []\n").unwrap();
        terminal
            .draw(|frame| draw_ui(frame, &config, state))
            .unwrap();
        let buffer = terminal.backend().buffer();
        let mut output = String::new();
        for y in 0..height {
            for x in 0..width {
                output.push_str(buffer.cell((x, y)).unwrap().symbol());
            }
            output.push('\n');
        }
        output
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
    fn last_and_next_run_durations_do_not_show_milliseconds() {
        let now = chrono::Local
            .with_ymd_and_hms(2026, 1, 1, 12, 0, 0)
            .unwrap();
        let mut job = job_entry();
        job.status = crate::state::JobStatus::Succeeded;
        job.started_at = Some(
            chrono::Local
                .with_ymd_and_hms(2026, 1, 1, 11, 59, 30)
                .unwrap(),
        );
        job.finished_at = Some(
            chrono::Local
                .with_ymd_and_hms(2026, 1, 1, 11, 59, 35)
                .unwrap(),
        );
        let next = chrono::Local
            .with_ymd_and_hms(2026, 1, 1, 12, 0, 30)
            .unwrap();

        assert!(format_last_run(&job, now).contains("took 5s"));
        assert!(!format_last_run(&job, now).contains(".000s"));
        assert!(format_time_with_delta(next, now).contains("in 30s"));
        assert!(!format_time_with_delta(next, now).contains(".000s"));
    }

    #[test]
    fn selection_is_preserved_by_uuid_across_reordering() {
        let first = job_entry();
        let second = job_entry();
        let selected = second.uuid;
        let mut state = UiState::new();
        state.entries = vec![first.clone(), second];
        state.selected_uuid = Some(selected);

        state.entries.reverse();
        state.reconcile_selection();

        assert_eq!(state.selected_uuid, Some(selected));
        assert_eq!(state.selected_entry().unwrap().uuid, selected);
    }

    #[test]
    fn clear_details_returns_to_summary() {
        let mut state = UiState::new();
        state.set_detail_mode(DetailMode::Log);
        state.detail_error = Some("error".to_string());

        state.clear_details();

        assert_eq!(state.detail_mode, DetailMode::Summary);
        assert!(state.detail_error.is_none());
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
        let service = service_entry(crate::state::JobStatus::Running, true);
        let mut state = UiState::new();
        state.entries = vec![first, service, second];

        let groups = group_entries(&state, EntryKind::Job);

        assert_eq!(groups.len(), 2);
        assert_eq!(groups[0].0, None);
        assert_eq!(groups[0].1[0].name, "inline");
        assert_eq!(groups[1].0.as_deref(), Some("maintenance"));
        assert_eq!(groups[1].1[0].name, "cleanup");
    }

    #[test]
    fn filters_and_collapsed_groups_change_keyboard_navigation_set() {
        let mut running = job_entry();
        running.name = "running".to_string();
        running.status = crate::state::JobStatus::Running;
        running.group = Some("ops".to_string());
        let idle = job_entry();
        let mut state = UiState::new();
        state.entries = vec![running.clone(), idle];
        state.filter = FilterMode::Running;

        assert_eq!(state.visible_ids(), vec![running.uuid]);

        state
            .collapsed_groups
            .insert((EntryKind::Job, "ops".to_string()));
        assert!(state.visible_ids().is_empty());
    }

    #[test]
    fn compact_status_uses_symbols_and_marks_unexpected_services() {
        let success = {
            let mut entry = job_entry();
            entry.status = crate::state::JobStatus::Succeeded;
            entry
        };
        let service = service_entry(crate::state::JobStatus::Running, false);

        assert!(compact_status(&success).starts_with("✓ succeeded"));
        assert!(compact_status(&service).contains("unexpected"));
    }

    #[test]
    fn responsive_table_hides_columns_before_truncating_primary_state() {
        let entry = job_entry();
        let uuid = entry.uuid;
        let mut state = UiState::new();
        state.connected = true;
        state.entries = vec![entry];
        state.selected_uuid = Some(uuid);

        let wide = render_at(140, 36, &state);
        let narrow = render_at(70, 18, &state);

        assert!(wide.contains("Trigger"));
        assert!(wide.contains("Details · example"));
        assert!(narrow.contains("Name"));
        assert!(narrow.contains("Status"));
        assert!(narrow.contains("Next"));
        assert!(!narrow.contains("Trigger"));
    }

    #[test]
    fn short_terminal_replaces_table_with_open_details() {
        let entry = job_entry();
        let uuid = entry.uuid;
        let mut state = UiState::new();
        state.entries = vec![entry];
        state.selected_uuid = Some(uuid);
        state.set_detail_mode(DetailMode::Log);
        state.detail_log = Some(service::LogResponse {
            job: "example".to_string(),
            uuid,
            log_path: "example.stdout.log".into(),
            stderr_log_path: None,
            content: "hello from stdout".to_string(),
            stderr_content: None,
        });

        let output = render_at(80, 18, &state);

        assert!(output.contains("Details · example"));
        assert!(output.contains("hello from stdout"));
        assert!(!output.contains("Runnables"));
    }
}
