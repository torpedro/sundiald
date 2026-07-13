use anyhow::Result;
use chrono::{DateTime, Local};
use colored::Colorize;

use super::client::fetch_status;
use crate::{config::SundialdConfig, service, state};

pub(crate) async fn print_status(config: &SundialdConfig) -> Result<()> {
    let (frame, _jobs) = render_status(config, None, None).await?;
    print!("{frame}");
    Ok(())
}

pub(crate) async fn render_status(
    config: &SundialdConfig,
    selected: Option<usize>,
    last_command: Option<&str>,
) -> Result<(String, Vec<service::JobStatusResponse>)> {
    let now = Local::now();
    let status = fetch_status(config).await?;

    let mut output = String::new();
    let groups = group_jobs(&status.jobs);
    let mut rendered_any_job = false;
    for (group, jobs) in &groups {
        let mut printed_group_header = false;
        if let Some(group) = group {
            if rendered_any_job {
                output.push_str("----\n");
            }
            output.push_str(&format!("{}\n", group.bold()));
            printed_group_header = true;
        }

        for &(index, job) in jobs {
            if rendered_any_job && !printed_group_header {
                output.push_str("----\n");
            }
            printed_group_header = false;
            rendered_any_job = true;

            let is_selected = selected == Some(index);
            let marker = if is_selected {
                "> "
            } else if selected.is_some() {
                "  "
            } else {
                ""
            };
            let name = if is_selected {
                job.name.bold().to_string()
            } else {
                job.name.clone()
            };

            output.push_str(&format!("{marker}{name}\n"));
            output.push_str(&format!("  status: {}\n", colored_status(job)));
            output.push_str(&format!("  last_run: {}\n", format_last_run(job, now)));
            output.push_str(&format!("  next_run: {}\n", format_next_run(job, now)));
        }
    }
    if let Some(last_command) = last_command {
        output.push('\n');
        output.push_str(
            "keys: arrows/j/k select, Enter log, h history, s schedule, r run/start, T SIGTERM/stop, K SIGKILL, R reload config, Backspace clear, q quit",
        );
        output.push('\n');
        if !last_command.is_empty() {
            output.push_str(last_command);
            output.push('\n');
        }
    }

    if !status.services.is_empty() {
        if !output.is_empty() {
            output.push_str("----\n");
        }
        output.push_str(&format!("{}\n", "services".bold()));
        for service in &status.services {
            output.push_str(&format!("{}\n", service.name));
            output.push_str(&format!("  status: {}\n", colored_service_status(service)));
            output.push_str(&format!(
                "  last_run: {}\n",
                format_service_last_run(service, now)
            ));
            output.push_str(&format!(
                "  next: {}\n",
                format_service_next_run(service, now)
            ));
        }
    }

    Ok((output, status.jobs))
}

pub(crate) fn group_jobs(
    jobs: &[service::JobStatusResponse],
) -> Vec<(Option<String>, Vec<(usize, &service::JobStatusResponse)>)> {
    let mut groups: Vec<(Option<String>, Vec<(usize, &service::JobStatusResponse)>)> = Vec::new();
    for (index, job) in jobs.iter().enumerate() {
        let group = job.group.clone();
        if let Some((_, entries)) = groups.iter_mut().find(|(existing, _)| *existing == group) {
            entries.push((index, job));
        } else {
            groups.push((group, vec![(index, job)]));
        }
    }
    groups
}

pub(crate) fn high_level_status(job: &service::JobStatusResponse) -> &'static str {
    match &job.status {
        state::JobStatus::Running => "running",
        state::JobStatus::Failed | state::JobStatus::StartFailed => "last run failed",
        state::JobStatus::Succeeded => "last run succeeded",
        state::JobStatus::Interrupted => "last run interrupted",
        state::JobStatus::Idle => "never run",
    }
}

fn colored_status(job: &service::JobStatusResponse) -> String {
    let status = match high_level_status(job) {
        "running" => "running".yellow().to_string(),
        "last run failed" => "last run failed".red().to_string(),
        "last run succeeded" => "last run succeeded".green().to_string(),
        "last run interrupted" => "last run interrupted".yellow().to_string(),
        status => status.to_string(),
    };

    let status = if job.manual_pending {
        format!("{status} manual_pending=true")
    } else {
        status
    };

    let status = match job.trigger.after.as_deref() {
        Some(upstream) => format!("{status} trigger={} after={upstream}", job.trigger.kind),
        None => format!("{status} trigger={}", job.trigger.kind),
    };

    let status = if matches!(job.status, state::JobStatus::Running) {
        match job.pid {
            Some(pid) => format!("{status} pid={pid}"),
            None => format!("{status} pid=unknown"),
        }
    } else {
        status
    };

    let status = match job.exit_code {
        Some(exit_code) => format!("{status} exit_code={exit_code}"),
        None => status,
    };

    match &job.terminated_by_signal {
        Some(signal) => format!("{status} signal={signal}"),
        None => status,
    }
}

fn colored_service_status(service: &service::ServiceStatusResponse) -> String {
    let status = match service.status {
        state::JobStatus::Running => "running".yellow().to_string(),
        state::JobStatus::Failed | state::JobStatus::StartFailed => {
            "last run failed".red().to_string()
        }
        state::JobStatus::Succeeded => "last run succeeded".green().to_string(),
        state::JobStatus::Interrupted => "last run interrupted".yellow().to_string(),
        state::JobStatus::Idle => "never run".to_string(),
    };

    if matches!(service.status, state::JobStatus::Running) {
        match service.pid {
            Some(pid) => format!("{status} pid={pid}"),
            None => format!("{status} pid=unknown"),
        }
    } else {
        status
    }
}

/// Shows when the last run happened plus how long it took: "took X" for a
/// completed run, "running for X" while still active, or just the "ago"
/// timestamp with no duration when the run length isn't known (e.g.
/// interrupted before finishing).
pub(crate) fn format_last_run(job: &service::JobStatusResponse, now: DateTime<Local>) -> String {
    if matches!(job.status, state::JobStatus::Running) {
        let Some(started) = job.started_at else {
            return "never".to_string();
        };
        let elapsed = format_duration_precise(now.signed_duration_since(started));
        return format!("{} (running for {elapsed})", format_timestamp(started));
    }

    let Some(time) = job.finished_at.or(job.started_at) else {
        return "never".to_string();
    };
    let ago = format_duration(now.signed_duration_since(time));

    match (job.started_at, job.finished_at) {
        (Some(started), Some(finished)) => {
            let took = format_duration_precise(finished.signed_duration_since(started));
            format!("{} ({ago} ago, took {took})", format_timestamp(time))
        }
        _ => format!("{} ({ago} ago)", format_timestamp(time)),
    }
}

fn format_service_last_run(
    service: &service::ServiceStatusResponse,
    now: DateTime<Local>,
) -> String {
    if matches!(service.status, state::JobStatus::Running) {
        let Some(started) = service.started_at else {
            return "never".to_string();
        };
        let elapsed = format_duration_precise(now.signed_duration_since(started));
        return format!("{} (running for {elapsed})", format_timestamp(started));
    }

    let Some(time) = service.finished_at.or(service.started_at) else {
        return "never".to_string();
    };
    let ago = format_duration(now.signed_duration_since(time));

    match (service.started_at, service.finished_at) {
        (Some(started), Some(finished)) => {
            let took = format_duration_precise(finished.signed_duration_since(started));
            format!("{} ({ago} ago, took {took})", format_timestamp(time))
        }
        _ => format!("{} ({ago} ago)", format_timestamp(time)),
    }
}

fn format_next_run(job: &service::JobStatusResponse, now: DateTime<Local>) -> String {
    match job.trigger.kind.as_str() {
        "manual" => return "manual".yellow().to_string(),
        "dependency" => {
            return job
                .trigger
                .after
                .as_deref()
                .map(|upstream| format!("after {upstream}").cyan().to_string())
                .unwrap_or_else(|| "after unknown".to_string());
        }
        _ => {}
    }

    job.next_run
        .map(|time| {
            format!(
                "{} (in {})",
                format_timestamp(time),
                format_duration(time.signed_duration_since(now))
            )
        })
        .unwrap_or_else(|| "none found".to_string())
}

fn format_service_next_run(
    service: &service::ServiceStatusResponse,
    now: DateTime<Local>,
) -> String {
    match (service.next_start, service.next_stop) {
        (Some(start), Some(stop)) => format!(
            "start {} (in {}) / stop {} (in {})",
            format_timestamp(start),
            format_duration(start.signed_duration_since(now)),
            format_timestamp(stop),
            format_duration(stop.signed_duration_since(now))
        ),
        (Some(start), None) => format!(
            "start {} (in {})",
            format_timestamp(start),
            format_duration(start.signed_duration_since(now))
        ),
        (None, Some(stop)) => format!(
            "stop {} (in {})",
            format_timestamp(stop),
            format_duration(stop.signed_duration_since(now))
        ),
        (None, None) => service.schedule.clone(),
    }
}

fn format_timestamp(time: DateTime<Local>) -> String {
    time.format("%Y-%m-%d %H:%M:%S %:z").to_string()
}

fn format_duration(duration: chrono::Duration) -> String {
    let total_milliseconds = duration.num_milliseconds().max(0);
    let total_seconds = total_milliseconds / 1_000;
    format_duration_seconds(total_seconds)
}

fn format_duration_precise(duration: chrono::Duration) -> String {
    let total_milliseconds = duration.num_milliseconds().max(0);
    let total_seconds = total_milliseconds / 1_000;
    let milliseconds = total_milliseconds % 1_000;
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
    parts.push(format!("{seconds}.{milliseconds:03}s"));

    parts.join(" ")
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

#[cfg(test)]
mod tests {
    use chrono::TimeZone;
    use uuid::Uuid;

    use super::*;

    fn job_response(status: state::JobStatus) -> service::JobStatusResponse {
        service::JobStatusResponse {
            uuid: Uuid::new_v4(),
            name: "example".to_string(),
            group: None,
            status,
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
    fn format_duration_picks_the_coarsest_relevant_units() {
        assert_eq!(format_duration(chrono::Duration::zero()), "0s");
        assert_eq!(format_duration(chrono::Duration::milliseconds(250)), "0s");
        assert_eq!(format_duration(chrono::Duration::seconds(45)), "45s");
        assert_eq!(format_duration(chrono::Duration::seconds(90)), "1m 30s");
        assert_eq!(format_duration(chrono::Duration::seconds(3661)), "1h 1m 1s");
        assert_eq!(format_duration(chrono::Duration::seconds(90_000)), "1d 1h");
        assert_eq!(
            format_duration_precise(chrono::Duration::milliseconds(250)),
            "0.250s"
        );
        assert_eq!(
            format_duration_precise(chrono::Duration::milliseconds(1250)),
            "1.250s"
        );
        assert_eq!(
            format_duration_precise(chrono::Duration::seconds(5)),
            "5.000s"
        );
        assert_eq!(
            format_duration_precise(chrono::Duration::milliseconds(75_250)),
            "1m 15.250s"
        );
        assert_eq!(
            format_duration_precise(chrono::Duration::milliseconds(7_384_005)),
            "2h 3m 4.005s"
        );
    }

    #[test]
    fn format_last_run_shows_never_when_there_is_no_history() {
        let now = Local.with_ymd_and_hms(2026, 1, 1, 12, 0, 0).unwrap();
        let job = job_response(state::JobStatus::Idle);

        assert_eq!(format_last_run(&job, now), "never");
    }

    #[test]
    fn format_last_run_shows_elapsed_time_while_running() {
        let now = Local.with_ymd_and_hms(2026, 1, 1, 12, 0, 10).unwrap();
        let mut job = job_response(state::JobStatus::Running);
        job.started_at = Some(Local.with_ymd_and_hms(2026, 1, 1, 12, 0, 0).unwrap());

        assert!(format_last_run(&job, now).ends_with("(running for 10.000s)"));
    }

    #[test]
    fn format_last_run_shows_took_duration_for_a_completed_run() {
        let now = Local.with_ymd_and_hms(2026, 1, 1, 12, 1, 0).unwrap();
        let mut job = job_response(state::JobStatus::Succeeded);
        job.started_at = Some(Local.with_ymd_and_hms(2026, 1, 1, 12, 0, 0).unwrap());
        job.finished_at = Some(Local.with_ymd_and_hms(2026, 1, 1, 12, 0, 5).unwrap());

        assert!(format_last_run(&job, now).ends_with("(55s ago, took 5.000s)"));
    }

    #[test]
    fn format_last_run_omits_duration_when_run_length_is_unknown() {
        // e.g. Interrupted: started_at is known but finished_at never was.
        let now = Local.with_ymd_and_hms(2026, 1, 1, 12, 1, 0).unwrap();
        let mut job = job_response(state::JobStatus::Interrupted);
        job.started_at = Some(Local.with_ymd_and_hms(2026, 1, 1, 12, 0, 0).unwrap());

        assert_eq!(
            format_last_run(&job, now),
            format!("{} (1m ago)", format_timestamp(job.started_at.unwrap()))
        );
    }

    #[test]
    fn format_next_run_reports_manual_trigger_jobs_distinctly() {
        let now = Local.with_ymd_and_hms(2026, 1, 1, 12, 0, 0).unwrap();
        let mut job = job_response(state::JobStatus::Idle);
        job.trigger.kind = "manual".to_string();

        assert!(format_next_run(&job, now).contains("manual"));
    }

    #[test]
    fn format_next_run_shows_none_found_without_a_next_run() {
        let now = Local.with_ymd_and_hms(2026, 1, 1, 12, 0, 0).unwrap();
        let job = job_response(state::JobStatus::Idle);

        assert_eq!(format_next_run(&job, now), "none found");
    }
}
