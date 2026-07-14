use crate::{
    config::AlertConfig,
    state::{JobState, JobStatus, StateSnapshot},
};

use super::alert::write_alert;

#[derive(Debug, Clone, PartialEq, Eq)]
struct OrphanCandidate {
    job_name: String,
    pid: u32,
}

pub(crate) async fn alert_orphaned_process_groups(alert: &AlertConfig, snapshot: &StateSnapshot) {
    for candidate in orphan_candidates(snapshot) {
        if !process_group_exists(candidate.pid) {
            continue;
        }
        write_alert(
            alert,
            &candidate.job_name,
            &format!(
                "orphaned process group detected after restart: pid={} is still present; \
                 sundiald will not kill it automatically",
                candidate.pid
            ),
        )
        .await;
    }
}

fn orphan_candidates(snapshot: &StateSnapshot) -> Vec<OrphanCandidate> {
    snapshot.jobs.iter().filter_map(orphan_candidate).collect()
}

fn orphan_candidate(job: &JobState) -> Option<OrphanCandidate> {
    if !matches!(job.status, JobStatus::Running) {
        return None;
    }
    Some(OrphanCandidate {
        job_name: job.name.clone(),
        pid: job.pid?,
    })
}

#[cfg(unix)]
fn process_group_exists(pid: u32) -> bool {
    let Ok(pgid) = libc::pid_t::try_from(pid) else {
        return false;
    };
    let result = unsafe { libc::kill(-pgid, 0) };
    result == 0
        || std::io::Error::last_os_error()
            .raw_os_error()
            .is_some_and(|code| code == libc::EPERM)
}

#[cfg(not(unix))]
fn process_group_exists(_pid: u32) -> bool {
    false
}

#[cfg(test)]
mod tests {
    use chrono::Local;
    use uuid::Uuid;

    use super::*;

    fn job_state(name: &str, status: JobStatus, pid: Option<u32>) -> JobState {
        JobState {
            uuid: Uuid::new_v4(),
            name: name.to_string(),
            status,
            pid,
            started_at: Some(Local::now()),
            finished_at: None,
            exit_code: None,
            log_path: None,
            last_error: None,
            terminated_by_signal: None,
        }
    }

    #[test]
    fn orphan_candidates_only_include_previously_running_jobs_with_pids() {
        let snapshot = StateSnapshot {
            updated_at: Local::now(),
            revision: 0,
            jobs: vec![
                job_state("running", JobStatus::Running, Some(123)),
                job_state("missing-pid", JobStatus::Running, None),
                job_state("finished", JobStatus::Succeeded, Some(456)),
            ],
        };

        assert_eq!(
            orphan_candidates(&snapshot),
            vec![OrphanCandidate {
                job_name: "running".to_string(),
                pid: 123,
            }]
        );
    }
}
