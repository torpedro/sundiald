mod client;
mod render;
mod watch;

use anyhow::Result;

use crate::config::SundialdConfig;
use client::{encode_path_segment, post_api, report_response};

pub(crate) use render::print_status;
pub(crate) use watch::watch_status;

/// Shared implementation for the `run`/`terminate`/`kill` CLI commands: each
/// just POSTs to a different job-control endpoint and prints a different
/// success message.
///
/// Deliberately does not check `job` against the locally loaded config
/// first: the server is the source of truth on whether a job exists or is
/// running. A job renamed or removed via `reload` keeps running under its
/// old name until it finishes (see `api_signal_job` in service/api.rs), so a
/// local pre-check here would incorrectly block `terminate`/`kill` on a
/// name the local config no longer lists but the server is still tracking.
pub(crate) async fn post_job_action(
    config: &SundialdConfig,
    job: &str,
    action: &str,
    success_message: &str,
) -> Result<()> {
    let job = encode_path_segment(job);
    let response = post_api(config, &format!("/jobs/{job}/{action}")).await?;
    report_response(response, action, success_message).await
}

pub(crate) async fn reload_config(config: &SundialdConfig) -> Result<()> {
    let response = post_api(config, "/reload").await?;
    report_response(response, "reload", "config reloaded").await
}
