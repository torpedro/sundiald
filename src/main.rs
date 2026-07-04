mod cli;
mod config;
mod service;
mod state;

use std::path::PathBuf;

use anyhow::Result;
use clap::{Parser, Subcommand};

use crate::config::SundialdConfig;

#[derive(Debug, Parser)]
#[command(name = "sundiald", version, about = "A scheduled shell job runner")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Run the long-lived job runner service.
    Serve {
        /// YAML config file to load.
        #[arg(short, long, default_value = "sundiald.yaml")]
        config: PathBuf,
    },
    /// Validate and summarize the YAML config.
    Config {
        /// YAML config file to inspect.
        #[arg(short, long, default_value = "sundiald.yaml")]
        config: PathBuf,
    },
    /// Run a configured job immediately.
    Run {
        /// Job name to run.
        job: String,
        /// YAML config file to load.
        #[arg(short, long, default_value = "sundiald.yaml")]
        config: PathBuf,
    },
    /// Send SIGTERM to a running job.
    Terminate {
        /// Job name to terminate.
        job: String,
        /// YAML config file to load.
        #[arg(short, long, default_value = "sundiald.yaml")]
        config: PathBuf,
    },
    /// Send SIGKILL to a running job.
    Kill {
        /// Job name to kill.
        job: String,
        /// YAML config file to load.
        #[arg(short, long, default_value = "sundiald.yaml")]
        config: PathBuf,
    },
    /// Tell the running service to reload its config from disk.
    Reload {
        /// YAML config file to load (used to locate the running service's API).
        #[arg(short, long, default_value = "sundiald.yaml")]
        config: PathBuf,
    },
    /// Show configured jobs, high-level run status, last run, and next run.
    Status {
        /// YAML config file to inspect.
        #[arg(short, long, default_value = "sundiald.yaml")]
        config: PathBuf,
        /// Keep status live and refresh every second.
        #[arg(short, long)]
        watch: bool,
    },
    /// Print a starter YAML config.
    SampleConfig,
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    match cli.command {
        Command::Serve {
            config: config_path,
        } => {
            let config = SundialdConfig::load_and_ensure_ids(&config_path)?;
            service::run(config, config_path).await
        }
        Command::Config { config } => {
            let config = SundialdConfig::load(&config)?;
            println!("config: ok");
            println!("state_dir: {}", config.state_dir.display());
            println!("log_dir: {}", config.log_dir.display());
            println!("log_retention_days: {}", config.log_retention_days);
            println!("service_log: {}", config.service_log.display());
            println!("api_bind: {}", config.api_bind);
            println!("alert.log: {}", config.alert.log.display());
            println!("alert.event_dir: {}", config.alert.event_dir.display());
            println!("alert.retention_days: {}", config.alert.retention_days);
            if let Some(alert_command) = &config.alert.command {
                println!(
                    "alert.command: {} {}",
                    alert_command.program,
                    alert_command.args.join(" ")
                );
            }
            if config.alert.pushover.is_some() {
                println!("alert.pushover: configured");
            }
            println!("jobs: {}", config.jobs.len());
            for job in &config.jobs {
                match job.uuid {
                    Some(uuid) => println!("- {} [{uuid}]: {}", job.name, job.command),
                    None => println!("- {} [no uuid yet]: {}", job.name, job.command),
                }
            }
            Ok(())
        }
        Command::Run { job, config } => {
            let config = SundialdConfig::load(&config)?;
            cli::post_job_action(
                &config,
                &job,
                "run",
                &format!("queued manual run for {job}"),
            )
            .await
        }
        Command::Terminate { job, config } => {
            let config = SundialdConfig::load(&config)?;
            cli::post_job_action(
                &config,
                &job,
                "terminate",
                &format!("sent SIGTERM to {job}"),
            )
            .await
        }
        Command::Kill { job, config } => {
            let config = SundialdConfig::load(&config)?;
            cli::post_job_action(&config, &job, "kill", &format!("sent SIGKILL to {job}")).await
        }
        Command::Reload { config } => {
            let config = SundialdConfig::load(&config)?;
            cli::reload_config(&config).await
        }
        Command::Status { config, watch } => {
            let config = SundialdConfig::load(&config)?;
            if watch {
                cli::watch_status(config).await?;
            } else {
                cli::print_status(&config).await?;
            }
            Ok(())
        }
        Command::SampleConfig => {
            print!("{}", config::sample_config());
            Ok(())
        }
    }
}
