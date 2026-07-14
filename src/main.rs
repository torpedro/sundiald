mod cli;
mod config;
mod service;
mod state;

use std::{env, path::PathBuf};

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
    /// Run the long-lived job runner daemon.
    Daemon {
        /// YAML config file to load.
        #[arg(short, long)]
        config: Option<PathBuf>,
    },
    /// Validate and summarize the YAML config.
    Config {
        /// YAML config file to inspect.
        #[arg(short, long)]
        config: Option<PathBuf>,
    },
    /// Run a configured job immediately.
    Run {
        /// Job name to run.
        job: String,
        /// YAML config file to load.
        #[arg(short, long)]
        config: Option<PathBuf>,
    },
    /// Send SIGTERM to a running job.
    Terminate {
        /// Job name to terminate.
        job: String,
        /// YAML config file to load.
        #[arg(short, long)]
        config: Option<PathBuf>,
    },
    /// Send SIGKILL to a running job.
    Kill {
        /// Job name to kill.
        job: String,
        /// YAML config file to load.
        #[arg(short, long)]
        config: Option<PathBuf>,
    },
    /// Start a configured service.
    StartService {
        /// Service name or UUID to start.
        service: String,
        /// YAML config file to load.
        #[arg(short, long)]
        config: Option<PathBuf>,
    },
    /// Stop a running service with SIGTERM.
    StopService {
        /// Service name or UUID to stop.
        service: String,
        /// YAML config file to load.
        #[arg(short, long)]
        config: Option<PathBuf>,
    },
    /// Stop a running service with SIGKILL.
    KillService {
        /// Service name or UUID to kill.
        service: String,
        /// YAML config file to load.
        #[arg(short, long)]
        config: Option<PathBuf>,
    },
    /// Tell the running service to reload its config from disk.
    Reload {
        /// YAML config file to load (used to locate the running service's API).
        #[arg(short, long)]
        config: Option<PathBuf>,
    },
    /// Show recent run history for a job.
    History {
        /// Job name or UUID to inspect.
        job: String,
        /// YAML config file to load.
        #[arg(short, long)]
        config: Option<PathBuf>,
        /// Maximum number of runs to show.
        #[arg(short, long, default_value_t = 20)]
        limit: usize,
    },
    /// Open the interactive status UI.
    Ui {
        /// YAML config file to inspect.
        #[arg(short, long)]
        config: Option<PathBuf>,
        /// Print one status frame instead of opening the interactive UI.
        #[arg(long)]
        once: bool,
    },
    /// Print a starter YAML config.
    SampleConfig,
}

fn default_config_path() -> PathBuf {
    env::var_os("HOME")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("~"))
        .join(".config/sundiald/config.yaml")
}

fn resolve_config_path(config: Option<PathBuf>) -> PathBuf {
    config.unwrap_or_else(default_config_path)
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    match cli.command {
        Command::Daemon {
            config: config_path,
        } => {
            let config_path = resolve_config_path(config_path);
            let config = SundialdConfig::load_and_ensure_ids(&config_path)?;
            service::run(config, config_path).await
        }
        Command::Config { config } => {
            let config = resolve_config_path(config);
            let config = SundialdConfig::load(&config)?;
            println!("config: ok");
            println!("state_dir: {}", config.state_dir.display());
            println!("log_dir: {}", config.log_dir.display());
            println!("log_retention_days: {}", config.log_retention_days);
            println!("shutdown_grace_period: {}", config.shutdown_grace_period);
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
            println!("job_files: {}", config.job_files.len());
            for job_file in &config.job_files {
                println!("- {}: {}", job_file.name, job_file.path.display());
            }
            println!("jobs: {}", config.jobs.len());
            for job in &config.jobs {
                let group = job
                    .group
                    .as_deref()
                    .map(|group| format!(" group={group}"))
                    .unwrap_or_default();
                match job.uuid {
                    Some(uuid) => println!("- {} [{uuid}{group}]: {}", job.name, job.command),
                    None => println!("- {} [no uuid yet{group}]: {}", job.name, job.command),
                }
            }
            println!("services: {}", config.services.len());
            for service in &config.services {
                let group = service
                    .group
                    .as_deref()
                    .map(|group| format!(" group={group}"))
                    .unwrap_or_default();
                match service.uuid {
                    Some(uuid) => {
                        println!("- {} [{uuid}{group}]: {}", service.name, service.command)
                    }
                    None => println!(
                        "- {} [no uuid yet{group}]: {}",
                        service.name, service.command
                    ),
                }
            }
            Ok(())
        }
        Command::Run { job, config } => {
            let config = resolve_config_path(config);
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
            let config = resolve_config_path(config);
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
            let config = resolve_config_path(config);
            let config = SundialdConfig::load(&config)?;
            cli::post_job_action(&config, &job, "kill", &format!("sent SIGKILL to {job}")).await
        }
        Command::StartService { service, config } => {
            let config = resolve_config_path(config);
            let config = SundialdConfig::load(&config)?;
            cli::post_service_action(
                &config,
                &service,
                "start",
                &format!("queued service start for {service}"),
            )
            .await
        }
        Command::StopService { service, config } => {
            let config = resolve_config_path(config);
            let config = SundialdConfig::load(&config)?;
            cli::post_service_action(
                &config,
                &service,
                "stop",
                &format!("sent SIGTERM to {service}"),
            )
            .await
        }
        Command::KillService { service, config } => {
            let config = resolve_config_path(config);
            let config = SundialdConfig::load(&config)?;
            cli::post_service_action(
                &config,
                &service,
                "kill",
                &format!("sent SIGKILL to {service}"),
            )
            .await
        }
        Command::Reload { config } => {
            let config = resolve_config_path(config);
            let config = SundialdConfig::load(&config)?;
            cli::reload_config(&config).await
        }
        Command::History { job, config, limit } => {
            let config = resolve_config_path(config);
            let config = SundialdConfig::load(&config)?;
            cli::print_history(&config, &job, limit).await
        }
        Command::Ui { config, once } => {
            let config = resolve_config_path(config);
            let config = SundialdConfig::load(&config)?;
            if once {
                cli::print_status(&config).await?;
            } else {
                cli::watch_status(config).await?;
            }
            Ok(())
        }
        Command::SampleConfig => {
            print!("{}", config::sample_config());
            Ok(())
        }
    }
}
