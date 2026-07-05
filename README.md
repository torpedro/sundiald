# sundiald

`sundiald` is a small Rust job runner. It reads a YAML config, starts shell commands on a schedule, captures stdout/stderr to disk, records state for inspection, logs job start/finish events to stdout and its own log file, and writes alerts for jobs that exit with a non-zero status code.

## Build

```sh
cargo build
```

## Example config

Generate a starter config:

```sh
mkdir -p ~/.config/sundiald
cargo run -- sample-config > ~/.config/sundiald/config.yaml
```

```yaml
state_dir: .sundiald
log_dir: .sundiald/logs
service_log: .sundiald/sundiald.log
api_bind: 127.0.0.1:8787
# Delete job log files older than this many days. Set to 0 to keep logs forever.
log_retention_days: 14
alert:
  log: .sundiald/alerts.log
  event_dir: .sundiald/alerts
  # Delete alert event JSON files older than this many days. Set to 0 to keep forever.
  retention_days: 90
  # Optional command run when a job fails. No environment variables are used.
  # Placeholders available in args: {job}, {message}, {alert_file}
  # command:
  #   program: /usr/local/bin/sundiald-alert
  #   args: ["--event", "{alert_file}"]
  # Optional Pushover output. Credentials are read from this config file.
  # pushover:
  #   token: "your-pushover-application-token"
  #   user: "your-pushover-user-or-group-key"
  #   title: "sundiald"
  #   priority: 0
# Optional named files containing additional job definitions.
# Each file is a YAML list of jobs, using the same shape as entries under `jobs`.
# job_files:
#   - name: maintenance
#     path: jobs/maintenance.yaml
jobs:
  - name: heartbeat
    uuid: a63d6b30-d69d-4e08-946e-1ad554d0d541
    command: "echo sundiald is alive"
    schedule:
      seconds: ["0"]
      minutes: ["*/1"]
      hours: ["*"]
      days_of_week: ["mon", "tue", "wed", "thu", "fri", "sat", "sun"]
      days_of_month: ["*"]
      months: ["*"]
  - name: long-lived
    uuid: 87b8069d-2fd9-487e-852a-066314cb1f77
    command: "echo sleeping; sleep 30; echo awake"
    # Fire an alert if this job is still running after 20 seconds.
    alert_if_running_for_longer_than: "20s"
    schedule:
      seconds: ["30"]
      minutes: ["*/5"]
      hours: ["*"]
      days_of_week: ["mon", "tue", "wed", "thu", "fri", "sat", "sun"]
      days_of_month: ["*"]
      months: ["*"]
  - name: fails
    uuid: 14036dee-250c-4625-a3d6-21a068f82a4a
    command: "echo this job fails; exit 42"
    schedule:
      manual_only: true
```

Schedule fields accept `*`, exact numbers, ranges like `1-5`, steps like `*/15`, and comma-separated values like `1,15,30`. For non-manual jobs, `seconds`, `minutes`, and `hours` are required. Seconds and minutes use `0` through `59`; hours use `0` through `23`. Weekdays accept `mon` through `sun`; months accept `jan` through `dec` or `1` through `12`. Day and month fields default to `["*"]` when omitted. Set `manual_only: true` in a schedule to disable scheduled runs while keeping manual runs available.

Job `command` strings are executed through `sh -c`, so standard shell environment expansion works there, e.g. `$HOME`, `${HOME}`, and variables assigned earlier in the command string. Config path fields are resolved as paths and are not shell-expanded.

If both `days_of_week` and `days_of_month` are restricted (not left as `*`), a day matches when *either* is satisfied, matching standard cron semantics — e.g. `days_of_month: ["1"]` plus `days_of_week: ["mon"]` runs on the 1st of the month *or* on Mondays, not only on a Monday that happens to be the 1st. If only one of the two is restricted, only that one applies.

Failures are appended to `alert.log` and also written as JSON files under `alert.event_dir`. If `alert.command` is present, sundiald runs that configured program with configured args. If `alert.pushover` is present, sundiald sends the alert to Pushover using the configured application token and user/group key. Sundiald does not pass alert data through environment variables. A failure to deliver to `alert.command` or Pushover is logged to stderr but does not itself generate another alert.

Set a job's optional `alert_if_running_for_longer_than` (e.g. `"45s"`, `"10m"`, `"2h"`, `"1d"`, or a compound value like `"1h30m"`) to fire the same alert channels once if a run is still active past that threshold — useful for catching a job that's hung or unexpectedly slow. It fires at most once per run (not repeated for the rest of that run) and doesn't affect the job itself; it keeps running either way.

Job log files under `log_dir` and alert event JSON files under `alert.event_dir` are pruned automatically based on `log_retention_days` and `alert.retention_days` (checked once at startup and then hourly). Set either to `0` to keep files forever.

Each job has a stable `uuid` used internally to track it across renames — `name` is just a label. You don't need to set `uuid` by hand: `daemon` (and `reload`) generate one for any job missing it and write it back into the YAML file in place, next to that job's `name`, without disturbing comments or formatting elsewhere in the file. As long as you keep the `uuid` line when you rename a job, the service recognizes it as the same job across the rename — its live/last run status carries over under the new name instead of resetting. Removing a job from the config entirely (not renaming it) while it's still running leaves it visible in `status` and controllable by its last-known name until it finishes, since there's no new name to carry it forward to.

Use `job_files` to split job definitions into named external files:

```yaml
job_files:
  - name: maintenance
    path: jobs/maintenance.yaml
  - name: reports
    path: /etc/sundiald/reports.yaml
```

Each referenced file is a YAML list of job definitions:

```yaml
- name: rotate-logs
  command: "/usr/local/bin/rotate-app-logs"
  schedule:
    seconds: ["0"]
    minutes: ["0"]
    hours: ["3"]
- name: rebuild-report
  command: "/usr/local/bin/rebuild-report"
  schedule:
    manual_only: true
```

Relative `job_files.path` values are resolved relative to the main config file. Jobs loaded from a job file keep the file's `name` as their `group` in the HTTP status response, so callers can present them grouped by source later. Missing job UUIDs are written back to the file that defined the job, not necessarily the main config.

## Run the service

```sh
cargo run -- daemon
```

When `--config` is not supplied, sundiald reads `~/.config/sundiald/config.yaml`. Pass `--config <path>` to use a different config file.

Job state (status, last run, exit code) persists in `state_dir/state.json` and is reloaded on startup, so `status` reflects history across restarts. A job that was `running` when the service last stopped is marked `interrupted`, since its process died with the previous instance and its actual outcome is unknown. On Unix, if sundiald sees that the previous run's process group is still present during startup, it writes an alert for the orphaned process group but does not kill it automatically.

Run history is recorded in a SQLite database at `state_dir/history.sqlite3`. Each run inserts a row when the job is triggered, including the trigger time and whether it was `automatic` or `manual`; when the run finishes, that row is updated with finish time, duration in milliseconds, and exit code. A job that fails to start still gets a history row with a finish time and duration, but no exit code.

```sh
cargo run -- reload
```

Reloads the config from disk without restarting — this picks up job/schedule/log/alert changes. The config is validated before being swapped in; an invalid file is rejected and the service keeps running on its previous config. Changing `api_bind` still requires a full restart, since it means rebinding the HTTP listener.

## Install as a systemd service

Build and install the release binary somewhere on the system path:

```sh
cargo build --release
sudo install -m 0755 target/release/sundiald /usr/local/bin/sundiald
```

Create a dedicated service user and directories for config, state, and logs:

```sh
sudo useradd --system --home /var/lib/sundiald --shell /usr/sbin/nologin sundiald
sudo install -d -o sundiald -g sundiald /etc/sundiald /var/lib/sundiald /var/log/sundiald /var/log/sundiald/jobs /var/log/sundiald/alerts
cargo run -- sample-config | sudo tee /etc/sundiald/sundiald.yaml >/dev/null
sudo chown root:sundiald /etc/sundiald/sundiald.yaml
sudo chmod 0640 /etc/sundiald/sundiald.yaml
```

Edit `/etc/sundiald/sundiald.yaml` so writable paths point at the service-owned directories:

```yaml
state_dir: /var/lib/sundiald
log_dir: /var/log/sundiald/jobs
service_log: /var/log/sundiald/sundiald.log
api_bind: 127.0.0.1:8787
alert:
  log: /var/log/sundiald/alerts.log
  event_dir: /var/log/sundiald/alerts
```

Create `/etc/systemd/system/sundiald.service`:

```ini
[Unit]
Description=sundiald scheduled job runner
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=sundiald
Group=sundiald
ExecStart=/usr/local/bin/sundiald daemon --config /etc/sundiald/sundiald.yaml
Restart=on-failure
RestartSec=5s

[Install]
WantedBy=multi-user.target
```

Enable and start the service:

```sh
sudo systemctl daemon-reload
sudo systemctl enable --now sundiald
sudo systemctl status sundiald
```

Use the installed binary to inspect or control the running service:

```sh
sundiald ui --config /etc/sundiald/sundiald.yaml
sundiald reload --config /etc/sundiald/sundiald.yaml
sundiald run heartbeat --config /etc/sundiald/sundiald.yaml
```

View service logs with `journalctl -u sundiald -f`. Per-job stdout/stderr logs are written under the configured `log_dir`, and alert events are written under `alert.event_dir`.

## Inspect

```sh
cargo run -- config
cargo run -- run heartbeat
cargo run -- terminate sleepy
cargo run -- kill sleepy
cargo run -- history heartbeat
cargo run -- ui
cargo run -- ui --once
```

The `ui` command opens the interactive view by default, grouping jobs from named job files under their configured group names. Use `ui --once` to print one status frame and exit. In interactive mode, use arrow keys or `j`/`k` to select a job, `Enter` to show the recent log file for the selected job, `h` to show recent run history, `s` to show the next 10 scheduled runs, `r` to run the selected job immediately, `T` to send SIGTERM, `K` to send SIGKILL, `R` to reload config, `Del` to clear details, and `q` to quit.

Job-control and history commands accept either a job name or a job UUID. UUIDs are stable across renames and are what the interactive UI uses internally.

Manual runs are requested through the HTTP API and executed by the long-running `daemon` process, so ad-hoc jobs are still child processes of the main sundiald service.

## HTTP API

`daemon` starts a local HTTP API at `api_bind`.

```sh
curl http://127.0.0.1:8787/status
curl http://127.0.0.1:8787/jobs/heartbeat/history
curl 'http://127.0.0.1:8787/jobs/heartbeat/logs/latest?tail=40'
curl -X POST http://127.0.0.1:8787/jobs/heartbeat/run
curl -X POST http://127.0.0.1:8787/jobs/sleepy/terminate
curl -X POST http://127.0.0.1:8787/jobs/sleepy/kill
curl -X POST http://127.0.0.1:8787/reload
```

The CLI uses this API for `ui`, `history`, `run`/`terminate`/`kill`, and `reload`, so the same surface can back a web UI later. Job route parameters accept either job names or UUIDs.

`/status` reports a `status` of `idle`, `running`, `succeeded`, `failed`, `start_failed`, or `interrupted` (was `running` when the service last restarted) per job, plus a `terminated_by_signal` field (`"SIGTERM"`/`"SIGKILL"`/`null`) when a run ended because it was signaled via `terminate`/`kill` rather than exiting on its own, a `uuid` field with the job's stable identity, a `group` field for jobs loaded from a named job file, and `next_runs` with up to 10 upcoming scheduled run times.

`/jobs/{job}/history?limit=50` returns recent SQLite run-history rows including trigger kind, start/finish times, duration, exit code, final status, error text, signal, group, and log path. `/jobs/{job}/logs/latest?tail=40` returns the recent stdout/stderr log content for the latest known run.
