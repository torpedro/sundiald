# Configuration

Generate a starter config:

```sh
mkdir -p ~/.config/sundiald
cargo run -- sample-config > ~/.config/sundiald/config.yaml
```

```yaml
state_dir: /home/you/.local/state/sundiald
log_dir: /home/you/.local/state/sundiald/logs
service_log: /home/you/.local/state/sundiald/sundiald.log
api_bind: 127.0.0.1:8787
# Required when api_bind is not a loopback address. CLI commands send it as a bearer token.
# api_token: "replace-with-a-long-random-secret"
# What to do when the daemon misses scheduled seconds while suspended or busy:
# `skip` ignores them; `run_once` runs each affected job once after recovery.
missed_run_policy: skip
# Delete job log files older than this many days. Set to 0 to keep logs forever.
log_retention_days: 14
# Time to wait for running processes to exit after SIGTERM during daemon shutdown.
shutdown_grace_period: "30s"
alert:
  log: /home/you/.local/state/sundiald/alerts.log
  event_dir: /home/you/.local/state/sundiald/alerts
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
  # Optional Flares output. Credentials are read from this config file.
  # flares:
  #   url: "http://127.0.0.1:8000"
  #   token: "your-flares-api-token"
  #   title: "sundiald"
  #   severity: warning
# Environment variables inherited by inline jobs and services in this file.
env:
  APP_ENV: production
# Optional named files containing additional job and service definitions.
# Each file is a YAML map with optional `env`, `jobs`, and `services` lists.
# job_files:
#   - name: maintenance
#     path: maintenance.yaml
jobs:
  - name: heartbeat
    uuid: a63d6b30-d69d-4e08-946e-1ad554d0d541
    command: "echo sundiald is alive"
    trigger:
      schedule: "0 */1 * * * mon-sun"
  - name: long-lived
    uuid: 87b8069d-2fd9-487e-852a-066314cb1f77
    command: "echo sleeping; sleep 30; echo awake"
    # Fire an alert if this job is still running after 20 seconds.
    alert_if_running_for_longer_than: "20s"
    trigger:
      schedule: "30 */5 * * * mon-sun"
  - name: fails
    uuid: 14036dee-250c-4625-a3d6-21a068f82a4a
    command: "echo this job fails; exit 42"
    trigger: manual
services:
  - name: web
    uuid: 3e6012cb-d80f-4645-9b1f-15b943b35a83
    command: "python3 -m http.server 8080"
    schedule: permanent
  - name: office-worker
    uuid: 8bb33865-08a4-47ef-bdbf-028108c99c42
    command: "bin/worker"
    stop_grace_period: "45s"
    schedule:
      start: "0 0 9 * * mon-fri"
      stop: "0 0 17 * * mon-fri"
```

Each job has one `trigger`: `schedule`, `after`, or `manual`. Scheduled jobs run by time. Dependency jobs use `trigger.after: <job-name>` and run when that upstream job finishes successfully, including when the upstream was started manually. Manual jobs never run automatically but remain runnable through the CLI/API/UI.

Schedules use a six-field cron expression under `trigger.schedule`: `second minute hour day-of-month month day-of-week`. Fields accept `*`, exact numbers, ranges like `1-5`, steps like `*/15`, and comma-separated values like `1,15,30`. Seconds and minutes use `0` through `59`; hours use `0` through `23`. Weekdays accept `mon` through `sun`; months accept `jan` through `dec` or `1` through `12`.

`missed_run_policy` controls schedule ticks missed while the daemon process is alive but delayed or suspended. The default, `skip`, only considers the current second. `run_once` starts each affected job once after recovery, using its most recent missed occurrence; it does not replay every missed occurrence and does not catch up time elapsed while sundiald was stopped. For windowed services, only the most recent missed start or stop transition is applied.

Service entries are for commands expected to keep running. `schedule: permanent` declares a manually controlled service with no automatic start/stop times. A window schedule uses `schedule.start` and `schedule.stop`, both six-field cron expressions; matching `start` ticks start the service if it is not already running, and matching `stop` ticks send SIGTERM. Services are not automatically started when the daemon starts or reloads, even if they are permanent or currently inside a configured runtime window. Use `stop_grace_period` to control how long sundiald waits after a service is outside its runtime before alerting that it is still running; the default is `30s`.

Job and service `command` strings are executed through `sh -c`, so standard shell environment expansion works there, e.g. `$HOME`, `${HOME}`, and variables assigned earlier in the command string. Environment variables under the root `env` map are inherited by inline jobs and services in `sundiald.yaml`; external job files can define their own top-level `env` map for jobs and services in that file. Config path fields are resolved as paths and are not shell-expanded.

If both day-of-week and day-of-month are restricted (not left as `*`), a day matches when *either* is satisfied, matching standard cron semantics — e.g. `0 0 9 1 * mon` runs at 09:00:00 on the 1st of the month *or* on Mondays, not only on a Monday that happens to be the 1st. If only one of the two is restricted, only that one applies.

Failures are appended to `alert.log` and also written as JSON files under `alert.event_dir`. If `alert.command` is present, sundiald runs that configured program with configured args. If `alert.pushover` is present, sundiald sends the alert to Pushover using the configured application token and user/group key. Sundiald does not pass alert data through environment variables.

If `alert.flares` is present, sundiald sends a one-shot alert through `flares-client` to the configured Flares server. `url` and `token` are required; `title` defaults to `sundiald: <job-name>`, and `severity` defaults to `warning` (also accepts `info` or `critical`). Each message starts with the job or service name. Flares requests time out after 15 seconds and are not retried automatically. Pending deliveries are accepted for processing; other unsuccessful delivery statuses are logged. Titles and messages are truncated to Flares’ limits of 250 and 1024 Unicode characters; the local event retains the full message.

A failure to deliver to `alert.command`, Pushover, or Flares is logged to stderr but does not itself generate another alert.

Set a job's optional `alert_if_running_for_longer_than` (e.g. `"45s"`, `"10m"`, `"2h"`, `"1d"`, or a compound value like `"1h30m"`) to fire the same alert channels once if a run is still active past that threshold — useful for catching a job that's hung or unexpectedly slow. It fires at most once per run (not repeated for the rest of that run) and doesn't affect the job itself; it keeps running either way.

By default, runtime state, history, and logs are written under `$HOME/.local/state/sundiald`; `sample-config` prints this as an absolute path for the current user. Each job and service gets a directory under `log_dir` named after the sanitized name. Each run gets a collision-resistant timestamp-and-UUID filename and writes stdout to `<run>.stdout.log`; stderr is written to a sibling `<run>.stderr.log` only if the process actually writes stderr. Output is copied byte-for-byte, including non-UTF-8 and unterminated lines; the JSON log API uses lossy UTF-8 conversion only for display. Log files under `log_dir` and alert event JSON files under `alert.event_dir` are pruned automatically based on `log_retention_days` and `alert.retention_days` (checked once at startup and then hourly), including logs inside per-job directories. Empty per-job directories are removed. Set either retention value to `0` to keep files forever.

Each job and service has a stable `uuid` used internally to track it across renames — `name` is just a label. You don't need to set `uuid` by hand: `daemon` (and `reload`) generate one for any entry missing it and write it back into the YAML file in place, next to that entry's `name`, without disturbing comments or formatting elsewhere in the file. As long as you keep the `uuid` line when you rename an entry, the service recognizes it as the same runnable across the rename.

Use `job_files` to split job definitions into named external files:

```yaml
job_files:
  - name: maintenance
    path: maintenance.yaml
  - name: reports
    path: /etc/sundiald/reports.yaml
```

Each referenced file must be a YAML map with optional `env`, `jobs`, and `services` sections:

```yaml
env:
  APP_ENV: production
  REPORT_ROOT: /srv/reports
jobs:
  - name: rotate-logs
    command: "/usr/local/bin/rotate-app-logs"
    trigger:
      schedule: "0 0 3 * * *"
  - name: rebuild-report
    command: "/usr/local/bin/rebuild-report"
    trigger: manual
services:
  - name: report-watcher
    command: "/usr/local/bin/report-watcher"
    schedule:
      start: "0 0 8 * * mon-fri"
      stop: "0 0 18 * * mon-fri"
```

See [examples/maintenance.yaml](../examples/maintenance.yaml) for a complete external jobs file.

Relative `job_files.path` values are resolved relative to the main config file. Jobs and services loaded from a job file keep the file's `name` as their `group` in the HTTP status response, and inherit any environment variables from that file's optional top-level `env` map. Missing UUIDs are written back to the file that defined the entry, not necessarily the main config.
