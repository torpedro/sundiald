# sundiald

`sundiald` is a small Rust job runner. It reads a YAML config, starts shell commands on a schedule, captures stdout/stderr to disk, records state for inspection, logs job start/finish events to stdout and its own log file, and writes alerts for jobs that exit with a non-zero status code.

## Build

```sh
cargo build
```

## Example config

Generate a starter config:

```sh
cargo run -- sample-config > sundiald.yaml
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

Schedule fields accept `*`, exact numbers, ranges like `1-5`, steps like `*/15`, and comma-separated values like `1,15,30`. Seconds and minutes use `0` through `59`; hours use `0` through `23`. Weekdays accept `mon` through `sun`; months accept `jan` through `dec` or `1` through `12`. If `seconds` is omitted, it defaults to `["0"]` so older minute-based schedules still run once per matching minute. Set `manual_only: true` in a schedule to disable scheduled runs while keeping manual runs available.

If both `days_of_week` and `days_of_month` are restricted (not left as `*`), a day matches when *either* is satisfied, matching standard cron semantics — e.g. `days_of_month: ["1"]` plus `days_of_week: ["mon"]` runs on the 1st of the month *or* on Mondays, not only on a Monday that happens to be the 1st. If only one of the two is restricted, only that one applies.

Failures are appended to `alert.log` and also written as JSON files under `alert.event_dir`. If `alert.command` is present, sundiald runs that configured program with configured args. If `alert.pushover` is present, sundiald sends the alert to Pushover using the configured application token and user/group key. Sundiald does not pass alert data through environment variables. A failure to deliver to `alert.command` or Pushover is logged to stderr but does not itself generate another alert.

Set a job's optional `alert_if_running_for_longer_than` (e.g. `"45s"`, `"10m"`, `"2h"`, `"1d"`, or a compound value like `"1h30m"`) to fire the same alert channels once if a run is still active past that threshold — useful for catching a job that's hung or unexpectedly slow. It fires at most once per run (not repeated for the rest of that run) and doesn't affect the job itself; it keeps running either way.

Job log files under `log_dir` and alert event JSON files under `alert.event_dir` are pruned automatically based on `log_retention_days` and `alert.retention_days` (checked once at startup and then hourly). Set either to `0` to keep files forever.

Each job has a stable `uuid` used internally to track it across renames — `name` is just a label. You don't need to set `uuid` by hand: `serve` (and `reload`) generate one for any job missing it and write it back into the YAML file in place, next to that job's `name`, without disturbing comments or formatting elsewhere in the file. As long as you keep the `uuid` line when you rename a job, the service recognizes it as the same job across the rename — its live/last run status carries over under the new name instead of resetting. Removing a job from the config entirely (not renaming it) while it's still running leaves it visible in `status` and controllable by its last-known name until it finishes, since there's no new name to carry it forward to.

## Run the service

```sh
cargo run -- serve --config sundiald.yaml
```

Job state (status, last run, exit code) persists in `state_dir/state.json` and is reloaded on startup, so `status` reflects history across restarts. A job that was `running` when the service last stopped is marked `interrupted`, since its process died with the previous instance and its actual outcome is unknown. On Unix, if sundiald sees that the previous run's process group is still present during startup, it writes an alert for the orphaned process group but does not kill it automatically.

```sh
cargo run -- reload --config sundiald.yaml
```

Reloads `sundiald.yaml` from disk without restarting — this picks up job/schedule/log/alert changes. The config is validated before being swapped in; an invalid file is rejected and the service keeps running on its previous config. Changing `api_bind` still requires a full restart, since it means rebinding the HTTP listener.

## Inspect

```sh
cargo run -- config --config sundiald.yaml
cargo run -- run heartbeat --config sundiald.yaml
cargo run -- terminate sleepy --config sundiald.yaml
cargo run -- kill sleepy --config sundiald.yaml
cargo run -- status --config sundiald.yaml
cargo run -- status --config sundiald.yaml --watch
```

In watch mode, use arrow keys or `j`/`k` to select a job, `r` to run the selected job immediately, `T` to send SIGTERM, `K` to send SIGKILL, `R` to reload config, and `q` to quit.

Manual runs are requested through the HTTP API and executed by the long-running `serve` process, so ad-hoc jobs are still child processes of the main sundiald service.

## HTTP API

`serve` starts a local HTTP API at `api_bind`.

```sh
curl http://127.0.0.1:8787/status
curl -X POST http://127.0.0.1:8787/jobs/heartbeat/run
curl -X POST http://127.0.0.1:8787/jobs/sleepy/terminate
curl -X POST http://127.0.0.1:8787/jobs/sleepy/kill
curl -X POST http://127.0.0.1:8787/reload
```

The CLI uses this API for `status`, `status --watch`, `run`/`terminate`/`kill`, and `reload`, so the same surface can back a web UI later.

`/status` reports a `status` of `idle`, `running`, `succeeded`, `failed`, `start_failed`, or `interrupted` (was `running` when the service last restarted) per job, plus a `terminated_by_signal` field (`"SIGTERM"`/`"SIGKILL"`/`null`) when a run ended because it was signaled via `terminate`/`kill` rather than exiting on its own, and a `uuid` field with the job's stable identity.
