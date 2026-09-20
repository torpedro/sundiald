# sundiald
# sundiald

`sundiald` is a small Rust job and service runner. It reads a YAML config, starts shell commands on schedules or by request, captures stdout/stderr to disk, records state for inspection, logs lifecycle events to stdout and its own log file, and writes alerts for failed jobs or unexpected service exits.

## Install

Install from crates.io with Rust and Cargo:

```sh
cargo install sundiald --locked
```

For the commands below, replace `cargo run --` with `sundiald` when using the installed binary.

## Run the service

```sh
cargo run -- daemon
```

When `--config` is not supplied, sundiald reads `~/.config/sundiald/config.yaml`. Pass `--config <path>` to use a different config file.

Job state (status, last run, exit code) persists atomically in `state_dir/state.json` and is reloaded on startup, so `status` reflects history across restarts. A job that was `running` when the service last stopped is marked `interrupted`, since its process died with the previous instance and its actual outcome is unknown. On Unix, if sundiald sees that the previous run's process group is still present during startup, it writes an alert for the orphaned process group but does not kill it automatically.

Run history is recorded in a WAL-mode SQLite database at `state_dir/history.sqlite3`. Each job or service process inserts a row when triggered, including the trigger time and trigger kind; when the process finishes, that row is updated with finish time, duration in milliseconds, and exit code. A process that fails to start still gets a history row with a finish time and duration, but no exit code. On daemon startup, unfinished rows left by a previous process are finalized with an `interrupted` status.

```sh
cargo run -- reload
```

Reloads the config from disk without restarting — this picks up job/schedule/log/alert changes. The config and its output destinations are validated before being swapped in; an invalid or unusable configuration is rejected and the service keeps running on its previous config. Changing `api_bind`, `api_token`, or `state_dir` requires a full restart because the listener, authentication middleware, and history database are initialized at daemon startup.

When the daemon shuts down, it sends SIGTERM to running jobs and services, waits up to `shutdown_grace_period` for jobs and up to each service's `stop_grace_period` when set, then escalates any remaining process groups to SIGKILL. This lets stdout/stderr logs, state, and SQLite history finish cleanly before the daemon exits.

## Use the CLI

```sh
cargo run -- config
cargo run -- run heartbeat
cargo run -- terminate sleepy
cargo run -- kill sleepy
cargo run -- start-service web
cargo run -- stop-service web
cargo run -- kill-service web
cargo run -- history heartbeat
cargo run -- ui
cargo run -- ui --once
```

The `ui` command opens the interactive view by default, grouping jobs and services from named job files under their configured group names. Use `ui --once` to print one status frame and exit. Selection is preserved by UUID across refreshes and reloads, and network requests run without blocking keyboard input or status updates.

Use arrows or `j`/`k` to select an entry, with `Home`, `End`, `PgUp`, and `PgDn` for larger lists. `/` searches names, groups, and triggers; `f` cycles through all, running, failed, and unexpected views; `g` collapses the selected group and `G` expands all groups. `Enter`, `h`, `s`, and `i` select log, history, schedule, and summary details. `Tab` moves focus between the table and details, left/right changes detail tabs, and `F` follows the selected running log. Use `r` to run a job or start a service, `T` to send SIGTERM or stop a service, `K` to request a confirmed SIGKILL, `R` to reload config, `x` to dismiss the current notice or error, `Backspace` to return to summary, `?` for keyboard help, and `q` to quit.

Job-control and history commands accept either a job name or a job UUID. UUIDs are stable across renames and are what the interactive UI uses internally.

Manual job runs and service controls are requested through the HTTP API and executed by the long-running `daemon` process, so ad-hoc processes are still child processes of the main sundiald service.

## HTTP API and further documentation

The HTTP API uses JSON and bearer authentication when `api_bind` is not a loopback
address.

- [Configuration](docs/configuration.md) — the YAML format, job files, and environment.
- [HTTP API](docs/api.md) — endpoints and curl examples.
- [Running under systemd](docs/systemd.md) — service user, unit file, and log handling.
- [Development](docs/development.md) — building and running tests.
- [Manual releases](docs/releases.md) and [changelog](CHANGELOG.md).
