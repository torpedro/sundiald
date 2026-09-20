# Changelog

## Unreleased

- Discover server and client files from explicit paths, then the XDG/home user
  directory, then `/etc/sundiald`. Selected files are never merged or silently
  skipped on parse/read errors.
- Separate client connection settings from daemon configuration. Clients default
  to localhost and support `client.yaml`, URL/token overrides, token files, and
  environment references. Existing nondefault connections must migrate to client
  settings; client commands no longer accept `--config` or daemon YAML.
- Add XDG config discovery, `SUNDIALD_CONFIG` / `SUNDIALD_CLIENT_CONFIG` path
  overrides, and redacted `client-config` diagnostics.

## 0.1.1 - 2026-09-20

- Add `scripts/make_release.sh`, an interactive helper that bumps the version, updates the changelog, verifies packaging, and optionally commits, publishes, tags, and pushes the tag.

- Add Flares as an alert destination. Configure it under `alert.flares`; alerts continue to run alongside any configured `alert.command` and Pushover destinations.
- Upgrade rusqlite to 0.40.2, retaining bundled SQLite.
- Update the Rust dependency group, including axum, reqwest, tokio, and uuid.
- Enforce Clippy warnings in CI and verify packaging on every run.
- Split the README into `docs/`, and move `RELEASING.md` to `docs/releases.md`.

## 0.1.0

- Initial release: job scheduler and service runner with a terminal UI, YAML configuration, SQLite-backed run history, an HTTP API, and command and Pushover alert destinations.
