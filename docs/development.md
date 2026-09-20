# Development

```sh
cargo fmt --all --check
cargo clippy --workspace --all-targets --locked -- -D warnings
cargo test --workspace --locked
cargo publish --workspace --dry-run --locked --registry crates-io
```

CI runs exactly these checks on pull requests and pushes to `main`. Clippy warnings
fail the build. Commit `Cargo.lock` for reproducible dependency resolution.

```sh
cargo build
```

Tests use temporary config, state, and history directories; they do not write to
`~/.config/sundiald` or contact Pushover, Flares, or any other alert destination.
Coverage includes schedule and window transitions, job and service lifecycles,
missed-tick policies, config validation and reload, UUID write-back, SQLite history
across restarts, and the HTTP API.
