# Running under systemd

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
sundiald ui
sundiald reload
sundiald run heartbeat
```

These commands connect to the default local API without reading the system
configuration. For another address or authentication, supply a separate
[client configuration](client-configuration.md).

View service logs with `journalctl -u sundiald -f`. Per-job stdout and stderr logs are written under per-job directories in the configured `log_dir`, and alert events are written under `alert.event_dir`.
