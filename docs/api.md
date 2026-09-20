# HTTP API

`daemon` starts an HTTP API at `api_bind`. Loopback bindings do not require authentication. A non-loopback `api_bind` is rejected unless `api_token` is configured; all endpoints except `/health` then require `Authorization: Bearer <api_token>`. The bundled CLI reads its own [client connection settings](client-configuration.md) and supplies the token as a bearer header automatically. The API serves plain HTTP, so use a trusted private network or a TLS reverse proxy when exposing it beyond the host.

```sh
curl http://127.0.0.1:8787/status
curl http://127.0.0.1:8787/jobs/heartbeat/history
curl 'http://127.0.0.1:8787/jobs/heartbeat/logs/latest?tail=40'
curl -X POST http://127.0.0.1:8787/jobs/heartbeat/run
curl -X POST http://127.0.0.1:8787/jobs/sleepy/terminate
curl -X POST http://127.0.0.1:8787/jobs/sleepy/kill
curl -X POST http://127.0.0.1:8787/services/web/start
curl -X POST http://127.0.0.1:8787/services/web/stop
curl -X POST http://127.0.0.1:8787/services/web/kill
curl http://127.0.0.1:8787/services/web/history
curl 'http://127.0.0.1:8787/services/web/logs/latest?tail=40'
curl -X POST http://127.0.0.1:8787/reload
```

The CLI uses this API for `ui`, `history`, job run/terminate/kill, service start/stop/kill, and `reload`, so the same surface can back a web UI later. Job and service route parameters accept either names or UUIDs.

`/status` reports `jobs` and `services`. Both use a `status` of `idle`, `running`, `succeeded`, `failed`, `start_failed`, or `interrupted`, plus `uuid`, `group`, `pid`, log path, exit code, and `terminated_by_signal` fields. Job rows include a `trigger` object and `next_runs` with up to 10 upcoming run times. Service rows include `schedule`, `next_start`, and `next_stop`.

`/jobs/{job}/history?limit=50` and `/services/{service}/history?limit=50` return recent SQLite run-history rows including trigger kind, start/finish times, duration, exit code, final status, error text, signal, group, and log path. `/jobs/{job}/logs/latest?tail=40` and `/services/{service}/logs/latest?tail=40` return recent stdout/stderr log content for the latest known run.
