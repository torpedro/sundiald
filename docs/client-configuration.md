# Client connections

The daemon owns schedules, jobs, services, storage, and alert settings. CLI and
terminal UI commands need only its URL and, when configured, an API token.

Without a client file or overrides, client commands connect to
`http://127.0.0.1:8787` without a token:

```sh
sundiald ui
sundiald run heartbeat
sundiald reload
```

They do not read the daemon's `config.yaml`, load its job files, or validate its
schedules. A broken server configuration therefore does not prevent inspecting
or controlling the running daemon. `reload` asks the daemon to reread its own
original file; it does not upload the client's file.

## Client file

The first available configuration is selected in this order:

1. `--client-config`, or `SUNDIALD_CLIENT_CONFIG` if no flag was supplied.
2. `$XDG_CONFIG_HOME/sundiald/client.yaml` when `XDG_CONFIG_HOME` is an absolute,
   nonempty path; otherwise `$HOME/.config/sundiald/client.yaml`.
3. `/etc/sundiald/client.yaml`.

The selected file is used alone; files are not merged. A missing user file falls
back to the system file. An explicitly selected file must exist, and an unreadable
or invalid selected file is an error, not a reason to fall back. If neither user
nor system file exists, clients use the default local connection. XDG replaces
the home config directory rather than adding another search location.

```yaml
url: https://scheduler.example.com/sundiald
token_file: api-token
```

The token file contains just the API token; a trailing newline is accepted.
Alternatively, use exactly one of `token: "..."` or
`token_env: MY_SUNDIALD_TOKEN`. The named environment variable must be set and
nonempty. Unknown client configuration fields are rejected.

Generate a template or inspect the resolved settings:

```sh
sundiald sample-client-config
sundiald client-config
sundiald client-config --client-config ./production.yaml
```

`client-config` prints the file used, resolved URL, and whether a token is
configured, including which configuration layer supplied each setting. It does
not print the token or contact the server.

Client file paths and token file paths support `~/`. A relative `token_file`
inside YAML is relative to that YAML file. Paths supplied through flags or
environment variables are relative to the client's working directory. Other
shell substitutions such as `$HOME` inside YAML are not expanded.

## Overrides and precedence

Connection settings use command-line flags, then environment variables, then the
selected file, then defaults. URL and authentication are resolved independently.
Changing the URL alone retains the configured authentication.

| Setting | Flag | Environment variable |
| --- | --- | --- |
| Client file | `--client-config` | `SUNDIALD_CLIENT_CONFIG` |
| API URL | `--url` | `SUNDIALD_URL` |
| Inline token | `--token` | `SUNDIALD_TOKEN` |
| Token file | `--token-file` | `SUNDIALD_TOKEN_FILE` |
| Name of token environment variable | `--token-env` | `SUNDIALD_TOKEN_ENV` |

Choose one authentication source per layer. A higher-priority source replaces
the lower-priority source completely: for example, `SUNDIALD_TOKEN` overrides a
file's `token_file` without reading that token file. Missing tokens, unreadable
selected token files, and invalid values are errors rather than silent fallback.
The selected YAML must still parse even when flags override its values.

Examples:

```sh
sundiald ui --url https://scheduler.example.com --token-file ./api-token
sundiald history heartbeat --client-config ./production.yaml
SUNDIALD_URL=http://127.0.0.1:9000 sundiald reload
```

HTTP and HTTPS URLs may contain a path prefix. Do not put credentials, a query,
or a fragment in the URL. The client does not follow redirects. Use the actual
server address, not the daemon's wildcard listening address.

## Migrating existing commands

**Client commands no longer automatically read the daemon's `config.yaml`.** If
you used a nondefault port or authentication there, create a `client.yaml` or
supply connection overrides. Clients do not need the daemon's alert credentials,
job definitions, or access to its files.

Client commands do not accept `--config` or daemon YAML. Use `--client-config`
for a client file, or `--url` and token overrides. Server-only fields such as
`api_bind`, `api_token`, and `jobs` are rejected in client files.

The `daemon` and `config` commands continue to use server YAML, selected by
`--config`, then `SUNDIALD_CONFIG`, then the user config directory's `config.yaml`,
then `/etc/sundiald/config.yaml`. A server file is required.
`SUNDIALD_CONFIG` never selects a client connection file. Existing server path
resolution, UUID write-back, and reload restrictions are unchanged.
