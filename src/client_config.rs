use std::{
    fs,
    path::{Path, PathBuf},
};

use anyhow::{Context, Result, bail};
use clap::Args;
use serde::Deserialize;

use crate::config_path::{Environment, discover, environment, expand_home};

const DEFAULT_URL: &str = "http://127.0.0.1:8787";

#[derive(Args, Default)]
pub struct ClientOptions {
    /// Client YAML file (otherwise SUNDIALD_CLIENT_CONFIG, then user and /etc/sundiald discovery).
    #[arg(long)]
    pub client_config: Option<PathBuf>,
    /// Server HTTP(S) URL; overrides SUNDIALD_URL and the client file.
    #[arg(long)]
    pub url: Option<String>,
    /// API token; prefer --token-file or SUNDIALD_TOKEN to avoid shell history.
    #[arg(long, conflicts_with_all = ["token_file", "token_env"])]
    pub token: Option<String>,
    /// File containing the API token.
    #[arg(long, conflicts_with_all = ["token", "token_env"])]
    pub token_file: Option<PathBuf>,
    /// Name of an environment variable containing the API token.
    #[arg(long, conflicts_with_all = ["token", "token_file"])]
    pub token_env: Option<String>,
}

#[derive(Clone)]
pub struct ClientConfig {
    pub url: String,
    pub token: Option<String>,
    pub source: Option<PathBuf>,
    pub url_source: &'static str,
    pub token_source: &'static str,
}

impl Default for ClientConfig {
    fn default() -> Self {
        Self {
            url: DEFAULT_URL.into(),
            token: None,
            source: None,
            url_source: "default",
            token_source: "none",
        }
    }
}

#[derive(Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct ClientFile {
    url: Option<String>,
    token: Option<String>,
    token_file: Option<PathBuf>,
    token_env: Option<String>,
}

fn env_text(environment: &Environment, name: &str) -> Result<Option<String>> {
    environment
        .get(name)
        .map(|v| {
            v.to_str()
                .map(str::to_owned)
                .with_context(|| format!("{name} must contain valid UTF-8"))
        })
        .transpose()
}

impl ClientOptions {
    pub fn load(&self) -> Result<ClientConfig> {
        self.load_with(&environment(), Path::new("/etc/sundiald"))
    }

    fn load_with(
        &self,
        environment: &Environment,
        system_directory: &Path,
    ) -> Result<ClientConfig> {
        let explicit = self
            .client_config
            .clone()
            .or_else(|| environment.get("SUNDIALD_CLIENT_CONFIG").map(PathBuf::from));
        let source = discover(explicit, environment, "client.yaml", system_directory)?;
        let mut loaded_source = None;
        let mut file = ClientFile::default();
        if let Some(path) = &source {
            match fs::read_to_string(path) {
                Ok(raw) => {
                    file = serde_yaml::from_str(&raw).with_context(|| {
                        format!("failed to parse client config {}", path.display())
                    })?;
                    loaded_source = Some(path.clone());
                }
                Err(error) => {
                    return Err(error).with_context(|| {
                        format!("failed to read client config {}", path.display())
                    });
                }
            }
        }

        let (url, url_source) = if let Some(url) = &self.url {
            (url.clone(), "command line")
        } else if let Some(url) = env_text(environment, "SUNDIALD_URL")? {
            (url, "SUNDIALD_URL")
        } else if let Some(url) = file.url {
            (url, "config file")
        } else {
            (DEFAULT_URL.into(), "default")
        };
        let url = reqwest::Url::parse(&url).map_err(|_| anyhow::anyhow!("invalid client URL"))?;
        if !matches!(url.scheme(), "http" | "https")
            || url.host_str().is_none()
            || !url.username().is_empty()
            || url.password().is_some()
            || url.query().is_some()
            || url.fragment().is_some()
        {
            bail!("client URL must be HTTP(S) without credentials, query, or fragment");
        }

        let cli_auth =
            self.token.is_some() || self.token_file.is_some() || self.token_env.is_some();
        let env_auth = [
            "SUNDIALD_TOKEN",
            "SUNDIALD_TOKEN_FILE",
            "SUNDIALD_TOKEN_ENV",
        ]
        .iter()
        .any(|key| environment.contains_key(*key));
        let (token, token_file, token_env, token_source, base) = if cli_auth {
            (
                self.token.clone(),
                self.token_file.clone(),
                self.token_env.clone(),
                "command line",
                PathBuf::from("."),
            )
        } else if env_auth {
            (
                env_text(environment, "SUNDIALD_TOKEN")?,
                environment.get("SUNDIALD_TOKEN_FILE").map(PathBuf::from),
                env_text(environment, "SUNDIALD_TOKEN_ENV")?,
                "environment",
                PathBuf::from("."),
            )
        } else {
            let base = loaded_source
                .as_deref()
                .and_then(Path::parent)
                .unwrap_or_else(|| Path::new("."))
                .to_path_buf();
            (
                file.token,
                file.token_file,
                file.token_env,
                "config file",
                base,
            )
        };
        if [token.is_some(), token_file.is_some(), token_env.is_some()]
            .into_iter()
            .filter(|v| *v)
            .count()
            > 1
        {
            bail!("choose only one of token, token_file, or token_env in {token_source}");
        }
        let token = if let Some(path) = token_file {
            let path = expand_home(&path, environment)?;
            let path = if path.is_absolute() {
                path
            } else {
                base.join(path)
            };
            Some(
                fs::read_to_string(&path)
                    .with_context(|| format!("failed to read token file {}", path.display()))?
                    .trim_end_matches(['\r', '\n'])
                    .to_string(),
            )
        } else if let Some(name) = token_env {
            Some(
                env_text(environment, &name)?
                    .with_context(|| format!("token environment variable {name} is not set"))?,
            )
        } else {
            token
        };
        if let Some(token) = &token
            && (token.is_empty() || !token.bytes().all(|b| b.is_ascii_graphic()))
        {
            bail!("API token must be nonempty printable ASCII without spaces");
        }
        let token_source = if token.is_some() {
            token_source
        } else {
            "none"
        };
        Ok(ClientConfig {
            url: url.as_str().trim_end_matches('/').into(),
            token,
            source: loaded_source,
            url_source,
            token_source,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config_path::user_directory;
    use std::collections::HashMap;

    fn environment(temp: &tempfile::TempDir) -> Environment {
        HashMap::from([("HOME".into(), temp.path().as_os_str().to_owned())])
    }

    #[test]
    fn system_fallback_loads_relative_tokens_but_does_not_hide_user_errors() {
        let temp = tempfile::tempdir().unwrap();
        let env = environment(&temp);
        let system = temp.path().join("system");
        fs::create_dir(&system).unwrap();
        fs::write(
            system.join("client.yaml"),
            "url: https://system.example\ntoken_file: token\n",
        )
        .unwrap();
        fs::write(system.join("token"), "system-token\n").unwrap();
        let config = ClientOptions::default().load_with(&env, &system).unwrap();
        assert_eq!(config.url, "https://system.example");
        assert_eq!(config.token.as_deref(), Some("system-token"));
        assert_eq!(config.source, Some(system.join("client.yaml")));
        let user = user_directory(&env).unwrap();
        fs::create_dir_all(&user).unwrap();
        fs::write(user.join("client.yaml"), "invalid: [").unwrap();
        assert!(ClientOptions::default().load_with(&env, &system).is_err());
        fs::write(user.join("client.yaml"), "{}\n").unwrap();
        let config = ClientOptions::default().load_with(&env, &system).unwrap();
        assert_eq!(config.url, DEFAULT_URL);
        assert!(config.token.is_none());
        let options = ClientOptions {
            client_config: Some(temp.path().join("absent")),
            ..ClientOptions::default()
        };
        assert!(options.load_with(&env, &system).is_err());
        let options = ClientOptions {
            client_config: Some(system.clone()),
            ..ClientOptions::default()
        };
        assert!(options.load_with(&env, &system).is_err());
    }

    #[test]
    fn defaults_do_not_read_server_configuration() {
        let temp = tempfile::tempdir().unwrap();
        let directory = temp.path().join(".config/sundiald");
        fs::create_dir_all(&directory).unwrap();
        fs::write(directory.join("config.yaml"), "this is not valid: [ YAML").unwrap();
        let config = ClientOptions::default()
            .load_with(&environment(&temp), &temp.path().join("system"))
            .unwrap();
        assert_eq!(config.url, DEFAULT_URL);
        assert!(config.token.is_none());
        assert!(config.source.is_none());
        assert!(
            ClientOptions::default()
                .load_with(&HashMap::new(), &temp.path().join("system"))
                .is_ok()
        );
    }

    #[test]
    fn discovery_respects_xdg_and_explicit_files() {
        let temp = tempfile::tempdir().unwrap();
        let mut env = environment(&temp);
        env.insert(
            "XDG_CONFIG_HOME".into(),
            temp.path().join("xdg").into_os_string(),
        );
        let directory = user_directory(&env).unwrap();
        fs::create_dir_all(&directory).unwrap();
        fs::write(directory.join("client.yaml"), "url: https://xdg.example\n").unwrap();
        assert_eq!(
            ClientOptions::default()
                .load_with(&env, &temp.path().join("system"))
                .unwrap()
                .url,
            "https://xdg.example"
        );
        let alternate = temp.path().join("alternate.yaml");
        fs::write(&alternate, "url: https://alternate.example\n").unwrap();
        env.insert(
            "SUNDIALD_CLIENT_CONFIG".into(),
            alternate.clone().into_os_string(),
        );
        assert_eq!(
            ClientOptions::default()
                .load_with(&env, &temp.path().join("system"))
                .unwrap()
                .source,
            Some(alternate)
        );
        let options = ClientOptions {
            client_config: Some(directory.join("client.yaml")),
            ..ClientOptions::default()
        };
        assert_eq!(
            options
                .load_with(&env, &temp.path().join("system"))
                .unwrap()
                .url,
            "https://xdg.example"
        );
        env.insert("XDG_CONFIG_HOME".into(), "relative-path".into());
        assert_eq!(
            user_directory(&env).unwrap(),
            temp.path().join(".config/sundiald")
        );
    }

    #[test]
    fn flags_override_environment_which_overrides_file() {
        let temp = tempfile::tempdir().unwrap();
        let file = temp.path().join("client.yaml");
        fs::write(
            &file,
            "url: https://file.example\ntoken_file: missing-token\n",
        )
        .unwrap();
        let mut env = environment(&temp);
        env.insert("SUNDIALD_URL".into(), "https://env.example/base/".into());
        env.insert("SUNDIALD_TOKEN".into(), "env-token".into());
        let mut options = ClientOptions {
            client_config: Some(file),
            ..ClientOptions::default()
        };
        let config = options
            .load_with(&env, &temp.path().join("system"))
            .unwrap();
        assert_eq!(config.url, "https://env.example/base");
        assert_eq!(config.token.as_deref(), Some("env-token"));
        options.url = Some("https://cli.example".into());
        options.token = Some("cli-token".into());
        let config = options
            .load_with(&env, &temp.path().join("system"))
            .unwrap();
        assert_eq!(config.url, "https://cli.example");
        assert_eq!(config.token.as_deref(), Some("cli-token"));
        env.remove("SUNDIALD_TOKEN");
        options.token = None;
        assert!(
            options
                .load_with(&env, &temp.path().join("system"))
                .is_err()
        );
    }

    #[test]
    fn token_files_are_relative_to_client_file_and_support_home() {
        let temp = tempfile::tempdir().unwrap();
        let directory = temp.path().join("settings");
        fs::create_dir(&directory).unwrap();
        fs::write(directory.join("api-token"), "file-token\r\n").unwrap();
        let file = directory.join("client.yaml");
        fs::write(&file, "token_file: api-token\n").unwrap();
        let options = ClientOptions {
            client_config: Some(file.clone()),
            ..ClientOptions::default()
        };
        assert_eq!(
            options
                .load_with(&environment(&temp), &temp.path().join("system"))
                .unwrap()
                .token
                .as_deref(),
            Some("file-token")
        );
        fs::write(&file, "token_file: ~/settings/api-token\n").unwrap();
        assert_eq!(
            options
                .load_with(&environment(&temp), &temp.path().join("system"))
                .unwrap()
                .token
                .as_deref(),
            Some("file-token")
        );
        fs::write(&file, "token_env: MY_TOKEN\n").unwrap();
        let mut env = environment(&temp);
        assert!(
            options
                .load_with(&env, &temp.path().join("system"))
                .is_err()
        );
        env.insert("MY_TOKEN".into(), "env-secret".into());
        assert_eq!(
            options
                .load_with(&env, &temp.path().join("system"))
                .unwrap()
                .token
                .as_deref(),
            Some("env-secret")
        );
    }

    #[test]
    fn explicit_missing_and_invalid_client_files_fail() {
        let temp = tempfile::tempdir().unwrap();
        let file = temp.path().join("client.yaml");
        let options = ClientOptions {
            client_config: Some(file.clone()),
            ..ClientOptions::default()
        };
        assert!(
            options
                .load_with(&environment(&temp), &temp.path().join("system"))
                .is_err()
        );
        for raw in [
            "url: [",
            "urll: http://localhost",
            "token: secret\ntoken_file: other",
        ] {
            fs::write(&file, raw).unwrap();
            assert!(
                options
                    .load_with(&environment(&temp), &temp.path().join("system"))
                    .is_err()
            );
        }
        let env = HashMap::from([(
            "SUNDIALD_CLIENT_CONFIG".into(),
            temp.path().join("absent").into_os_string(),
        )]);
        assert!(
            ClientOptions::default()
                .load_with(&env, &temp.path().join("system"))
                .is_err()
        );
    }

    #[test]
    fn invalid_urls_and_tokens_are_rejected_without_echoing_secrets() {
        let temp = tempfile::tempdir().unwrap();
        for url in [
            "ftp://example.com",
            "https://user:secret@example.com",
            "https://example.com?token=secret",
            "https://example.com/#secret",
            "not a URL",
        ] {
            let options = ClientOptions {
                url: Some(url.into()),
                ..ClientOptions::default()
            };
            let error = options
                .load_with(&HashMap::new(), &temp.path().join("system"))
                .err()
                .unwrap();
            assert!(!format!("{error:#}").contains("secret"));
        }
        for token in ["", "secret with spaces", "secret\nheader", "秘密"] {
            let options = ClientOptions {
                token: Some(token.into()),
                ..ClientOptions::default()
            };
            assert!(
                options
                    .load_with(&HashMap::new(), &temp.path().join("system"))
                    .is_err()
            );
        }
        let env = HashMap::from([
            ("SUNDIALD_TOKEN".into(), "secret".into()),
            ("SUNDIALD_TOKEN_FILE".into(), "file".into()),
        ]);
        assert!(
            ClientOptions::default()
                .load_with(&env, &temp.path().join("system"))
                .is_err()
        );
    }

    #[test]
    fn server_fields_are_rejected_in_client_files() {
        let temp = tempfile::tempdir().unwrap();
        let file = temp.path().join("server.yaml");
        fs::write(&file, "api_bind: 127.0.0.1:8787\napi_token: secret\n").unwrap();
        let options = ClientOptions {
            client_config: Some(file),
            ..ClientOptions::default()
        };
        assert!(
            options
                .load_with(&environment(&temp), &temp.path().join("system"))
                .is_err()
        );
    }
}
