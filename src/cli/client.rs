use std::{sync::OnceLock, time::Duration};

use anyhow::Result;

use crate::{client_config::ClientConfig, service};

pub(crate) fn api_base(config: &ClientConfig) -> String {
    config.url.clone()
}

pub(crate) fn api_client() -> reqwest::Client {
    static CLIENT: OnceLock<reqwest::Client> = OnceLock::new();
    CLIENT
        .get_or_init(|| {
            reqwest::Client::builder()
                .redirect(reqwest::redirect::Policy::none())
                .connect_timeout(Duration::from_secs(3))
                .timeout(Duration::from_secs(10))
                .build()
                .expect("fixed CLI HTTP client settings must be valid")
        })
        .clone()
}

pub(crate) fn authorize(
    config: &ClientConfig,
    request: reqwest::RequestBuilder,
) -> reqwest::RequestBuilder {
    match &config.token {
        Some(token) => request.bearer_auth(token),
        None => request,
    }
}

pub(crate) fn encode_path_segment(value: &str) -> String {
    let mut encoded = String::new();
    for byte in value.as_bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'.' | b'_' | b'~' => {
                encoded.push(*byte as char);
            }
            byte => encoded.push_str(&format!("%{byte:02X}")),
        }
    }
    encoded
}

/// POSTs to `path` on the configured sundiald API, with a uniform connection
/// error message shared by every CLI command that talks to the API.
pub(crate) async fn post_api(config: &ClientConfig, path: &str) -> Result<reqwest::Response> {
    authorize(
        config,
        api_client().post(format!("{}{path}", api_base(config))),
    )
    .send()
    .await
    .map_err(|error| {
        anyhow::anyhow!(
            "failed to connect to sundiald api at {}: {error:#}",
            api_base(config)
        )
    })
}

pub(crate) async fn get_api(config: &ClientConfig, path: &str) -> Result<reqwest::Response> {
    authorize(
        config,
        api_client().get(format!("{}{path}", api_base(config))),
    )
    .send()
    .await
    .map_err(|error| {
        anyhow::anyhow!(
            "failed to connect to sundiald api at {}: {error:#}",
            api_base(config)
        )
    })
}

pub(crate) async fn report_response(
    response: reqwest::Response,
    action: &str,
    success_message: &str,
) -> Result<()> {
    if response.status().is_success() {
        println!("{success_message}");
        Ok(())
    } else {
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        anyhow::bail!("{action} rejected by api: HTTP {status}: {body}");
    }
}

pub(crate) async fn fetch_status(config: &ClientConfig) -> Result<service::StatusResponse> {
    authorize(
        config,
        api_client().get(format!("{}/status", api_base(config))),
    )
    .send()
    .await
    .map_err(|error| {
        anyhow::anyhow!(
            "failed to connect to sundiald api at {}: {error:#}",
            api_base(config)
        )
    })?
    .error_for_status()
    .map_err(|error| anyhow::anyhow!("sundiald api returned an error: {error}"))?
    .json()
    .await
    .map_err(|error| anyhow::anyhow!("failed to parse sundiald api status response: {error}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encode_path_segment_escapes_reserved_and_unicode_bytes() {
        assert_eq!(encode_path_segment("simple-job_1"), "simple-job_1");
        assert_eq!(encode_path_segment("a/b?c#d e"), "a%2Fb%3Fc%23d%20e");
        assert_eq!(encode_path_segment("cafe\u{301}"), "cafe%CC%81");
    }

    #[test]
    fn authorize_adds_the_configured_bearer_token() {
        let config = ClientConfig {
            token: Some("test-secret".into()),
            ..ClientConfig::default()
        };
        let request = authorize(&config, api_client().get("http://127.0.0.1/status"))
            .build()
            .unwrap();

        assert_eq!(
            request.headers()[reqwest::header::AUTHORIZATION],
            "Bearer test-secret"
        );
    }
}
