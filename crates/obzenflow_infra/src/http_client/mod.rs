// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! HTTP client implementations for outbound requests.
//!
//! The core `HttpClient` trait lives in `obzenflow_core::http_client`.
//! Concrete implementations live here (in infra) behind feature flags.
//! Default source-configuration helpers bind this client into Adapter-owned builders.

use obzenflow_adapters::sources::{
    HttpPollConfig, HttpPollConfigBuilder, HttpPullConfig, HttpPullConfigBuilder,
};
use obzenflow_core::http_client::HttpClient;
use std::sync::Arc;

#[cfg(feature = "reqwest-client")]
mod reqwest_client;

#[cfg(feature = "reqwest-client")]
pub use reqwest_client::ReqwestHttpClient;

pub fn default_http_client() -> Result<Arc<dyn HttpClient>, HttpClientFactoryError> {
    #[cfg(feature = "reqwest-client")]
    {
        Ok(Arc::new(ReqwestHttpClient::new()))
    }

    #[cfg(not(feature = "reqwest-client"))]
    {
        Err(HttpClientFactoryError::FeatureNotEnabled(
            "reqwest-client".to_string(),
        ))
    }
}

/// Preload an HTTP pull configuration builder with the infra-owned default client.
pub fn http_pull_config() -> Result<HttpPullConfigBuilder, HttpClientFactoryError> {
    let client = default_http_client()?;
    Ok(HttpPullConfig::builder().client(client))
}

/// Preload an HTTP polling configuration builder with the infra-owned default client.
pub fn http_poll_config() -> Result<HttpPollConfigBuilder, HttpClientFactoryError> {
    let client = default_http_client()?;
    Ok(HttpPollConfig::builder().client(client))
}

#[derive(Debug, thiserror::Error)]
pub enum HttpClientFactoryError {
    #[error("Feature not enabled: {0}")]
    FeatureNotEnabled(String),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(feature = "reqwest-client")]
    #[test]
    fn default_helpers_preload_cold_clients() {
        http_pull_config()
            .expect("default pull client is available")
            .build()
            .expect("pull config builds");
        http_poll_config()
            .expect("default poll client is available")
            .poll_interval(std::time::Duration::from_secs(1))
            .build()
            .expect("poll config builds");
    }

    #[cfg(not(feature = "reqwest-client"))]
    #[test]
    fn default_helpers_report_the_disabled_feature() {
        assert!(matches!(
            http_pull_config(),
            Err(HttpClientFactoryError::FeatureNotEnabled(_))
        ));
        assert!(matches!(
            http_poll_config(),
            Err(HttpClientFactoryError::FeatureNotEnabled(_))
        ));
    }
}
