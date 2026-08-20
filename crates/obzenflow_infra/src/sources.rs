// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Concrete default-resource composition for source adapters.

use crate::http_client::{default_http_client, HttpClientFactoryError};
use obzenflow_adapters::sources::{
    HttpPollConfig, HttpPollConfigBuilder, HttpPullConfig, HttpPullConfigBuilder,
};

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
