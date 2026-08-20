// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

#[cfg(feature = "http-pull")]
use std::time::Duration;

#[cfg(feature = "http-pull")]
use obzenflow_core::http_client::{HttpClient, MockHttpClient};

#[cfg(feature = "http-pull")]
use std::sync::Arc;

#[cfg(feature = "http-pull")]
#[test]
fn http_pull_config_preloads_default_client_when_feature_enabled() {
    let _config = obzenflow::sources::http_pull_config()
        .expect("http_pull_config ok")
        .build()
        .expect("build ok");
}

#[cfg(not(feature = "http-pull"))]
#[test]
fn http_pull_config_errors_when_feature_disabled() {
    let err = obzenflow::sources::http_pull_config().expect_err("http_pull_config should error");
    assert!(matches!(
        err,
        obzenflow_infra::http_client::HttpClientFactoryError::FeatureNotEnabled(_)
    ));
}

#[cfg(feature = "http-pull")]
#[test]
fn http_poll_config_preloads_default_client_when_feature_enabled() {
    let _config = obzenflow::sources::http_poll_config()
        .expect("http_poll_config ok")
        .poll_interval(Duration::from_secs(1))
        .build()
        .expect("build ok");
}

#[cfg(feature = "http-pull")]
#[test]
fn facade_helpers_preserve_custom_client_overrides() {
    let pull_client: Arc<dyn HttpClient> = Arc::new(MockHttpClient::new());
    let pull = obzenflow::sources::http_pull_config()
        .expect("http_pull_config ok")
        .client(pull_client.clone())
        .build()
        .expect("pull config builds");
    assert!(Arc::ptr_eq(&pull.client, &pull_client));

    let poll_client: Arc<dyn HttpClient> = Arc::new(MockHttpClient::new());
    let poll = obzenflow::sources::http_poll_config()
        .expect("http_poll_config ok")
        .client(poll_client.clone())
        .poll_interval(Duration::from_secs(1))
        .build()
        .expect("poll config builds");
    assert!(Arc::ptr_eq(&poll.client, &poll_client));
}

#[cfg(not(feature = "http-pull"))]
#[test]
fn http_poll_config_errors_when_feature_disabled() {
    let err = obzenflow::sources::http_poll_config().expect_err("http_poll_config should error");
    assert!(matches!(
        err,
        obzenflow_infra::http_client::HttpClientFactoryError::FeatureNotEnabled(_)
    ));
}
