// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

#[cfg(feature = "http-pull")]
use std::time::Duration;

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

#[cfg(not(feature = "http-pull"))]
#[test]
fn http_poll_config_errors_when_feature_disabled() {
    let err = obzenflow::sources::http_poll_config().expect_err("http_poll_config should error");
    assert!(matches!(
        err,
        obzenflow_infra::http_client::HttpClientFactoryError::FeatureNotEnabled(_)
    ));
}
