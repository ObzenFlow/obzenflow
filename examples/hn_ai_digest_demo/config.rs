// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::mock_server::spawn_mock_hn_server;
use anyhow::{Context, Result};
use obzenflow::ai::TokenCount;
use obzenflow::env::env_var_or;
use obzenflow::stages::sinks::postgres::PostgresConnection;
use obzenflow::stages::sources::Url;
use serde::{Deserialize, Serialize};
use std::num::NonZeroU64;

#[path = "../support/postgres_transport.rs"]
mod postgres_transport;

pub(crate) const DEFAULT_HN_MAX_STORIES: usize = 60;
pub(crate) const DEFAULT_HN_SOURCE_RATE_LIMIT: f64 = 10.0;
const DEFAULT_HN_POLL_TIMEOUT_SECS: u64 = 120;
const DEFAULT_HN_GROUP_MAX_STORIES: usize = 10;
const HN_LIVE_BASE_URL: &str = "https://hacker-news.firebaseio.com/";
const DEFAULT_HN_DIGEST_POSTGRES_SCHEMA: &str = "obzenflow_example";

#[derive(Clone, Debug)]
pub(crate) struct HnDigestPostgresConfig {
    pub(crate) connection: PostgresConnection,
    pub(crate) schema: String,
}

impl HnDigestPostgresConfig {
    pub(crate) fn from_env() -> Result<Self> {
        let connection = PostgresConnection::deferred_from_env(
            "OBZENFLOW_POSTGRES_URL",
            postgres_transport::from_environment()?,
        );
        let schema = env_var_or::<String>(
            "OBZENFLOW_POSTGRES_SCHEMA",
            DEFAULT_HN_DIGEST_POSTGRES_SCHEMA.to_string(),
        )?;

        Ok(Self { connection, schema })
    }
}

#[derive(Clone, Copy, Debug, Default, Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
pub(crate) enum HnSource {
    #[default]
    Mock,
    Live,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct HnSettings {
    #[serde(default)]
    source: HnSource,
    #[serde(default = "default_max_stories")]
    max_stories: usize,
    #[serde(default = "default_poll_timeout_secs")]
    poll_timeout_secs: u64,
    #[serde(default)]
    interests: Option<String>,
    #[serde(default)]
    digest: DigestSettings,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
struct DigestSettings {
    group_budget_tokens: Option<NonZeroU64>,
    group_max_stories: usize,
}

impl Default for DigestSettings {
    fn default() -> Self {
        Self {
            group_budget_tokens: None,
            group_max_stories: DEFAULT_HN_GROUP_MAX_STORIES,
        }
    }
}

fn default_max_stories() -> usize {
    DEFAULT_HN_MAX_STORIES
}

fn default_poll_timeout_secs() -> u64 {
    DEFAULT_HN_POLL_TIMEOUT_SECS
}

impl HnSettings {
    pub(crate) fn into_inputs(self) -> Result<HnRunInputs> {
        let (base_url, mode_label) = match self.source {
            HnSource::Live => (
                Url::parse(HN_LIVE_BASE_URL).expect("the HN API URL is valid"),
                "live",
            ),
            HnSource::Mock => (
                spawn_mock_hn_server()
                    .context("mock HN server failed to start")?
                    .detach(),
                "mock",
            ),
        };
        Ok(HnRunInputs {
            max_stories: self.max_stories,
            poll_timeout_secs: self.poll_timeout_secs,
            source_rate_limit: DEFAULT_HN_SOURCE_RATE_LIMIT,
            budget_per_group_override: self
                .digest
                .group_budget_tokens
                .map(|tokens| TokenCount::new(tokens.get())),
            max_stories_per_group: match self.digest.group_max_stories {
                0 => None,
                stories => Some(stories),
            },
            interests: self.interests,
            mode_label: mode_label.to_string(),
            base_url,
        })
    }
}

#[derive(Clone)]
pub struct HnRunInputs {
    pub max_stories: usize,
    pub poll_timeout_secs: u64,
    pub source_rate_limit: f64,
    pub budget_per_group_override: Option<TokenCount>,
    pub max_stories_per_group: Option<usize>,
    pub interests: Option<String>,
    pub mode_label: String,
    pub base_url: Url,
}
