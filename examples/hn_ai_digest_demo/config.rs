// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::mock_server::{spawn_mock_hn_server, MockHnServer};
use anyhow::{anyhow, Context, Result};
use obzenflow::ai::TokenCount;
use obzenflow::env::{env_bool_or, env_var, env_var_or};
use obzenflow::sinks::postgres::{PostgresConnection, PostgresTransport};
use obzenflow::sources::Url;

pub(crate) const DEFAULT_HN_MAX_STORIES: usize = 60;
pub(crate) const DEFAULT_HN_SOURCE_RATE_LIMIT: f64 = 10.0;
const DEFAULT_HN_DIGEST_POSTGRES_SCHEMA: &str = "obzenflow_example";

#[derive(Clone, Debug)]
pub(crate) struct HnDigestPostgresConfig {
    pub(crate) connection: PostgresConnection,
    pub(crate) schema: String,
}

impl HnDigestPostgresConfig {
    pub(crate) fn from_env() -> Result<Self> {
        let connection =
            PostgresConnection::from_env("OBZENFLOW_POSTGRES_URL", PostgresTransport::VerifiedTls)
                .context(
                    "configure the HN digest PostgreSQL connection from OBZENFLOW_POSTGRES_URL",
                )?;
        let schema = env_var_or::<String>(
            "OBZENFLOW_POSTGRES_SCHEMA",
            DEFAULT_HN_DIGEST_POSTGRES_SCHEMA.to_string(),
        )?;

        Ok(Self { connection, schema })
    }
}

#[derive(Clone)]
pub struct HnRunInputs {
    pub(crate) digest_sink_key: String,
    pub max_stories: usize,
    pub poll_timeout_secs: usize,
    pub source_rate_limit: f64,
    pub budget_per_group_override: Option<TokenCount>,
    pub max_stories_per_group: Option<usize>,
    pub interests: Option<String>,
    pub mode_label: String,
    pub base_url: Url,
}

pub struct PreparedHnRun {
    pub inputs: HnRunInputs,
    pub(crate) mock_server: Option<MockHnServer>,
}

impl PreparedHnRun {
    pub async fn from_env() -> Result<Self> {
        let digest_sink_key = env_var_or::<String>("HN_DIGEST_OUTPUT", "console".to_owned())?;
        let max_stories = env_var_or::<usize>("HN_MAX_STORIES", DEFAULT_HN_MAX_STORIES)?;
        let poll_timeout_secs = env_var_or::<usize>("HN_POLL_TIMEOUT_SECS", 120)?;
        let live = env_bool_or("HN_LIVE", false)?;
        let source_rate_limit =
            env_var_or::<f64>("HN_SOURCE_RATE_LIMIT", DEFAULT_HN_SOURCE_RATE_LIMIT)?;
        if source_rate_limit <= 0.0 {
            return Err(anyhow!("HN_SOURCE_RATE_LIMIT must be greater than zero"));
        }

        let mut mock_server = None;
        let (base_url, mode_label) = if live {
            (
                Url::parse("https://hacker-news.firebaseio.com/")
                    .map_err(|error| anyhow!("invalid HN base URL: {error}"))?,
                "live".to_string(),
            )
        } else {
            let server = spawn_mock_hn_server().await?;
            let url = server.base_url();
            mock_server = Some(server);
            (url, "mock".to_string())
        };

        let budget_per_group_override = env_var::<usize>("HN_AI_GROUP_BUDGET_TOKENS")?
            .map(|tokens| {
                if tokens == 0 {
                    Err(anyhow!(
                        "HN_AI_GROUP_BUDGET_TOKENS must be greater than zero"
                    ))
                } else {
                    Ok(TokenCount::new(tokens as u64))
                }
            })
            .transpose()?;

        let max_stories_per_group = match env_var_or::<usize>("HN_AI_GROUP_MAX_STORIES", 10)? {
            0 => None,
            value => Some(value),
        };

        let interests = env_var::<String>("HN_AI_INTERESTS")?;

        Ok(Self {
            inputs: HnRunInputs {
                digest_sink_key,
                max_stories,
                poll_timeout_secs,
                source_rate_limit,
                budget_per_group_override,
                max_stories_per_group,
                interests,
                mode_label,
                base_url,
            },
            mock_server,
        })
    }
}

impl HnRunInputs {
    pub fn group_max_stories_label(&self) -> String {
        match self.max_stories_per_group {
            None => "unlimited".to_string(),
            Some(value) => value.to_string(),
        }
    }
}
