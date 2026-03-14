// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::mock_server::{spawn_mock_hn_server, MockHnServer};
use super::util::{env_bool, env_usize};
use anyhow::{anyhow, Result};
use obzenflow::ai::{ModelConfig, TokenCount};
use obzenflow::sources::Url;

const DEFAULT_HN_SOURCE_RATE_LIMIT: f64 = 10.0;

fn env_f64(key: &str) -> Option<f64> {
    std::env::var(key).ok().and_then(|value| value.parse().ok())
}

pub struct DemoConfig {
    pub max_stories: usize,
    pub poll_timeout_secs: usize,
    pub source_rate_limit: f64,
    pub ai: ModelConfig,
    pub budget_per_group_tokens: u64,
    pub budget_per_group: TokenCount,
    pub max_stories_per_group: Option<usize>,
    pub interests: Option<String>,
    pub mode_label: String,
    pub base_url: Url,
    pub(crate) mock_server: Option<MockHnServer>,
}

impl DemoConfig {
    pub async fn from_env() -> Result<Self> {
        let max_stories = env_usize("HN_MAX_STORIES").unwrap_or(30);
        let poll_timeout_secs = env_usize("HN_POLL_TIMEOUT_SECS").unwrap_or(120);
        let live = env_bool("HN_LIVE").unwrap_or(false);
        let source_rate_limit =
            env_f64("HN_SOURCE_RATE_LIMIT").unwrap_or(DEFAULT_HN_SOURCE_RATE_LIMIT);
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

        let ai = ModelConfig::from_env_with_prefix("HN_AI_")?;

        let budget_per_group_tokens =
            env_usize("HN_AI_GROUP_BUDGET_TOKENS").unwrap_or(match ai.provider_label() {
                "ollama" => 2500,
                _ => 6000,
            }) as u64;
        if budget_per_group_tokens == 0 {
            return Err(anyhow!(
                "HN_AI_GROUP_BUDGET_TOKENS must be greater than zero"
            ));
        }
        let budget_per_group = TokenCount::new(budget_per_group_tokens);

        let max_stories_per_group = match env_usize("HN_AI_GROUP_MAX_STORIES").unwrap_or(10) {
            0 => None,
            value => Some(value),
        };

        let interests = std::env::var("HN_AI_INTERESTS").ok();

        Ok(Self {
            max_stories,
            poll_timeout_secs,
            source_rate_limit,
            ai,
            budget_per_group_tokens,
            budget_per_group,
            max_stories_per_group,
            interests,
            mode_label,
            base_url,
            mock_server,
        })
    }

    pub fn group_max_stories_label(&self) -> String {
        match self.max_stories_per_group {
            None => "unlimited".to_string(),
            Some(value) => value.to_string(),
        }
    }
}
