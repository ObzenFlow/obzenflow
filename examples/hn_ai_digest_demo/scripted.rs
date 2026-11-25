// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::domain::{HnStory, HnStoryId};
use async_trait::async_trait;
use obzenflow::queries::{QueryContext, QueryError, SourceDiagnosticReason, SourcePollErrorKind};
use obzenflow::stages::sources::ListDetail;
use serde_json::Value;

const LISTED_STORIES: usize = 64;
const DELETED_EVERY_NTH: usize = 11;
const TOPICS: [&str; 10] = [
    "Rust",
    "AI",
    "Databases",
    "Security",
    "Browsers",
    "Linux",
    "Compilers",
    "Startups",
    "Robotics",
    "Distributed Systems",
];
const VERBS: [&str; 8] = [
    "tooling update",
    "benchmark",
    "postmortem",
    "deep dive",
    "launch",
    "migration guide",
    "research note",
    "case study",
];

fn scripted_story(offset: usize) -> HnStory {
    let id = 100 + offset as u64;
    let topic = TOPICS[offset % TOPICS.len()];
    let verb = VERBS[offset % VERBS.len()];
    HnStory {
        id: HnStoryId(id),
        item_type: Some("story".to_string()),
        by: Some(format!("author{}", (offset % 17) + 1)),
        time: Some(1_774_600_000 + offset as u64),
        title: Some(format!("{topic}: {verb}")),
        url: Some(format!("https://example.com/hn/{id}")),
        score: Some(180_u32.saturating_sub(offset as u32 * 2)),
        descendants: Some((offset % 29) as u32),
    }
}

#[obzenflow::system("scripted-stories")]
pub struct ScriptedStories;

#[async_trait]
impl ListDetail for ScriptedStories {
    async fn list(&self, _ctx: &QueryContext) -> Result<Vec<String>, QueryError> {
        Ok((0..LISTED_STORIES)
            .map(|offset| (100 + offset).to_string())
            .collect())
    }

    async fn detail(&self, id: &str, _ctx: &QueryContext) -> Result<Option<Value>, QueryError> {
        let offset = id
            .parse::<usize>()
            .ok()
            .and_then(|id| id.checked_sub(100))
            .filter(|offset| *offset < LISTED_STORIES)
            .ok_or_else(|| {
                QueryError::terminal(
                    SourcePollErrorKind::Validation,
                    SourceDiagnosticReason::InvalidValue,
                )
            })?;
        if (offset + 1) % DELETED_EVERY_NTH == 0 {
            return Ok(None);
        }
        serde_json::to_value(scripted_story(offset))
            .map(Some)
            .map_err(|_| {
                QueryError::terminal(
                    SourcePollErrorKind::Other,
                    SourceDiagnosticReason::UnrepresentableValue,
                )
            })
    }
}

#[cfg(test)]
pub fn scripted_stories(max_stories: usize) -> Vec<HnStory> {
    (0..LISTED_STORIES.min(max_stories))
        .filter(|offset| (offset + 1) % DELETED_EVERY_NTH != 0)
        .map(scripted_story)
        .collect()
}
