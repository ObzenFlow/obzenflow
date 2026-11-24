// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::decoder::{item, selection, StoriesPort, StoryPosition, StoryReply};
use super::domain::{HnStory, HnStoryId};
use async_trait::async_trait;
use obzenflow::queries::{QueryContext, QueryError};
use obzenflow::stages::sources::{FiniteRead, SourceDiagnosticReason, SourcePollErrorKind};

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

pub struct ScriptedStories;

#[obzenflow::system("scripted-stories")]
#[async_trait]
impl StoriesPort for ScriptedStories {
    async fn read(
        &self,
        from: &StoryPosition,
        _ctx: &QueryContext,
    ) -> Result<StoryReply, QueryError> {
        match from {
            StoryPosition::Start { limit } => Ok(selection(
                (0..LISTED_STORIES.min(*limit))
                    .map(|i| HnStoryId(100 + i as u64))
                    .collect(),
            )),
            StoryPosition::Selected { ids, next_index } => match ids.get(*next_index) {
                Some(id) => {
                    let offset =
                        id.0.checked_sub(100)
                            .and_then(|offset| usize::try_from(offset).ok())
                            .filter(|offset| *offset < LISTED_STORIES)
                            .ok_or_else(|| {
                                QueryError::terminal(
                                    SourcePollErrorKind::Validation,
                                    SourceDiagnosticReason::InvalidValue,
                                )
                            })?;
                    let record = if (offset + 1) % DELETED_EVERY_NTH == 0 {
                        None
                    } else {
                        Some(serde_json::to_value(scripted_story(offset)).map_err(|_| {
                            QueryError::terminal(
                                SourcePollErrorKind::Other,
                                SourceDiagnosticReason::UnrepresentableValue,
                            )
                        })?)
                    };
                    Ok(item(ids, *next_index, record))
                }
                None => Ok(FiniteRead::EndOfInput),
            },
        }
    }
}

#[cfg(test)]
pub fn scripted_stories(max_stories: usize) -> Vec<HnStory> {
    (0..LISTED_STORIES.min(max_stories))
        .filter(|offset| (offset + 1) % DELETED_EVERY_NTH != 0)
        .map(scripted_story)
        .collect()
}
