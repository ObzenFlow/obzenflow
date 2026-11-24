// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::domain::{HnStory, HnStoryId};
use async_trait::async_trait;
use obzenflow::queries::{QueryContext, QueryError, QueryPort};
use obzenflow::stages::sources::{
    FiniteRead, HttpApi, SourceDiagnosticReason, SourceError, SourceRead,
    TypedAsyncFiniteSourceHandler,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::time::Duration;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum StoryPosition {
    Start {
        limit: usize,
    },
    Selected {
        ids: Vec<HnStoryId>,
        next_index: usize,
    },
}

pub type StoryReply = FiniteRead<Value, StoryPosition>;

#[obzenflow::port]
#[async_trait]
pub trait StoriesPort {
    #[query(type = "hn.stories.read")]
    async fn read(
        &self,
        from: &StoryPosition,
        ctx: &QueryContext,
    ) -> Result<StoryReply, QueryError>;
}

pub fn selection(ids: Vec<HnStoryId>) -> StoryReply {
    FiniteRead::Batch {
        records: Vec::new(),
        next: StoryPosition::Selected { ids, next_index: 0 },
    }
}

pub fn item(ids: &[HnStoryId], index: usize, record: Option<Value>) -> StoryReply {
    FiniteRead::Batch {
        records: record.into_iter().collect(),
        next: StoryPosition::Selected {
            ids: ids.to_vec(),
            next_index: index + 1,
        },
    }
}

pub fn decode_item(record: Value) -> Result<HnStory, SourceError> {
    serde_json::from_value(record)
        .map_err(|_| SourceError::Deserialization(SourceDiagnosticReason::MalformedInput.into()))
}

#[async_trait]
impl StoriesPort for HttpApi {
    async fn read(
        &self,
        from: &StoryPosition,
        ctx: &QueryContext,
    ) -> Result<StoryReply, QueryError> {
        match from {
            StoryPosition::Start { limit } => {
                let mut ids: Vec<HnStoryId> = self.get_json("v0/topstories.json", ctx).await?;
                ids.truncate(*limit);
                Ok(selection(ids))
            }
            StoryPosition::Selected { ids, next_index } => match ids.get(*next_index) {
                Some(id) => {
                    let record = self.get_json(&format!("v0/item/{id}.json"), ctx).await?;
                    Ok(item(ids, *next_index, record))
                }
                None => Ok(FiniteRead::EndOfInput),
            },
        }
    }
}

pub struct HnStories {
    pub port: QueryPort<dyn StoriesPort>,
    pub max_stories: usize,
    pub poll_timeout: Duration,
}

#[async_trait]
impl TypedAsyncFiniteSourceHandler for HnStories {
    type Output = HnStory;

    fn poll_timeout(&self) -> Option<Duration> {
        Some(self.poll_timeout)
    }

    fn read(&self) -> SourceRead<HnStory> {
        SourceRead::finite(
            self.port.clone(),
            StoryPosition::Start {
                limit: self.max_stories,
            },
            decode_item,
        )
    }
}
