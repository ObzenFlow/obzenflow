// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::domain::{HnStory, HnStoryId};
use async_trait::async_trait;
use obzenflow::stages::sources::{
    AsyncFiniteSourceConnector, Refuse, SourceError, SourceReaderInitContext,
    TypedAsyncFiniteSourceHandler,
};

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

pub fn scripted_stories(max_stories: usize) -> Vec<HnStory> {
    (0..LISTED_STORIES.min(max_stories))
        .filter(|offset| (offset + 1) % DELETED_EVERY_NTH != 0)
        .map(scripted_story)
        .collect()
}

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

pub struct ScriptedStories {
    stories: Vec<HnStory>,
}

impl ScriptedStories {
    pub fn new(stories: Vec<HnStory>) -> Self {
        Self { stories }
    }
}

#[obzenflow::system("scripted-stories")]
#[async_trait]
impl AsyncFiniteSourceConnector for ScriptedStories {
    type Output = HnStory;
    type Reader = ScriptedStoriesReader;
    type Continuation = Refuse;

    async fn open(&self, _context: SourceReaderInitContext) -> Result<Self::Reader, SourceError> {
        Ok(ScriptedStoriesReader {
            stories: self.stories.clone().into_iter(),
        })
    }
}

pub struct ScriptedStoriesReader {
    stories: std::vec::IntoIter<HnStory>,
}

#[async_trait]
impl TypedAsyncFiniteSourceHandler for ScriptedStoriesReader {
    type Output = HnStory;

    async fn next(&mut self) -> Result<Option<Vec<HnStory>>, SourceError> {
        Ok(self.stories.next().map(|story| vec![story]))
    }
}
