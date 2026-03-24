// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::domain::HnStory;
use obzenflow::sources::{DecodeError, ListDetailDecoder, RequestSpec, Url};
use obzenflow_core::TypedPayload;

/// Build the decoder for the HN Firebase API:
/// `topstories.json` → many `item/{id}.json`.
pub fn hn_story_decoder(base_url: Url, max_stories: usize) -> ListDetailDecoder<u64, HnStory> {
    let topstories_url = base_url
        .join("v0/topstories.json")
        .expect("topstories url should join");

    ListDetailDecoder::new(
        HnStory::versioned_event_type(),
        topstories_url,
        move |response| {
            let ids: Vec<u64> = response
                .json()
                .map_err(|e| DecodeError::Parse(e.to_string()))?;
            let take = max_stories.min(ids.len());
            tracing::info!(ids = ids.len(), take, "HN topstories fetched");
            Ok(ids)
        },
        {
            let base_url = base_url.clone();
            move |id: &u64| {
                let url = base_url
                    .join(&format!("v0/item/{id}.json"))
                    .expect("item url should join");
                RequestSpec::get(url)
            }
        },
        |response| {
            // Deleted items return `null` — treat as skip.
            let story: Option<HnStory> = response
                .json()
                .map_err(|e| DecodeError::Parse(e.to_string()))?;

            if let Some(story) = &story {
                tracing::info!(id = story.id, "HN item fetched");
            }

            Ok(story)
        },
    )
    .max_list_items(max_stories)
    .on_skip(|id| tracing::info!(id, "HN item was deleted (null), skipping"))
}
