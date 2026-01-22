use super::domain::HnStory;
use obzenflow::sources::{DecodeError, DecodeResult, HttpResponse, PullDecoder, RequestSpec, Url};
use obzenflow_core::TypedPayload;
use std::collections::VecDeque;

/// Cursor state for the two-phase fetch pattern:
/// `topstories.json` → many `item/{id}.json`.
#[derive(Clone, Debug)]
pub enum HnFetchState {
    FetchItems { pending: VecDeque<u64> },
}

/// Decoder that fetches HN top stories via the Firebase API.
#[derive(Clone, Debug)]
pub struct HnStoryDecoder {
    base_url: Url,
    max_stories: usize,
}

impl HnStoryDecoder {
    pub fn new(base_url: Url, max_stories: usize) -> Self {
        Self {
            base_url,
            max_stories,
        }
    }

    fn topstories_url(&self) -> Url {
        self.base_url
            .join("v0/topstories.json")
            .expect("topstories url should join")
    }

    fn item_url(&self, id: u64) -> Url {
        self.base_url
            .join(&format!("v0/item/{id}.json"))
            .expect("item url should join")
    }
}

impl PullDecoder for HnStoryDecoder {
    type Cursor = HnFetchState;
    type Item = HnStory;

    fn event_type(&self) -> String {
        HnStory::versioned_event_type()
    }

    fn request_spec(&self, cursor: Option<&Self::Cursor>) -> RequestSpec {
        match cursor {
            None => RequestSpec::get(self.topstories_url()),
            Some(HnFetchState::FetchItems { pending }) => {
                let id = pending
                    .front()
                    .copied()
                    .expect("pending ids should not be empty");
                RequestSpec::get(self.item_url(id))
            }
        }
    }

    fn decode_success(
        &self,
        cursor: Option<&Self::Cursor>,
        response: &HttpResponse,
    ) -> Result<DecodeResult<Self::Cursor, Self::Item>, DecodeError> {
        match cursor {
            None => {
                let ids: Vec<u64> = response
                    .json()
                    .map_err(|e| DecodeError::Parse(e.to_string()))?;

                let take = self.max_stories.min(ids.len());
                tracing::info!(ids = ids.len(), take, "HN topstories fetched");

                if take == 0 {
                    return Ok(DecodeResult {
                        items: Vec::new(),
                        next_cursor: None,
                    });
                }

                let pending = ids.into_iter().take(take).collect::<VecDeque<_>>();
                Ok(DecodeResult {
                    items: Vec::new(),
                    next_cursor: Some(HnFetchState::FetchItems { pending }),
                })
            }
            Some(HnFetchState::FetchItems { pending }) => {
                let mut pending = pending.clone();
                let Some(id) = pending.pop_front() else {
                    return Ok(DecodeResult {
                        items: Vec::new(),
                        next_cursor: None,
                    });
                };

                // Deleted items return `null` — treat as skip.
                let story: Option<HnStory> = response
                    .json()
                    .map_err(|e| DecodeError::Parse(e.to_string()))?;

                match &story {
                    Some(_) => tracing::info!(id, "HN item fetched"),
                    None => tracing::info!(id, "HN item was deleted (null), skipping"),
                }

                let next_cursor = if pending.is_empty() {
                    None
                } else {
                    Some(HnFetchState::FetchItems { pending })
                };

                Ok(DecodeResult {
                    items: story.into_iter().collect(),
                    next_cursor,
                })
            }
        }
    }
}
