// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::prepared::AdmittedSource;
use super::traits::{
    AsyncFiniteSourceHandler, AsyncInfiniteSourceHandler, FiniteSourceHandler,
    InfiniteSourceHandler,
};
use super::{RefusedBy, SourceContinuation, SourceError};
use crate::effects::SourceSystem;
use async_trait::async_trait;
use obzenflow_core::ChainEvent;

/// A structural source row with no reader or authored facts.
#[doc(hidden)]
pub struct PlaceholderSource {
    message: Option<&'static str>,
    warned: bool,
}

impl PlaceholderSource {
    pub fn new(message: Option<&'static str>) -> Self {
        Self {
            message,
            warned: false,
        }
    }

    fn warn_once(&mut self, kind: &str) {
        if !self.warned {
            self.warned = true;
            match self.message {
                Some(message) => tracing::warn!("{kind} placeholder executed: {message}"),
                None => tracing::warn!("{kind} placeholder executed"),
            }
        }
    }
}

impl AdmittedSource<PlaceholderSource> {
    pub fn placeholder(message: Option<&'static str>) -> Self {
        Self {
            handler: PlaceholderSource::new(message),
            continuation: SourceContinuation::Refuse {
                by: RefusedBy::Handler,
            },
            system: SourceSystem::placeholder(),
        }
    }
}

impl FiniteSourceHandler for PlaceholderSource {
    fn next(&mut self) -> Result<Option<Vec<ChainEvent>>, SourceError> {
        self.warn_once("finite source");
        Ok(None)
    }
}

impl InfiniteSourceHandler for PlaceholderSource {
    fn next(&mut self) -> Result<Vec<ChainEvent>, SourceError> {
        self.warn_once("infinite source");
        Ok(Vec::new())
    }
}

#[async_trait]
impl AsyncFiniteSourceHandler for PlaceholderSource {
    async fn next(&mut self) -> Result<Option<Vec<ChainEvent>>, SourceError> {
        self.warn_once("async source");
        Ok(None)
    }
}

#[async_trait]
impl AsyncInfiniteSourceHandler for PlaceholderSource {
    async fn next(&mut self) -> Result<Vec<ChainEvent>, SourceError> {
        self.warn_once("async infinite source");
        Ok(Vec::new())
    }
}
