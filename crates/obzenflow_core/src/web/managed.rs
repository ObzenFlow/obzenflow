// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Managed response types for ObzenFlow-owned web surfaces.

use crate::web::types::Response;
use futures_core::Stream;
use serde::{Deserialize, Serialize};
use std::pin::Pin;
use std::task::{Context, Poll};

/// A single Server-Sent Events (SSE) frame.
///
/// This is the wire-level value model. Concrete servers adapt these values into
/// framework-native SSE primitives internally.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SseFrame {
    pub id: Option<String>,
    pub event: Option<String>,
    pub data: String,
    pub comment: Option<String>,
    pub retry_ms: Option<u64>,
}

impl SseFrame {
    pub fn data(data: impl Into<String>) -> Self {
        Self {
            id: None,
            event: None,
            data: data.into(),
            comment: None,
            retry_ms: None,
        }
    }

    pub fn event(event: impl Into<String>, data: impl Into<String>) -> Self {
        Self {
            id: None,
            event: Some(event.into()),
            data: data.into(),
            comment: None,
            retry_ms: None,
        }
    }

    pub fn comment(comment: impl Into<String>) -> Self {
        Self {
            id: None,
            event: None,
            data: String::new(),
            comment: Some(comment.into()),
            retry_ms: None,
        }
    }
}

/// Framework-neutral SSE body wrapper.
///
/// The goal is to keep type machinery out of the public API while still allowing
/// implementations to produce SSE frames from any async source.
pub struct SseBody {
    inner: Pin<Box<dyn Stream<Item = SseFrame> + Send>>,
}

impl std::fmt::Debug for SseBody {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SseBody").finish_non_exhaustive()
    }
}

impl SseBody {
    pub fn new<S>(stream: S) -> Self
    where
        S: Stream<Item = SseFrame> + Send + 'static,
    {
        Self {
            inner: Box::pin(stream),
        }
    }

    pub fn into_stream(self) -> Pin<Box<dyn Stream<Item = SseFrame> + Send>> {
        self.inner
    }
}

impl Stream for SseBody {
    type Item = SseFrame;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.inner.as_mut().poll_next(cx)
    }
}

/// Framework-neutral response enum for managed routes.
#[derive(Debug)]
pub enum ManagedResponse {
    Unary(Response),
    Sse(SseBody),
}

impl From<Response> for ManagedResponse {
    fn from(value: Response) -> Self {
        Self::Unary(value)
    }
}

