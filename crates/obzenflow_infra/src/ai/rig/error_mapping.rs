// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use obzenflow_core::ai::AiClientError;
use obzenflow_core::http_client::HttpClientError;
use rig_rs::http_client;
use rig_rs::ProviderError;

pub(crate) fn map_rig_error(err: ProviderError) -> AiClientError {
    let message = err.to_string();
    match &err {
        ProviderError::ProviderResponse(reply) | ProviderError::InvalidAuthentication(reply) => {
            match reply.status {
                Some(status) => status_failure(status.as_u16(), message),
                None => AiClientError::Other { message },
            }
        }
        ProviderError::Http(failure) => transport_failure(failure, message),
        ProviderError::Url(_) | ProviderError::Request(_) | ProviderError::UnsupportedOption(_) => {
            AiClientError::InvalidRequest { message }
        }
        _ => AiClientError::Other { message },
    }
}

fn status_failure(status: u16, message: String) -> AiClientError {
    match status {
        401 | 403 => AiClientError::Auth { message },
        429 => AiClientError::RateLimited {
            message,
            retry_after: None,
        },
        400..=499 => AiClientError::InvalidRequest { message },
        500..=599 => AiClientError::Remote { message },
        _ => AiClientError::Other { message },
    }
}

fn transport_failure(failure: &http_client::Error, message: String) -> AiClientError {
    match failure {
        http_client::Error::InvalidStatusCodeWithDetails { status, .. } => {
            status_failure(status.as_u16(), message)
        }
        http_client::Error::Protocol(_) | http_client::Error::StreamEnded => {
            AiClientError::Remote { message }
        }
        http_client::Error::InvalidHeaderValue(_)
        | http_client::Error::InvalidContentType(_)
        | http_client::Error::NoHeaders => AiClientError::InvalidRequest { message },
        http_client::Error::Instance(source) => match source.downcast_ref() {
            Some(HttpClientError::Timeout(_)) => AiClientError::Timeout { message },
            _ => AiClientError::Remote { message },
        },
        _ => AiClientError::Other { message },
    }
}
