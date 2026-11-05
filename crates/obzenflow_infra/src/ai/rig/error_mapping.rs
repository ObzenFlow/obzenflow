// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use crate::ai::provider_errors::{unclaimed_status, ModelCalls};
use obzenflow_core::ai::AiProviderError;
use obzenflow_core::http_client::{HttpClientError, HttpResponse};
use rig_rs::http_client;
use rig_rs::ProviderError;

pub(crate) fn map_rig_error(err: ProviderError, calls: &ModelCalls) -> AiProviderError {
    let message = err.to_string();
    match &err {
        ProviderError::ProviderResponse(reply) | ProviderError::InvalidAuthentication(reply) => {
            match reply.status {
                Some(status) => unclaimed_status(&HttpResponse::new(
                    status.as_u16(),
                    reply.headers.clone().unwrap_or_default(),
                    reply.body.clone(),
                )),
                None => AiProviderError::Other { message },
            }
        }
        ProviderError::Http(failure) => transport_failure(failure, calls, message),
        ProviderError::Url(_) | ProviderError::Request(_) | ProviderError::UnsupportedOption(_) => {
            AiProviderError::InvalidRequest { message }
        }
        _ => AiProviderError::Other { message },
    }
}

fn transport_failure(
    failure: &http_client::Error,
    calls: &ModelCalls,
    message: String,
) -> AiProviderError {
    match failure {
        http_client::Error::Instance(source) => match source.downcast_ref::<HttpClientError>() {
            Some(error) => calls.unsent(error.clone()),
            None => AiProviderError::Remote { message },
        },
        http_client::Error::Protocol(_) | http_client::Error::StreamEnded => {
            AiProviderError::Remote { message }
        }
        http_client::Error::InvalidHeaderValue(_)
        | http_client::Error::InvalidContentType(_)
        | http_client::Error::NoHeaders => AiProviderError::InvalidRequest { message },
        _ => AiProviderError::Other { message },
    }
}
