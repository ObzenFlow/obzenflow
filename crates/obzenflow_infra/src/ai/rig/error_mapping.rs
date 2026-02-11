// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use obzenflow_core::ai::AiClientError;
use std::error::Error;

pub(crate) fn map_rig_error<E>(err: E) -> AiClientError
where
    E: Error + Send + Sync + 'static,
{
    let message = err.to_string();
    let err_ref: &(dyn Error + 'static) = &err;
    let mut saw_rig_instance_error = false;

    if let Some(http_err) = find_rig_http_error(err_ref) {
        match http_err {
            rig_rs::http_client::Error::InvalidStatusCode(code)
            | rig_rs::http_client::Error::InvalidStatusCodeWithMessage(code, _) => {
                return match code.as_u16() {
                    401 | 403 => AiClientError::Auth { message },
                    429 => AiClientError::RateLimited {
                        message,
                        retry_after: None,
                    },
                    400..=499 => AiClientError::InvalidRequest { message },
                    500..=599 => AiClientError::Remote { message },
                    _ => AiClientError::Other { message },
                };
            }
            rig_rs::http_client::Error::Protocol(_) | rig_rs::http_client::Error::StreamEnded => {
                return AiClientError::Remote { message };
            }
            rig_rs::http_client::Error::InvalidHeaderValue(_)
            | rig_rs::http_client::Error::InvalidContentType(_)
            | rig_rs::http_client::Error::NoHeaders => {
                return AiClientError::InvalidRequest { message };
            }
            rig_rs::http_client::Error::Instance(_) => {
                // Defer to the underlying error (often `reqwest::Error`) for better classification.
                saw_rig_instance_error = true;
            }
        };
    }

    if let Some(reqwest_err) = find_reqwest_error(err_ref) {
        if reqwest_err.is_timeout() {
            return AiClientError::Timeout { message };
        }

        if let Some(status) = reqwest_err.status() {
            let code = status.as_u16();
            return match code {
                401 | 403 => AiClientError::Auth { message },
                429 => AiClientError::RateLimited {
                    message,
                    retry_after: None,
                },
                400..=499 => AiClientError::InvalidRequest { message },
                500..=599 => AiClientError::Remote { message },
                _ => AiClientError::Other { message },
            };
        }

        if reqwest_err.is_connect() {
            return AiClientError::Remote { message };
        }
    }

    if err_ref.is::<serde_json::Error>()
        || err_ref.is::<url::ParseError>()
        || err_ref.is::<std::num::ParseIntError>()
    {
        return AiClientError::InvalidRequest { message };
    }

    if saw_rig_instance_error {
        return AiClientError::Remote { message };
    }

    AiClientError::Other { message }
}

fn find_reqwest_error<'a>(mut err: &'a (dyn Error + 'static)) -> Option<&'a reqwest::Error> {
    loop {
        if let Some(reqwest_err) = err.downcast_ref::<reqwest::Error>() {
            return Some(reqwest_err);
        }
        err = err.source()?;
    }
}

fn find_rig_http_error<'a>(
    mut err: &'a (dyn Error + 'static),
) -> Option<&'a rig_rs::http_client::Error> {
    loop {
        if let Some(rig_err) = err.downcast_ref::<rig_rs::http_client::Error>() {
            return Some(rig_err);
        }
        err = err.source()?;
    }
}
