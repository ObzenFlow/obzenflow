// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::default_http_client;
use obzenflow_adapters::effects::http;
use obzenflow_core::effect_binding::{HttpEndpoint, SystemLocation};
use obzenflow_core::event::observability::{HttpPullMeasurements, ObservationRecord};
use obzenflow_core::http_client::{HttpClient, HttpRequest, Url};
use obzenflow_core::BindingEvidenceError;
use obzenflow_runtime::effects::EffectContext;
use obzenflow_runtime::effects::{
    EffectError, EffectPortResolutionError, Port, PortImpl, ServesPort, SystemName,
};
use serde::de::DeserializeOwned;
use std::sync::Arc;
use std::sync::Mutex;

/// The HTTP system an application port can use to read JSON.
pub struct HttpApi {
    base: Url,
    client: Arc<dyn HttpClient>,
    measurements: Mutex<HttpPullMeasurements>,
}

impl SystemName for HttpApi {
    const NAME: &'static str = "http-api";
}

impl HttpApi {
    pub fn json(base: Url) -> Result<JsonEndpoint, BindingEvidenceError> {
        let location = HttpEndpoint::from_url(&base)?;
        Ok(JsonEndpoint { base, location })
    }

    pub async fn get_json<T: DeserializeOwned>(
        &self,
        path: &str,
        ctx: &EffectContext,
    ) -> Result<T, EffectError> {
        let url = self.base.join(path).map_err(http::unreadable)?;
        let response = self.client.send(HttpRequest::get(url)).await;
        {
            let mut totals = self
                .measurements
                .lock()
                .expect("HTTP measurement lock poisoned");
            totals.requests_total += 1;
            if let Ok(response) = &response {
                match response.status {
                    200..=299 => totals.responses_2xx += 1,
                    429 => {
                        totals.responses_4xx += 1;
                        totals.rate_limited_total += 1;
                    }
                    400..=499 => totals.responses_4xx += 1,
                    500..=599 => totals.responses_5xx += 1,
                    _ => {}
                }
            }
            ctx.observe(ObservationRecord::HttpPull(totals.clone()));
        }
        let response = response.map_err(http::unsent::<Self>)?;
        if !response.is_success() {
            return Err(http::unclaimed(&response));
        }
        response.json().map_err(http::unreadable)
    }
}

/// A configured endpoint. The port resolves its client on the first live call.
pub struct JsonEndpoint {
    base: Url,
    location: HttpEndpoint,
}

impl JsonEndpoint {
    pub fn deferred_port<P: ?Sized + Send + Sync + 'static>(
        self,
    ) -> Result<Port<P>, BindingEvidenceError>
    where
        PortImpl<HttpApi>: ServesPort<P, HttpApi>,
    {
        let port = Port::deferred(move || {
            Ok(HttpApi {
                base: self.base.clone(),
                measurements: Mutex::default(),
                client: default_http_client()
                    .map_err(|_| EffectPortResolutionError::ClientConstructionFailed)?,
            })
        });
        port.with_system_location(SystemLocation::Json(self.location))
    }
}
