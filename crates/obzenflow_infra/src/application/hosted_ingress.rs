// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::{ApplicationError, WebSurfaceAttachment};
use crate::web::endpoints::event_ingestion::{http_endpoint, IngestionConfig, IngestionState};
use obzenflow_core::ingress::HostedIngressBindingSlot;
use obzenflow_runtime::events::Events;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, PoisonError};

/// `HostedIngress` registers the hosted HTTP endpoints of a flow during the build.
/// `FlowApplication` attaches their routes before it starts the web server.
#[derive(Clone, Default)]
pub struct HostedIngress {
    serving: Arc<AtomicBool>,
    registrations: Arc<Mutex<Vec<Registration>>>,
}

pub(crate) struct Registration {
    pub(crate) surface: WebSurfaceAttachment,
    pub(crate) state: IngestionState,
}

impl HostedIngress {
    /// `http` registers a hosted HTTP endpoint and returns its event system. Call it in
    /// the factory of the flow. It returns an error if `FlowApplication` does not run
    /// with `--server`.
    pub fn http(&self, config: IngestionConfig) -> Result<impl Events + 'static, ApplicationError> {
        let (events, state, surface) = http_endpoint(config);
        if !self.serving.load(Ordering::Acquire) {
            return Err(ApplicationError::InvalidConfiguration(format!(
                "hosted ingress '{}' was configured, but FlowApplication is not running with \
                 --server",
                state.ingress_key()
            )));
        }
        self.registrations
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push(Registration { surface, state });
        Ok(events)
    }

    pub(crate) fn serve(&self, serving: bool) {
        self.serving.store(serving, Ordering::Release);
    }

    pub(crate) fn take_registrations(&self) -> Vec<Registration> {
        std::mem::take(
            &mut *self
                .registrations
                .lock()
                .unwrap_or_else(PoisonError::into_inner),
        )
    }
}

impl Registration {
    pub(crate) fn slot(&self) -> HostedIngressBindingSlot {
        self.state.ingress_slot()
    }
}
