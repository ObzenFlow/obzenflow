// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::SourceContinuation;
use crate::effects::SourceSystem;
use obzenflow_core::OneFactOutcome;

pub(super) mod sealed {
    pub trait Admitted<Family: ?Sized> {}
}

#[doc(hidden)]
pub struct AdmittedSource<H> {
    pub(crate) handler: H,
    pub(crate) continuation: SourceContinuation,
    pub(crate) system: SourceSystem,
}

impl<H> AdmittedSource<H> {
    pub fn handler(&self) -> &H {
        &self.handler
    }

    pub fn continuation(&self) -> SourceContinuation {
        self.continuation
    }

    pub fn system(&self) -> &SourceSystem {
        &self.system
    }
}

impl<H: super::UnifiedAsyncInfiniteSourceHandler> AdmittedSource<H> {
    pub fn hosted_ingress_slot(&self) -> Option<obzenflow_core::ingress::HostedIngressBindingSlot> {
        self.handler.hosted_ingress_slot()
    }
}

#[doc(hidden)]
#[diagnostic::on_unimplemented(
    message = "`{Self}` is not a source handler",
    note = "`source!` takes a typed source handler that declares its read port, or the `HostedSource<_>` from ingress registration"
)]
pub trait AdmitSource<Family: ?Sized>: sealed::Admitted<Family> + Send + Sync + 'static {
    type Output: OneFactOutcome + Send + Sync + 'static;
    type Handler: Send + Sync + 'static;

    fn admit(self) -> AdmittedSource<Self::Handler>;
}
