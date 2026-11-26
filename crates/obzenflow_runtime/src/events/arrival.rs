// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use super::{IngressReceiptSender, SourceReceiver};
use crate::stages::common::handlers::source::SourceError;
use crate::systems::{PortImpl, ServesPort};
use async_trait::async_trait;
use obzenflow_core::event::schema::TypedFact;
use obzenflow_core::event::SourceDiagnosticReason;
use obzenflow_core::ingress::{HostedIngressBindingSlot, IngressContext};
use serde::Serialize;
use std::fmt;
use std::sync::Arc;

/// `Events` is the framework's event port. Its system holds the events that senders
/// pushed. The source stage takes them one at a time.
///
/// ```
/// # use obzenflow_runtime::events::{source_channel, subscribe, Arrival, Events};
/// # use obzenflow_runtime::events::{IngressDecodeError, IngressDecoder, IngressRecord};
/// # use obzenflow_runtime::stages::common::handlers::SourceError;
/// # use obzenflow_runtime::systems::Port;
/// # #[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
/// # struct Debit { amount: u32 }
/// # impl obzenflow_core::TypedPayload for Debit { const EVENT_TYPE: &'static str = "docs.debit"; }
/// # #[derive(Clone)]
/// # struct DebitIngress;
/// # impl IngressDecoder for DebitIngress { type Output = Debit; }
/// struct DebitBus {
///     rx: obzenflow_runtime::events::SourceReceiver<serde_json::Value>,
/// }
///
/// #[async_trait::async_trait]
/// impl Events for DebitBus {
///     async fn ready(&self) -> Result<(), SourceError> {
///         Events::ready(&self.rx).await
///     }
///
///     fn take(&self) -> Option<Arrival> {
///         self.rx.take(1).pop().map(Arrival::new)
///     }
/// }
///
/// let (_tx, rx) = source_channel(16);
/// let bus = subscribe(DebitIngress).through(Port::to(DebitBus { rx }));
/// ```
#[async_trait]
pub trait Events: Send + Sync {
    /// `ready` waits until the system holds an event. It takes no event. An error stops
    /// the live reads of the stage.
    async fn ready(&self) -> Result<(), SourceError>;

    /// `take` removes the oldest event and returns it immediately. It returns `None` when
    /// the system holds no event.
    fn take(&self) -> Option<Arrival>;

    #[doc(hidden)]
    fn hosted_ingress(&self) -> Option<HostedIngressBindingSlot> {
        None
    }
}

/// `Arrival` is one event that [`Events::take`] returns.
pub struct Arrival {
    pub(crate) kind: ArrivalKind,
}

pub(crate) enum ArrivalKind {
    Body(serde_json::Value),
    Admitted(Box<AdmittedArrival>),
    Unrepresentable(String),
}

pub(crate) struct AdmittedArrival {
    pub(crate) fact: TypedFact,
    pub(crate) context: IngressContext,
    pub(crate) receipt: IngressReceiptSender,
}

impl Arrival {
    /// `Arrival::new` makes an event from its body. The decoder of the `subscribe`
    /// line decodes the body into the output of the source.
    pub fn new(body: serde_json::Value) -> Self {
        Self {
            kind: ArrivalKind::Body(body),
        }
    }

    #[doc(hidden)]
    pub fn admitted(
        fact: TypedFact,
        context: IngressContext,
        receipt: IngressReceiptSender,
    ) -> Self {
        Self {
            kind: ArrivalKind::Admitted(Box::new(AdmittedArrival {
                fact,
                context,
                receipt,
            })),
        }
    }
}

impl fmt::Debug for Arrival {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let kind = match &self.kind {
            ArrivalKind::Body(_) => "body",
            ArrivalKind::Admitted(_) => "admitted",
            ArrivalKind::Unrepresentable(_) => "unrepresentable",
        };
        f.debug_struct("Arrival")
            .field("kind", &kind)
            .finish_non_exhaustive()
    }
}

#[async_trait]
impl<T: Serialize + Send + 'static> Events for SourceReceiver<T> {
    async fn ready(&self) -> Result<(), SourceError> {
        SourceReceiver::ready(self)
            .await
            .map_err(|_| SourceError::Other(SourceDiagnosticReason::InputClosed.into()))
    }

    fn take(&self) -> Option<Arrival> {
        let value = SourceReceiver::take(self, 1).pop()?;
        Some(match serde_json::to_value(value) {
            Ok(body) => Arrival::new(body),
            Err(error) => Arrival {
                kind: ArrivalKind::Unrepresentable(error.to_string()),
            },
        })
    }
}

#[async_trait]
impl<T: Events + ?Sized> Events for Arc<T> {
    async fn ready(&self) -> Result<(), SourceError> {
        (**self).ready().await
    }

    fn take(&self) -> Option<Arrival> {
        (**self).take()
    }

    fn hosted_ingress(&self) -> Option<HostedIngressBindingSlot> {
        (**self).hosted_ingress()
    }
}

impl<T: Events + 'static> ServesPort<dyn Events, T> for PortImpl<T> {
    fn coerce(port: Arc<T>) -> Arc<dyn Events> {
        port
    }
}
