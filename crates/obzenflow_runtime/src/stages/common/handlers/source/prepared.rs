// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Sealed, single-owner source admission. No opening or producer calls happen
//! while preparing a descriptor. The supervisor alone acquires its live reader.

use super::erased::*;
use super::typed::*;
use super::SourceError;
use crate::stages::source::{
    AsyncFiniteSourceConnector, AsyncInfiniteSourceConnector, FiniteSourceConnector,
    InfiniteSourceConnector, SourceReaderInitContext,
};
use async_trait::async_trait;
use futures::future::BoxFuture;
use obzenflow_core::event::observability::ObservationRecorder;
use obzenflow_core::ingress::HostedIngressBindingSlot;
use obzenflow_core::{OneFactStageOutput, WriterId};
use std::sync::Arc;
use std::time::Duration;

mod sealed {
    pub trait Admitted<Kind> {}
}

#[doc(hidden)]
pub enum DirectSource {}
#[doc(hidden)]
pub enum ConnectorSource {}

// An opening future owns its configuration. Cancellation consumes admission,
// so an interrupted acquisition can never silently reopen or reuse a producer.
enum ReaderState<R: ?Sized, F> {
    Cold(F),
    Acquired(Box<R>),
    Consumed,
}

macro_rules! source_admission {
    ($admit:ident, $prepared:ident, $typed:ident, $connector:ident,
     $unified:ident, $seal:ident, [$($async:tt)*], [$($await:tt)*], $result:ty,
     $open:expr, $direct:expr, $metadata:expr, {$($extra:tt)*}) => {
        #[doc(hidden)]
        pub trait $admit<Kind>: sealed::Admitted<Kind> + Send + Sync + 'static {
            type Output: OneFactStageOutput + Send + Sync + 'static;
            fn prepare(self) -> $prepared;
        }

        impl<H: $typed + 'static> sealed::Admitted<(DirectSource, $prepared)> for H {}
        impl<C: $connector> sealed::Admitted<(ConnectorSource, $prepared)> for C {}

        impl<H: $typed + 'static> $admit<(DirectSource, $prepared)> for H {
            type Output = H::Output;
            fn prepare(self) -> $prepared {
                // Hosted ingress registers its cold binding, never its receiver.
                let slot = ($metadata)(&self);
                $prepared {
                    state: ReaderState::Cold(Box::new(move |_| ($direct)(self))),
                    writer_id: None,
                    recorder: None,
                    hosted_slot: slot,
                }
            }
        }

        impl<C: $connector> $admit<(ConnectorSource, $prepared)> for C {
            type Output = C::Output;
            fn prepare(self) -> $prepared {
                $prepared {
                    state: ReaderState::Cold(Box::new(move |context| ($open)(self, context))),
                    writer_id: None,
                    recorder: None,
                    hosted_slot: None,
                }
            }
        }

        #[doc(hidden)]
        pub struct $prepared {
            state: ReaderState<dyn $unified,
                Box<dyn FnOnce(SourceReaderInitContext) -> $result + Send + Sync>>,
            writer_id: Option<WriterId>,
            recorder: Option<Arc<dyn ObservationRecorder>>,
            // Only the asynchronous infinite family uses this cold registration.
            #[allow(dead_code)]
            hosted_slot: Option<HostedIngressBindingSlot>,
        }

        impl $seal for $prepared {}

        #[async_trait]
        impl $unified for $prepared {
            fn install_writer_id(&mut self, writer_id: WriterId) {
                self.writer_id = Some(writer_id);
            }

            fn install_observation_recorder(&mut self, recorder: Arc<dyn ObservationRecorder>) {
                self.recorder = Some(recorder);
            }

            $($async)* fn acquire(&mut self, context: SourceReaderInitContext) -> Result<(), SourceError> {
                if matches!(self.state, ReaderState::Acquired(_)) {
                    return Ok(());
                }
                let ReaderState::Cold(open) = std::mem::replace(&mut self.state, ReaderState::Consumed) else {
                    return Err(SourceError::Other("source acquisition already consumed".into()));
                };
                // Adapter construction is inert. Store ownership before installing
                // capabilities or performing any further fallible or awaited work.
                self.state = ReaderState::Acquired(open(context)$($await)*?);
                let ReaderState::Acquired(reader) = &mut self.state else { unreachable!() };
                if let Some(writer_id) = self.writer_id {
                    reader.install_writer_id(writer_id);
                }
                if let Some(recorder) = &self.recorder {
                    reader.install_observation_recorder(recorder.clone());
                }
                Ok(())
            }

            $($async)* fn next_invocation(&mut self) -> ErasedSourceInvocation {
                let ReaderState::Acquired(reader) = &mut self.state else {
                    unreachable!("the source supervisor must acquire before polling")
                };
                reader.next_invocation()$($await)*
            }

            $($extra)*
        }
    };
}

source_admission!(
    AdmitFiniteSource,
    PreparedFiniteSource,
    TypedFiniteSourceHandler,
    FiniteSourceConnector,
    UnifiedFiniteSourceHandler,
    SealFinite,
    [],
    [],
    Result<Box<dyn UnifiedFiniteSourceHandler>, SourceError>,
    |connector: C, context| Ok(Box::new(TypedFiniteSourceHandlerAdapter::new(
        connector.open(context)?
    )) as Box<dyn UnifiedFiniteSourceHandler>),
    |handler: H| Ok(Box::new(TypedFiniteSourceHandlerAdapter::new(handler))
        as Box<dyn UnifiedFiniteSourceHandler>),
    |_: &H| None,
    {}
);

source_admission!(
    AdmitInfiniteSource,
    PreparedInfiniteSource,
    TypedInfiniteSourceHandler,
    InfiniteSourceConnector,
    UnifiedInfiniteSourceHandler,
    SealInfinite,
    [],
    [],
    Result<Box<dyn UnifiedInfiniteSourceHandler>, SourceError>,
    |connector: C, context| Ok(Box::new(TypedInfiniteSourceHandlerAdapter::new(
        connector.open(context)?
    )) as Box<dyn UnifiedInfiniteSourceHandler>),
    |handler: H| Ok(Box::new(TypedInfiniteSourceHandlerAdapter::new(handler))
        as Box<dyn UnifiedInfiniteSourceHandler>),
    |_: &H| None,
    {}
);

source_admission!(
    AdmitAsyncFiniteSource, PreparedAsyncFiniteSource, TypedAsyncFiniteSourceHandler, AsyncFiniteSourceConnector,
    UnifiedAsyncFiniteSourceHandler, SealAsyncFinite,
    [async], [.await], BoxFuture<'static, Result<Box<dyn UnifiedAsyncFiniteSourceHandler>, SourceError>>,
    |connector: C, context| Box::pin(async move { Ok(Box::new(TypedAsyncFiniteSourceHandlerAdapter::new(connector.open(context).await?)) as Box<dyn UnifiedAsyncFiniteSourceHandler>) }),
    |handler: H| Box::pin(async move { Ok(Box::new(TypedAsyncFiniteSourceHandlerAdapter::new(handler)) as Box<dyn UnifiedAsyncFiniteSourceHandler>) }),
    |_: &H| None, {
        fn poll_timeout(&self) -> Option<Duration> {
            match &self.state {
                ReaderState::Acquired(reader) => reader.poll_timeout(),
                _ => Some(Duration::from_secs(30)),
            }
        }

        async fn drain(&mut self) -> Result<(), SourceError> {
            if let ReaderState::Acquired(reader) = &mut self.state { reader.drain().await } else { Ok(()) }
        }
    }
);

source_admission!(
    AdmitAsyncInfiniteSource, PreparedAsyncInfiniteSource, TypedAsyncInfiniteSourceHandler, AsyncInfiniteSourceConnector,
    UnifiedAsyncInfiniteSourceHandler, SealAsyncInfinite,
    [async], [.await], BoxFuture<'static, Result<Box<dyn UnifiedAsyncInfiniteSourceHandler>, SourceError>>,
    |connector: C, context| Box::pin(async move { Ok(Box::new(TypedAsyncInfiniteSourceHandlerAdapter::new(connector.open(context).await?)) as Box<dyn UnifiedAsyncInfiniteSourceHandler>) }),
    |handler: H| Box::pin(async move { Ok(Box::new(TypedAsyncInfiniteSourceHandlerAdapter::new(handler)) as Box<dyn UnifiedAsyncInfiniteSourceHandler>) }),
    |handler: &H| handler.runtime_registration().map(|registration| registration.hosted_ingress_slot), {
        fn poll_timeout(&self) -> Option<Duration> {
            match &self.state {
                ReaderState::Acquired(reader) => reader.poll_timeout(),
                _ => None,
            }
        }

        fn hosted_ingress_slot(&self) -> Option<HostedIngressBindingSlot> { self.hosted_slot.clone() }

        async fn drain(&mut self) -> Result<(), SourceError> {
            if let ReaderState::Acquired(reader) = &mut self.state { reader.drain().await } else { Ok(()) }
        }
    }
);
