// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! A sink that emits each input as a structured event through the
//! application's tracing subscriber.

use async_trait::async_trait;
use obzenflow_core::event::schema::{EmptySet, WithMember};
use obzenflow_core::TypedPayload;
use obzenflow_runtime::effects::{
    port, system, Command, DeclaredEffectSet, EffectError, Port, SinkRole,
};
use obzenflow_runtime::stages::common::handler_error::HandlerError;
use serde::Serialize;
use serde_json::{Map, Value};
use std::marker::PhantomData;
use std::sync::OnceLock;

/// The level a tracing sink emits its event at.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum TracingLevel {
    Trace,
    Debug,
    Info,
    Warn,
    Error,
}

/// The event a tracing sink emits for one input: a level, a message and
/// named fields.
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct TracingOutput {
    level: TracingLevel,
    message: String,
    fields: Map<String, Value>,
}

impl TracingOutput {
    pub fn new(level: TracingLevel, message: impl Into<String>) -> Self {
        Self {
            level,
            message: message.into(),
            fields: Map::new(),
        }
    }

    pub fn trace(message: impl Into<String>) -> Self {
        Self::new(TracingLevel::Trace, message)
    }

    pub fn debug(message: impl Into<String>) -> Self {
        Self::new(TracingLevel::Debug, message)
    }

    pub fn info(message: impl Into<String>) -> Self {
        Self::new(TracingLevel::Info, message)
    }

    pub fn warn(message: impl Into<String>) -> Self {
        Self::new(TracingLevel::Warn, message)
    }

    pub fn error(message: impl Into<String>) -> Self {
        Self::new(TracingLevel::Error, message)
    }

    /// Add one named field. A value that can't be serialized records its
    /// serialization error as text.
    pub fn field(mut self, name: impl Into<String>, value: impl Serialize) -> Self {
        let value = serde_json::to_value(value)
            .unwrap_or_else(|error| Value::String(format!("unserializable: {error}")));
        self.fields.insert(name.into(), value);
        self
    }
}

/// The port a tracing sink emits through. An event a crash left in doubt may
/// be emitted again after a resume, so the method is `at_least_once`.
#[port]
#[async_trait]
pub trait TracingPort {
    #[effect(type = "obzenflow.tracing.event", at_least_once, records = reply)]
    async fn emit(&self, output: &TracingOutput) -> Result<(), EffectError>;
}

struct Tracing;

#[system("tracing")]
#[async_trait]
impl TracingPort for Tracing {
    async fn emit(&self, output: &TracingOutput) -> Result<(), EffectError> {
        let fields = Value::Object(output.fields.clone());
        let message = output.message.as_str();
        match output.level {
            TracingLevel::Trace => tracing::trace!(fields = %fields, "{message}"),
            TracingLevel::Debug => tracing::debug!(fields = %fields, "{message}"),
            TracingLevel::Info => tracing::info!(fields = %fields, "{message}"),
            TracingLevel::Warn => tracing::warn!(fields = %fields, "{message}"),
            TracingLevel::Error => tracing::error!(fields = %fields, "{message}"),
        }
        Ok(())
    }
}

fn tracing_port() -> Port<dyn TracingPort> {
    static PORT: OnceLock<Port<dyn TracingPort>> = OnceLock::new();
    PORT.get_or_init(|| Port::to(Tracing)).clone()
}

/// A sink that emits one tracing event per input. The callback builds the
/// event from the input; only the live system emits it, so a replay, which
/// calls no system, emits nothing.
pub struct TracingSink<T, F> {
    event: F,
    port: Port<dyn TracingPort>,
    _input: PhantomData<fn() -> T>,
}

impl<T, F> TracingSink<T, F>
where
    T: TypedPayload + Send + Sync + 'static,
    F: Fn(&T) -> TracingOutput + Clone + Send + Sync + 'static,
{
    pub fn new(event: F) -> Self {
        Self {
            event,
            port: tracing_port(),
            _input: PhantomData,
        }
    }
}

impl<T, F> TracingSink<T, F> {
    /// Emit through another system that answers `TracingPort`, such as a test
    /// port.
    pub fn through(mut self, port: Port<dyn TracingPort>) -> Self {
        self.port = port;
        self
    }
}

impl<T, F: Clone> Clone for TracingSink<T, F> {
    fn clone(&self) -> Self {
        Self {
            event: self.event.clone(),
            port: self.port.clone(),
            _input: PhantomData,
        }
    }
}

impl<T, F> std::fmt::Debug for TracingSink<T, F> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TracingSink")
            .field("input", &std::any::type_name::<T>())
            .field("port", &self.port)
            .finish_non_exhaustive()
    }
}

impl<T, F> Command for TracingSink<T, F> {
    type Ports = (Port<dyn TracingPort>, ());
    type AllowedEffects = DeclaredEffectSet<WithMember<TracingOutput, EmptySet>>;

    fn ports(&self) -> Self::Ports {
        (self.port.clone(), ())
    }
}

impl<T, F> SinkRole<T> for TracingSink<T, F>
where
    T: TypedPayload + Send + Sync + 'static,
    F: Fn(&T) -> TracingOutput + Clone + Send + Sync + 'static,
{
    type Request = TracingOutput;

    fn prepare(&self, input: &T) -> Result<TracingOutput, HandlerError> {
        Ok((self.event)(input))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::Deserialize;

    #[derive(Deserialize, Serialize)]
    struct Item(u64);
    impl TypedPayload for Item {
        const EVENT_TYPE: &'static str = "tracing.test.item";
    }

    #[test]
    fn tracing_request_carries_named_fields() {
        let sink = TracingSink::new(|item: &Item| {
            TracingOutput::info("terminal item observed").field("value", item.0)
        });
        let request = sink.prepare(&Item(7)).unwrap();
        assert_eq!(request.message, "terminal item observed");
        assert_eq!(request.fields.get("value"), Some(&Value::from(7)));
        assert_eq!(sink.ports().0.system_name(), "tracing");
    }

    struct CountEvents(std::sync::Arc<std::sync::atomic::AtomicUsize>);

    impl<S: tracing::Subscriber> tracing_subscriber::Layer<S> for CountEvents {
        fn on_event(&self, _: &tracing::Event<'_>, _: tracing_subscriber::layer::Context<'_, S>) {
            self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        }
    }

    #[tokio::test]
    async fn tracing_request_replay_emits_nothing() {
        use tracing_subscriber::layer::SubscriberExt;
        let events = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let _subscriber = tracing::subscriber::set_default(
            tracing_subscriber::registry().with(CountEvents(events.clone())),
        );
        let sink = TracingSink::new(|item: &Item| {
            TracingOutput::info("terminal item observed").field("value", item.0)
        });
        let request = sink.prepare(&Item(7)).unwrap();
        assert_eq!(
            events.load(std::sync::atomic::Ordering::SeqCst),
            0,
            "a replay builds the request and emits nothing"
        );
        Tracing.emit(&request).await.unwrap();
        assert_eq!(events.load(std::sync::atomic::Ordering::SeqCst), 1);
    }
}
