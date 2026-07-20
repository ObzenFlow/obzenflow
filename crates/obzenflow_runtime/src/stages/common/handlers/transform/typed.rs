// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Typed pure transform handlers (FLOWIP-120z).

use super::traits::TransformHandler;
use crate::stages::common::handler_error::HandlerError;
use crate::typing::TransformTyping;
use async_trait::async_trait;
use obzenflow_core::event::schema::{StageOutputFacts, TypedFactSet, TypedPayload};
use obzenflow_core::event::ChainEventFactory;
use obzenflow_core::ChainEvent;

/// Pure typed transform surface whose returned carrier is the handler's
/// exhaustive output contract.
pub trait TypedTransformHandler: Send + Sync {
    type Input: TypedPayload + Send + Sync + 'static;
    type Output: StageOutputFacts;

    fn process(&self, input: Self::Input) -> Result<Self::Output, HandlerError>;
}

/// Adapter lowering a typed output carrier through the ordinary transform
/// pending-output path. It never commits directly.
#[doc(hidden)]
#[derive(Clone, Debug)]
pub struct TypedTransformHandlerAdapter<H> {
    handler: H,
    lineage: obzenflow_core::config::LineagePolicy,
}

impl<H> TypedTransformHandlerAdapter<H> {
    pub fn new(handler: H) -> Self {
        Self {
            handler,
            lineage: obzenflow_core::config::LineagePolicy::default(),
        }
    }
}

impl<H> TransformTyping for TypedTransformHandlerAdapter<H>
where
    H: TypedTransformHandler,
{
    type Input = H::Input;
    type Output = H::Output;
}

#[async_trait]
impl<H> TransformHandler for TypedTransformHandlerAdapter<H>
where
    H: TypedTransformHandler + Send + Sync,
{
    fn process(&self, event: ChainEvent) -> Result<Vec<ChainEvent>, HandlerError> {
        let input = H::Input::try_from_event(&event)
            .map_err(|error| HandlerError::Deserialization(error.to_string()))?;
        let output = self.handler.process(input)?;
        let facts = output.into_facts().map_err(|error| {
            HandlerError::Other(format!(
                "typed transform output serialization failed: {error}"
            ))
        })?;

        Ok(facts
            .into_iter()
            .map(|fact| {
                ChainEventFactory::derived_data_event(
                    event.writer_id,
                    &event,
                    fact.event_type,
                    fact.payload,
                    self.lineage,
                )
            })
            .collect())
    }

    async fn drain(&mut self) -> Result<(), HandlerError> {
        Ok(())
    }

    fn install_lineage_policy(&mut self, policy: obzenflow_core::config::LineagePolicy) {
        self.lineage = policy;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use obzenflow_core::{StageId, StageOutputFacts, TypedPayload, WriterId};
    use serde::{Deserialize, Serialize};

    #[derive(Clone, Debug, Serialize, Deserialize)]
    struct Input {
        value: u32,
    }

    impl TypedPayload for Input {
        const EVENT_TYPE: &'static str = "typed_transform.input";
    }

    #[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
    struct First {
        value: u32,
    }

    impl TypedPayload for First {
        const EVENT_TYPE: &'static str = "typed_transform.first";
    }

    #[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
    struct Second {
        value: u32,
    }

    impl TypedPayload for Second {
        const EVENT_TYPE: &'static str = "typed_transform.second";
    }

    #[derive(Clone, Debug, StageOutputFacts)]
    enum Output {
        Both { first: First, second: Second },
    }

    #[derive(Clone, Debug)]
    struct Classifier;

    impl TypedTransformHandler for Classifier {
        type Input = Input;
        type Output = Output;

        fn process(&self, input: Input) -> Result<Self::Output, HandlerError> {
            Ok(Output::Both {
                first: First { value: input.value },
                second: Second {
                    value: input.value + 1,
                },
            })
        }
    }

    #[test]
    fn adapter_lowers_carrier_fields_in_order_as_derived_events() {
        let writer_id = WriterId::from(StageId::new());
        let parent = ChainEventFactory::data_event(
            writer_id,
            Input::versioned_event_type(),
            serde_json::json!(Input { value: 7 }),
        );
        let adapter = TypedTransformHandlerAdapter::new(Classifier);

        let outputs = TransformHandler::process(&adapter, parent.clone()).expect("classifies");

        assert_eq!(outputs.len(), 2);
        assert_eq!(outputs[0].event_type(), First::versioned_event_type());
        assert_eq!(outputs[1].event_type(), Second::versioned_event_type());
        assert_eq!(First::from_event(&outputs[0]), Some(First { value: 7 }));
        assert_eq!(Second::from_event(&outputs[1]), Some(Second { value: 8 }));
        assert!(outputs
            .iter()
            .all(|event| event.causality.parent_ids.first() == Some(&parent.id)));
    }
}
