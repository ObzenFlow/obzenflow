// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Tests for the FLOWIP-120n F16 resume sink delivery-safety gate.

#[cfg(test)]
mod tests {
    use async_trait::async_trait;
    use obzenflow_core::event::payloads::delivery_payload::DeliveryPayload;
    use obzenflow_core::TypedPayload;
    use obzenflow_runtime::effects::{Effects, SinkDeliverySafety};
    use obzenflow_runtime::stages::common::handler_error::HandlerError;
    use obzenflow_runtime::stages::common::handlers::EffectfulSinkHandler;
    use obzenflow_runtime::stages::sink::SinkTyped;
    use serde::{Deserialize, Serialize};
    use std::collections::HashMap;

    use crate::dsl::error::FlowBuildError;
    use crate::dsl::stage_descriptor::{EffectfulSinkDescriptor, SinkDescriptor, StageDescriptor};
    use crate::dsl::typing::validate_resume_sink_delivery_safety;

    #[derive(Clone, Debug, Serialize, Deserialize)]
    struct SinkInput {
        value: String,
    }

    impl TypedPayload for SinkInput {
        const EVENT_TYPE: &'static str = "test.sink_input";
        const SCHEMA_VERSION: u32 = 1;
    }

    fn sink_descriptor(
        name: &str,
        delivery_safety: Option<SinkDeliverySafety>,
    ) -> Box<dyn StageDescriptor> {
        let handler = SinkTyped::new(|_value: SinkInput| async move {});
        let handler = match delivery_safety {
            Some(SinkDeliverySafety::IdempotentProjection) => handler.idempotent(),
            Some(SinkDeliverySafety::NonIdempotentExternal) => handler.non_idempotent(),
            None => handler,
        };
        Box::new(SinkDescriptor {
            name: name.to_string(),
            handler,
            middleware: vec![],
        })
    }

    #[derive(Clone, Debug)]
    struct EffectfulSink;

    #[async_trait]
    impl EffectfulSinkHandler for EffectfulSink {
        type Input = SinkInput;

        async fn consume(
            &mut self,
            _input: Self::Input,
            _fx: &mut Effects,
        ) -> Result<DeliveryPayload, HandlerError> {
            unreachable!("gate tests never run the handler")
        }
    }

    fn descriptors(
        entries: Vec<(&str, Box<dyn StageDescriptor>)>,
    ) -> HashMap<String, Box<dyn StageDescriptor>> {
        entries
            .into_iter()
            .map(|(name, descriptor)| (name.to_string(), descriptor))
            .collect()
    }

    #[test]
    fn declared_idempotent_sink_passes_under_resume() {
        let descriptors = descriptors(vec![(
            "projection",
            sink_descriptor("projection", Some(SinkDeliverySafety::IdempotentProjection)),
        )]);

        validate_resume_sink_delivery_safety(&descriptors, false)
            .expect("idempotent projection sinks resume cleanly");
    }

    #[test]
    fn declared_non_idempotent_sink_refuses_resume() {
        let descriptors = descriptors(vec![(
            "external_writer",
            sink_descriptor(
                "external_writer",
                Some(SinkDeliverySafety::NonIdempotentExternal),
            ),
        )]);

        let err = validate_resume_sink_delivery_safety(&descriptors, false)
            .expect_err("non-idempotent sinks refuse resume without the opt-in");
        match err {
            FlowBuildError::ResumeRefusedNonIdempotentSink { stage } => {
                assert_eq!(stage, "external_writer");
            }
            other => panic!("unexpected error variant: {other:?}"),
        }
    }

    #[test]
    fn undeclared_sink_refuses_resume() {
        let descriptors = descriptors(vec![("mystery", sink_descriptor("mystery", None))]);

        let err = validate_resume_sink_delivery_safety(&descriptors, false)
            .expect_err("undeclared sinks fail closed under resume");
        match err {
            FlowBuildError::ResumeRefusedUndeclaredSink { stage } => {
                assert_eq!(stage, "mystery");
            }
            other => panic!("unexpected error variant: {other:?}"),
        }
    }

    #[test]
    fn opt_in_lets_both_refusal_classes_through() {
        let descriptors = descriptors(vec![
            (
                "external_writer",
                sink_descriptor(
                    "external_writer",
                    Some(SinkDeliverySafety::NonIdempotentExternal),
                ),
            ),
            ("mystery", sink_descriptor("mystery", None)),
        ]);

        validate_resume_sink_delivery_safety(&descriptors, true)
            .expect("allow_duplicate_sink_delivery accepts both refusal classes");
    }

    #[test]
    fn effectful_sink_passes_structurally_without_a_declaration() {
        let descriptor: Box<dyn StageDescriptor> = Box::new(EffectfulSinkDescriptor {
            name: "effectful".to_string(),
            handler: EffectfulSink,
            effects: vec![],
            middleware: vec![],
        });
        assert!(descriptor.is_effectful());
        assert_eq!(descriptor.sink_delivery_safety(), None);

        let descriptors = descriptors(vec![("effectful", descriptor)]);
        validate_resume_sink_delivery_safety(&descriptors, false)
            .expect("effectful sinks pass the gate structurally");
    }

    #[test]
    fn non_sink_stages_are_ignored_by_the_gate() {
        let transform = crate::transform!(name: "shaper", SinkInput -> SinkInput => placeholder!());
        assert_eq!(transform.sink_delivery_safety(), None);

        let descriptors = descriptors(vec![("shaper", transform)]);
        validate_resume_sink_delivery_safety(&descriptors, false)
            .expect("the gate classifies sink stages only");
    }

    #[test]
    fn sink_macro_delivery_clause_snapshots_through_the_typed_wrapper() {
        let idempotent = crate::sink!(
            name: "declared_idempotent",
            SinkInput => SinkTyped::new(|_value: SinkInput| async move {}),
            delivery: idempotent
        );
        assert_eq!(
            idempotent.sink_delivery_safety(),
            Some(SinkDeliverySafety::IdempotentProjection)
        );

        let non_idempotent = crate::sink!(
            name: "declared_non_idempotent",
            SinkInput => SinkTyped::new(|_value: SinkInput| async move {}),
            delivery: non_idempotent
        );
        assert_eq!(
            non_idempotent.sink_delivery_safety(),
            Some(SinkDeliverySafety::NonIdempotentExternal)
        );

        let closure_form = crate::sink!(
            name: "closure_declared",
            |_value: SinkInput| {},
            delivery: idempotent
        );
        assert_eq!(
            closure_form.sink_delivery_safety(),
            Some(SinkDeliverySafety::IdempotentProjection)
        );

        let undeclared = crate::sink!(
            name: "undeclared",
            SinkInput => SinkTyped::new(|_value: SinkInput| async move {})
        );
        assert_eq!(undeclared.sink_delivery_safety(), None);
    }
}
