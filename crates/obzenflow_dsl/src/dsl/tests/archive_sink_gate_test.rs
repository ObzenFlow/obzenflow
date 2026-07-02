// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Tests for the archive sink delivery-safety gate: FLOWIP-120n F16, extended
//! to both archive verbs by FLOWIP-120v.

#[cfg(test)]
mod tests {
    use obzenflow_core::TypedPayload;
    use obzenflow_runtime::bootstrap::ReplayVerb;
    use obzenflow_runtime::effects::SinkDeliverySafety;
    use obzenflow_runtime::stages::sink::SinkTyped;
    use serde::{Deserialize, Serialize};
    use std::collections::HashMap;

    use crate::dsl::error::FlowBuildError;
    use crate::dsl::stage_descriptor::{SinkDescriptor, StageDescriptor};
    use crate::dsl::typing::validate_archive_sink_delivery_safety;

    const BOTH_VERBS: [ReplayVerb; 2] = [ReplayVerb::Resume, ReplayVerb::Replay];

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

    fn descriptors(
        entries: Vec<(&str, Box<dyn StageDescriptor>)>,
    ) -> HashMap<String, Box<dyn StageDescriptor>> {
        entries
            .into_iter()
            .map(|(name, descriptor)| (name.to_string(), descriptor))
            .collect()
    }

    #[test]
    fn declared_idempotent_sink_passes_under_both_verbs() {
        for verb in BOTH_VERBS {
            let descriptors = descriptors(vec![(
                "projection",
                sink_descriptor("projection", Some(SinkDeliverySafety::IdempotentProjection)),
            )]);

            validate_archive_sink_delivery_safety(&descriptors, verb, false)
                .expect("idempotent projection sinks pass under both archive verbs");
        }
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

        let err = validate_archive_sink_delivery_safety(&descriptors, ReplayVerb::Resume, false)
            .expect_err("non-idempotent sinks refuse resume without the opt-in");
        let message = err.to_string();
        match err {
            FlowBuildError::ArchiveRefusedNonIdempotentSink { stage, verb } => {
                assert_eq!(stage, "external_writer");
                assert_eq!(verb, ReplayVerb::Resume);
            }
            other => panic!("unexpected error variant: {other:?}"),
        }
        assert!(message.starts_with("--resume-from refused"), "{message}");
        assert!(
            message.contains("re-delivers the recorded prefix"),
            "{message}"
        );
    }

    #[test]
    fn declared_non_idempotent_sink_refuses_replay() {
        let descriptors = descriptors(vec![(
            "external_writer",
            sink_descriptor(
                "external_writer",
                Some(SinkDeliverySafety::NonIdempotentExternal),
            ),
        )]);

        let err = validate_archive_sink_delivery_safety(&descriptors, ReplayVerb::Replay, false)
            .expect_err("non-idempotent sinks refuse replay without the opt-in");
        let message = err.to_string();
        match err {
            FlowBuildError::ArchiveRefusedNonIdempotentSink { stage, verb } => {
                assert_eq!(stage, "external_writer");
                assert_eq!(verb, ReplayVerb::Replay);
            }
            other => panic!("unexpected error variant: {other:?}"),
        }
        assert!(message.starts_with("--replay-from refused"), "{message}");
        assert!(
            message.contains("re-consumes the recorded stream"),
            "{message}"
        );
        // The steer names the canonical home for a non-idempotent write.
        assert!(
            message.contains("effectful transform plus plain sink"),
            "{message}"
        );
    }

    #[test]
    fn undeclared_sink_refuses_resume() {
        let descriptors = descriptors(vec![("mystery", sink_descriptor("mystery", None))]);

        let err = validate_archive_sink_delivery_safety(&descriptors, ReplayVerb::Resume, false)
            .expect_err("undeclared sinks fail closed under resume");
        match err {
            FlowBuildError::ArchiveRefusedUndeclaredSink { stage, verb } => {
                assert_eq!(stage, "mystery");
                assert_eq!(verb, ReplayVerb::Resume);
            }
            other => panic!("unexpected error variant: {other:?}"),
        }
    }

    #[test]
    fn undeclared_sink_refuses_replay() {
        let descriptors = descriptors(vec![("mystery", sink_descriptor("mystery", None))]);

        let err = validate_archive_sink_delivery_safety(&descriptors, ReplayVerb::Replay, false)
            .expect_err("undeclared sinks fail closed under replay");
        let message = err.to_string();
        match err {
            FlowBuildError::ArchiveRefusedUndeclaredSink { stage, verb } => {
                assert_eq!(stage, "mystery");
                assert_eq!(verb, ReplayVerb::Replay);
            }
            other => panic!("unexpected error variant: {other:?}"),
        }
        // The refusal names all four declaration homes.
        assert!(
            message.contains("`delivery: idempotent` on a sink! closure"),
            "{message}"
        );
        assert!(
            message.contains("`SinkHandler::delivery_safety()`"),
            "{message}"
        );
        assert!(
            message.contains("`SAFETY` on a typed `Delivery`"),
            "{message}"
        );
        assert!(message.contains("`.idempotent()`"), "{message}");
    }

    #[test]
    fn opt_in_lets_both_refusal_classes_through_under_both_verbs() {
        for verb in BOTH_VERBS {
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

            validate_archive_sink_delivery_safety(&descriptors, verb, true)
                .expect("allow_duplicate_sink_delivery accepts both refusal classes");
        }
    }

    #[test]
    fn non_sink_stages_are_ignored_by_the_gate() {
        for verb in BOTH_VERBS {
            let transform =
                crate::transform!(name: "shaper", SinkInput -> SinkInput => placeholder!());
            assert_eq!(transform.sink_delivery_safety(), None);

            let descriptors = descriptors(vec![("shaper", transform)]);
            validate_archive_sink_delivery_safety(&descriptors, verb, false)
                .expect("the gate classifies sink stages only");
        }
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
