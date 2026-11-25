// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! FLOWIP-122a hard archive-epoch gate at the application boundary.

use obzenflow::stages::sources;
use obzenflow_adapters::middleware::MiddlewareAttachmentValidationError;
use obzenflow_core::TypedPayload;
use obzenflow_dsl::dsl::MiddlewarePlanError;
use obzenflow_dsl::{flow, sink, source, FlowBuildError, FlowDefinition};
use obzenflow_infra::journal::disk_journals;
use obzenflow_runtime::commands::{perform, Perform};
use obzenflow_runtime::effects::EffectError;
use obzenflow_runtime::systems::Port;
use serde::{Deserialize, Serialize};
use std::ffi::OsString;
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

#[derive(Clone, Debug, Serialize, Deserialize)]
struct Input(u64);

impl TypedPayload for Input {
    const EVENT_TYPE: &'static str = "flowip_122a.archive_gate.input";
}

#[derive(Clone, Debug, Serialize)]
struct Record;

#[obzenflow_runtime::effects::port]
trait ArchiveGatePort: Send + Sync {
    #[effect(type = "archive_gate.record", at_least_once, records = reply)]
    fn record(&self, request: &Record) -> Result<(), EffectError>;
}

struct ArchiveGateDestination;

impl ArchiveGatePort for ArchiveGateDestination {
    fn record(&self, _: &Record) -> Result<(), EffectError> {
        Ok(())
    }
}

fn counting_destination(opens: Arc<AtomicUsize>) -> Perform<fn(&Input) -> Record, Input, Record> {
    perform(record_input as fn(&Input) -> Record).through(Port::deferred(move || {
        opens.fetch_add(1, Ordering::SeqCst);
        Ok(ArchiveGateDestination)
    }))
}

fn record_input(_: &Input) -> Record {
    Record
}

fn guarded_flow(
    output_root: PathBuf,
    materialisations: Arc<AtomicUsize>,
    opens: Arc<AtomicUsize>,
) -> FlowDefinition {
    FlowDefinition::materialize(move |_runtime_config| {
        materialisations.fetch_add(1, Ordering::SeqCst);
        let inputs = sources::ValuesSource::new([Input(1)]);
        let output = counting_destination(opens);
        Ok(flow! {
            name: "archive_manifest_epoch_gate",
            journals: disk_journals(output_root),

            stages: {
                inputs = source!(Input => inputs);
                output = sink!(Input => output);
            },

            topology: {
                inputs |> output;
            }
        })
    })
}

#[tokio::test(flavor = "multi_thread")]
async fn stale_manifest_refused_before_materialization() {
    for verb in ["--replay-from", "--resume-from"] {
        for (name, manifest) in [
            ("missing", r#"{"flow_id":"old"}"#),
            ("numeric", r#"{"journal_schema_version":3.0}"#),
            ("old", r#"{"journal_schema_version":"4.0"}"#),
            ("previous", r#"{"journal_schema_version":"15.0"}"#),
            (
                "legacy-fields",
                r#"{"manifest_version":"4.0","journal_format_version":4}"#,
            ),
            ("future", r#"{"journal_schema_version":"999.0"}"#),
            ("object", r#"{"journal_schema_version":{"major":3}}"#),
            ("malformed", r#"{"journal_schema_version":"3.0""#),
        ] {
            let temp = tempfile::tempdir().expect("temporary archive gate directory");
            let archive = temp.path().join(format!("archive-{name}"));
            std::fs::create_dir_all(&archive).expect("archive directory");
            std::fs::write(archive.join("run_manifest.json"), manifest)
                .expect("raw manifest fixture");
            let output_root = temp.path().join("must-not-exist");
            let materialisations = Arc::new(AtomicUsize::new(0));
            let opens = Arc::new(AtomicUsize::new(0));
            let args = vec![
                OsString::from("obzenflow"),
                OsString::from(verb),
                archive.into_os_string(),
                OsString::from("--allow-incomplete-archive"),
            ];

            let result = crate::replay_testkit::unsynced_application()
                .with_cli_args(args)
                .run_async(guarded_flow(
                    output_root.clone(),
                    Arc::clone(&materialisations),
                    Arc::clone(&opens),
                ))
                .await;
            assert!(result.is_err(), "{name} manifest must be refused");
            assert_eq!(
                materialisations.load(Ordering::SeqCst),
                0,
                "{name} manifest reached flow materialisation"
            );
            assert_eq!(
                opens.load(Ordering::SeqCst),
                0,
                "{name} manifest resolved the sink's port"
            );
            assert!(
                !output_root.exists(),
                "{name} manifest created output journals"
            );
        }
    }
}

struct CountingObserverFactory(Arc<AtomicUsize>);
struct PollObserver;

impl obzenflow_runtime::stages::observer::SourcePollObserver for PollObserver {}

impl obzenflow_adapters::middleware::MiddlewareFactory for CountingObserverFactory {
    fn label(&self) -> &'static str {
        "preflight_poll_observer"
    }

    fn override_key(&self) -> obzenflow_adapters::middleware::MiddlewareOverrideKey {
        obzenflow_adapters::middleware::MiddlewareOverrideKey::of::<Self>(self.label())
    }

    fn declaration(&self) -> obzenflow_adapters::middleware::MiddlewareDeclaration {
        obzenflow_adapters::middleware::MiddlewareDeclaration::observer(
            self.label(),
            vec![obzenflow_adapters::middleware::MiddlewareSurfaceKind::SourcePoll],
        )
    }

    fn materialize(
        &self,
        _request: obzenflow_adapters::middleware::MiddlewareAttachmentRequest<'_>,
        _context: &obzenflow_adapters::middleware::MiddlewareMaterializationContext<'_>,
    ) -> obzenflow_adapters::middleware::MiddlewareFactoryResult<
        obzenflow_adapters::middleware::MiddlewareSurfaceAttachment,
    > {
        self.0.fetch_add(1, Ordering::SeqCst);
        Ok(
            obzenflow_adapters::middleware::MiddlewareSurfaceAttachment::source_poll_observer(
                Arc::new(PollObserver),
            ),
        )
    }
}

#[tokio::test]
async fn invalid_later_plan_prevents_materialization() {
    use obzenflow_adapters::middleware::circuit_breaker;

    let root = tempfile::tempdir().unwrap();
    let output_root = root.path().join("must-not-exist");
    let journal_root = output_root.clone();
    let materialisations = Arc::new(AtomicUsize::new(0));
    let opens = Arc::new(AtomicUsize::new(0));
    let observer = CountingObserverFactory(materialisations.clone());
    let output = counting_destination(opens.clone());
    let definition = FlowDefinition::materialize(move |_| {
        let inputs = sources::ValuesSource::new([Input(1)]);
        Ok(flow! {
            name: "whole_flow_middleware_preflight",
            journals: disk_journals(journal_root),
            stages: {
                inputs = source!(Input with observer => inputs);
                output = sink!(Input => output);
                invalid = sink!(Input with circuit_breaker()
                    .count_window(2).minimum_calls(3).failure_rate_threshold(0.5) => placeholder!());
            },
            topology: {
                inputs |> output;
                inputs |> invalid;
            }
        })
    });
    let failure = definition
        .build(crate::replay_testkit::unsynced_context())
        .await
        .map(|_| ())
        .expect_err("the complete plan must reject an invalid later breaker");
    assert!(
        matches!(failure.error, FlowBuildError::MiddlewarePlan(_)),
        "{failure}"
    );
    assert!(failure.to_string().contains("invalid"), "{failure}");
    assert_eq!(materialisations.load(Ordering::SeqCst), 0);
    assert_eq!(opens.load(Ordering::SeqCst), 0);
    assert!(failure.run.is_none());
    assert!(!output_root.exists());
}

#[tokio::test]
async fn empty_observer_label_fails_preflight() {
    use obzenflow_adapters::middleware::source_poll_observer;
    for label in ["", "  "] {
        let root = tempfile::tempdir().unwrap();
        let output_root = root.path().join("must-not-exist");
        let journal_root = output_root.clone();
        let materialisations = Arc::new(AtomicUsize::new(0));
        let opens = Arc::new(AtomicUsize::new(0));
        let observer = CountingObserverFactory(materialisations.clone());
        let output = counting_destination(opens.clone());
        let definition = FlowDefinition::materialize(move |_| {
            let inputs = sources::ValuesSource::new([Input(1)]);
            Ok(flow! {
                name: "empty_observer_label_preflight",
                journals: disk_journals(journal_root),
                stages: {
                    inputs = source!(Input with observer => inputs);
                    invalid = source!(Input with source_poll_observer(label, PollObserver) => placeholder!());
                    output = sink!(Input => output);
                },
                topology: {
                    inputs |> output;
                    invalid |> output;
                }
            })
        });
        let failure = definition
            .build(crate::replay_testkit::unsynced_context())
            .await
            .map(|_| ())
            .expect_err("empty labels must fail preflight");
        assert!(
            matches!(&failure.error,
                FlowBuildError::MiddlewarePlan(plan) if matches!(
                    plan.as_ref(),
                    MiddlewarePlanError::InvalidBinding {
                        source: MiddlewareAttachmentValidationError::EmptyLabel,
                        ..
                    }
                )
            ),
            "{failure}"
        );
        assert_eq!(materialisations.load(Ordering::SeqCst), 0);
        assert_eq!(opens.load(Ordering::SeqCst), 0);
        assert!(failure.run.is_none());
        assert!(!output_root.exists());
    }
}

#[tokio::test]
async fn claimed_hosted_slot_fails_preflight() {
    use obzenflow_adapters::middleware::rate_limit;
    use obzenflow_core::ingress::{FilledHostedIngress, HostedIngressBindingSlot};
    use obzenflow_dsl::source;
    use obzenflow_runtime::stages::common::handlers::HostedSource;

    for prefilled in [false, true] {
        let root = tempfile::tempdir().unwrap();
        let output_root = root.path().join("must-not-exist");
        let journal_root = output_root.clone();
        let materialisations = Arc::new(AtomicUsize::new(0));
        let opens = Arc::new(AtomicUsize::new(0));
        let observer = CountingObserverFactory(materialisations.clone());
        let output = counting_destination(opens.clone());
        let slot = HostedIngressBindingSlot::new("orders");
        let previous_stage = obzenflow_core::StageId::new();
        if prefilled {
            slot.fill(FilledHostedIngress {
                stage_id: previous_stage,
                stage_key: "previous".into(),
                boundary: None,
            })
            .unwrap();
        }
        let shared_slot = slot.clone();
        let definition = FlowDefinition::materialize(move |_| {
            let (_, first_rx) = obzenflow::events::source_channel(1);
            let (_, second_rx) = obzenflow::events::source_channel(1);
            let first = HostedSource::<Input>::ingress(first_rx, shared_slot.clone());
            let second = HostedSource::<Input>::ingress(second_rx, shared_slot);
            Ok(flow! {
                name: "hosted_slot_ownership_preflight",
                journals: disk_journals(journal_root),
                stages: {
                    left = source!(Input with { rate_limit(10.0), observer } => first);
                    right = source!(Input with rate_limit(10.0) => second);
                    output = sink!(Input => output);
                },
                topology: {
                    left |> output;
                    right |> output;
                }
            })
        });
        let failure = definition
            .build(crate::replay_testkit::unsynced_context())
            .await
            .map(|_| ())
            .expect_err("hosted ownership must fail before live build work");
        if prefilled {
            assert!(
                matches!(&failure.error,
                    FlowBuildError::HostedIngressAlreadyBound {
                        ingress_key, stage_name, bound_stage
                    } if ingress_key == "orders" && stage_name == "left" && bound_stage == "previous"
                ),
                "{failure}"
            );
            let previous = slot.filled().expect("the prior binding is retained");
            assert_eq!(previous.stage_id, previous_stage);
            assert_eq!(previous.stage_key.as_str(), "previous");
            assert!(previous.boundary.is_none());
        } else {
            assert!(
                matches!(&failure.error,
                    FlowBuildError::DuplicateHostedIngressBinding {
                        ingress_key, first_stage, second_stage
                    } if ingress_key == "orders" && first_stage == "left" && second_stage == "right"
                ),
                "{failure}"
            );
            assert!(
                !slot.is_filled(),
                "rejected ownership must not fill the slot"
            );
        }
        assert_eq!(materialisations.load(Ordering::SeqCst), 0);
        assert_eq!(opens.load(Ordering::SeqCst), 0);
        assert!(failure.run.is_none());
        assert!(!output_root.exists());
    }
}
