// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Integration tests for FLOWIP-120u: the run substrate declaration on
//! `FlowJournalFactory`, the `FlowBuildFailure` carrier on the build's failure
//! arm, durable-gated manifest persistence, and the provider-owned preflight.

use obzenflow_adapters::middleware::{
    validate_attachment_request, MiddlewareAttachmentRequest, MiddlewareDeclaration,
    MiddlewareFactory, MiddlewareFactoryError, MiddlewareFactoryResult,
    MiddlewareMaterializationContext, MiddlewareOverrideKey, MiddlewareSafety,
    MiddlewareSurfaceAttachment, MiddlewareSurfaceKind,
};
use obzenflow_core::journal::factory::{FlowJournalFactory, RunResourcePlan, RunSubstrateState};
use obzenflow_core::{FlowId, TypedPayload};
use obzenflow_dsl::{flow, sink, source, FlowDefinition};
use obzenflow_infra::journal::{
    disk_journals, memory_journals, DiskJournalFactory, MemoryJournalFactory,
};
use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

#[derive(Clone, Debug, Serialize, Deserialize)]
struct TestEvent;
impl TypedPayload for TestEvent {
    const EVENT_TYPE: &'static str = "test.run_substrate";
    const SCHEMA_VERSION: u32 = 1;
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct OtherEvent;
impl TypedPayload for OtherEvent {
    const EVENT_TYPE: &'static str = "test.run_substrate.other";
    const SCHEMA_VERSION: u32 = 1;
}

#[test]
fn factories_declare_their_substrate() {
    let base = tempfile::tempdir().expect("tempdir");
    let flow_id = FlowId::new();
    let disk = DiskJournalFactory::new(base.path().to_path_buf(), flow_id)
        .expect("disk factory should build");
    match disk.run_state() {
        RunSubstrateState::Durable(locator) => {
            assert_eq!(
                locator.path(),
                base.path().join("flows").join(flow_id.to_string())
            );
        }
        RunSubstrateState::Ephemeral => panic!("disk factory must declare Durable"),
    }

    let memory = MemoryJournalFactory::new(FlowId::new());
    assert!(matches!(memory.run_state(), RunSubstrateState::Ephemeral));
}

#[test]
fn provider_preflight_accepts_small_plans() {
    let base = tempfile::tempdir().expect("tempdir");
    let disk = DiskJournalFactory::new(base.path().to_path_buf(), FlowId::new())
        .expect("disk factory should build");
    let plan = RunResourcePlan {
        stage_count: 2,
        edge_count: 1,
        metrics_enabled: false,
    };
    disk.resource_preflight(&plan)
        .expect("small plan must pass the disk FD preflight");

    // Memory inherits the accepting default: no location, no resource shape.
    let memory = MemoryJournalFactory::new(FlowId::new());
    memory
        .resource_preflight(&plan)
        .expect("memory preflight is the accepting default");
}

/// A build that fails before substrate selection (edge typing is validated
/// before the factory exists) carries no run state: nothing was created on
/// disk, so there is nothing to point the operator at.
#[tokio::test]
async fn pre_substrate_failure_carries_no_run_state() {
    let built = FlowDefinition::materialize(move |_runtime_config| {
        Ok(flow! {
            name: "pre_substrate_failure",
            journals: memory_journals(),

            stages: {
                src = source!(TestEvent => placeholder!());
                snk = sink!(OtherEvent => placeholder!());
            },

            topology: {
                src |> snk;
            }
        })
    })
    .build(obzenflow_runtime::run_context::FlowBuildContext::for_tests())
    .await;

    let failure = built
        .err()
        .expect("edge typing mismatch must fail the build");
    assert!(
        failure.run.is_none(),
        "pre-substrate failures carry no run state: {failure:?}"
    );
    assert!(matches!(
        failure.error,
        obzenflow_dsl::dsl::FlowBuildError::EdgeTypingMismatch { .. }
    ));
}

// This declaration is valid through configuration and topology planning. Its
// runtime resource fails only when the source observer is materialised, after
// the journal factory has selected the substrate and created the run directory.
const RESOURCE_FAILURE: &str = "observer resource unavailable during materialisation";

struct FailingObserverFactory {
    label: &'static str,
    materialisations: Arc<AtomicUsize>,
}

impl MiddlewareFactory for FailingObserverFactory {
    fn label(&self) -> &'static str {
        self.label
    }

    fn override_key(&self) -> MiddlewareOverrideKey {
        MiddlewareOverrideKey::of::<Self>(self.label)
    }

    fn declaration(&self) -> MiddlewareDeclaration {
        MiddlewareDeclaration::observer(self.label, vec![MiddlewareSurfaceKind::StageLifecycle])
    }

    fn materialize(
        &self,
        request: MiddlewareAttachmentRequest<'_>,
        context: &MiddlewareMaterializationContext<'_>,
    ) -> MiddlewareFactoryResult<MiddlewareSurfaceAttachment> {
        validate_attachment_request(&self.declaration(), &request).map_err(|err| {
            MiddlewareFactoryError::materialization_failed(self.label(), &context.config.name, err)
        })?;
        self.materialisations.fetch_add(1, Ordering::SeqCst);
        Err(MiddlewareFactoryError::materialization_failed(
            self.label(),
            &context.config.name,
            std::io::Error::other(RESOURCE_FAILURE),
        ))
    }

    fn safety_level(&self) -> MiddlewareSafety {
        MiddlewareSafety::Safe
    }
}

fn failing_materialization_flow(
    name: &'static str,
    base: std::path::PathBuf,
    materialisations: Arc<AtomicUsize>,
) -> obzenflow_dsl::FlowDefinition {
    FlowDefinition::materialize(move |_runtime_config| {
        Ok(flow! {
            name: "post_substrate_failure",
            journals: disk_journals(base),

            stages: {
                src = source!(TestEvent => placeholder!() with FailingObserverFactory {
                    label: name,
                    materialisations,
                });
                snk = sink!(TestEvent => placeholder!());
            },

            topology: {
                src |> snk;
            }
        })
    })
}

fn assert_materialization_failure(
    failure: &obzenflow_dsl::FlowBuildFailure,
    materialisations: &AtomicUsize,
    label: &str,
) {
    assert_eq!(
        materialisations.load(Ordering::SeqCst),
        1,
        "the valid plan must reach runtime materialisation exactly once"
    );
    assert!(
        matches!(
            &failure.error,
            obzenflow_dsl::dsl::FlowBuildError::StageCreationFailed { stage_name, .. }
                if stage_name == "src"
        ),
        "the failure must occur during stage creation: {failure:?}"
    );
    let message = failure.to_string();
    assert!(message.contains(RESOURCE_FAILURE), "{message}");
    assert!(message.contains(label), "{message}");
}

/// A build that fails after substrate selection carries the durable locator, so
/// the failure footer can still name the partial journals on disk.
#[tokio::test]
async fn post_substrate_failure_carries_the_durable_locator() {
    let base = tempfile::tempdir().expect("tempdir");
    let materialisations = Arc::new(AtomicUsize::new(0));
    let failure = failing_materialization_flow(
        "resource.a",
        base.path().to_path_buf(),
        materialisations.clone(),
    )
    .build(obzenflow_runtime::run_context::FlowBuildContext::for_tests())
    .await
    .err()
    .expect("runtime observer resource must fail the build");
    assert_materialization_failure(&failure, &materialisations, "resource.a");

    match failure.run.as_ref().and_then(|s| s.locator()) {
        Some(locator) => {
            assert!(
                locator.path().starts_with(base.path()),
                "locator must live under this run's base: {locator}"
            );
            assert!(
                locator.path().is_dir(),
                "the partial run directory must exist for the operator: {locator}"
            );
        }
        None => panic!("post-substrate failure must carry the durable locator: {failure:?}"),
    }
}

/// Two flows built concurrently in one process carry independent failure-path
/// state (the FLOWIP-120u F7 clobber, impossible now that the state rides the
/// build result instead of a process global).
#[tokio::test]
async fn concurrent_failing_builds_carry_independent_run_state() {
    let base_a = tempfile::tempdir().expect("tempdir a");
    let base_b = tempfile::tempdir().expect("tempdir b");

    let materialisations_a = Arc::new(AtomicUsize::new(0));
    let materialisations_b = Arc::new(AtomicUsize::new(0));
    let (a, b) = tokio::join!(
        failing_materialization_flow(
            "resource.a",
            base_a.path().to_path_buf(),
            materialisations_a.clone()
        )
        .build(obzenflow_runtime::run_context::FlowBuildContext::for_tests()),
        failing_materialization_flow(
            "resource.b",
            base_b.path().to_path_buf(),
            materialisations_b.clone()
        )
        .build(obzenflow_runtime::run_context::FlowBuildContext::for_tests()),
    );

    let failure_a = a.err().expect("build a must fail materialisation");
    let failure_b = b.err().expect("build b must fail materialisation");
    assert_materialization_failure(&failure_a, &materialisations_a, "resource.a");
    assert_materialization_failure(&failure_b, &materialisations_b, "resource.b");
    let locator_a = failure_a
        .run
        .as_ref()
        .and_then(|state| state.locator())
        .expect("build a must carry its locator");
    let locator_b = failure_b
        .run
        .as_ref()
        .and_then(|state| state.locator())
        .expect("build b must carry its locator");

    assert!(locator_a.path().starts_with(base_a.path()));
    assert!(locator_b.path().starts_with(base_b.path()));
    assert!(locator_a.path().is_dir());
    assert!(locator_b.path().is_dir());
    assert_ne!(locator_a.path(), locator_b.path());
}

/// A successful disk build reports Durable, and the manifest is persisted
/// within the run's location (the durable-gated write ran).
#[tokio::test]
async fn successful_disk_flow_reports_durable_and_persists_the_manifest() {
    let base = tempfile::tempdir().expect("tempdir");
    let base_path = base.path().to_path_buf();
    let handle = FlowDefinition::materialize(move |_runtime_config| {
        let journals = disk_journals(base_path);
        Ok(flow! {
            name: "durable_success",
            journals: journals,

            stages: {
                src = source!(TestEvent => placeholder!());
                snk = sink!(TestEvent => placeholder!());
            },

            topology: {
                src |> snk;
            }
        })
    })
    .build(obzenflow_runtime::run_context::FlowBuildContext::for_tests())
    .await
    .expect("flow must build");

    let locator = match handle.run_substrate() {
        RunSubstrateState::Durable(locator) => locator.clone(),
        RunSubstrateState::Ephemeral => panic!("disk flow must report Durable"),
    };
    assert!(
        locator.path().join("run_manifest.json").is_file(),
        "durable runs persist the manifest within their location: {locator}"
    );

    tokio::time::timeout(std::time::Duration::from_secs(30), handle.run())
        .await
        .expect("empty finite flow must complete promptly")
        .expect("run should succeed");
}

/// A successful memory build reports Ephemeral: no location exists anywhere.
#[tokio::test]
async fn successful_memory_flow_reports_ephemeral() {
    let handle = FlowDefinition::materialize(move |_runtime_config| {
        Ok(flow! {
            name: "ephemeral_success",
            journals: memory_journals(),

            stages: {
                src = source!(TestEvent => placeholder!());
                snk = sink!(TestEvent => placeholder!());
            },

            topology: {
                src |> snk;
            }
        })
    })
    .build(obzenflow_runtime::run_context::FlowBuildContext::for_tests())
    .await
    .expect("flow must build");

    assert!(matches!(
        handle.run_substrate(),
        RunSubstrateState::Ephemeral
    ));

    tokio::time::timeout(std::time::Duration::from_secs(30), handle.run())
        .await
        .expect("empty finite flow must complete promptly")
        .expect("run should succeed");
}
