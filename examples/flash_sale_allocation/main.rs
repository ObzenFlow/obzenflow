// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Flash-sale allocation: an effectful-stateful Process Manager witness.
//!
//! Run live, then replay the emitted run directory with verification:
//!
//! ```text
//! cargo run -p obzenflow --example flash_sale_allocation
//! cargo run -p obzenflow --example flash_sale_allocation -- \
//!   --replay-from target/flash-sale-allocation-logs/flows/<flow_id> --verify
//! ```
//!
//! The simulated warehouse comes from `[app.warehouse]` in `obzenflow.toml`.
//! Its id is binding evidence, so replaying under another id refuses.

mod allocation;
mod domain;
mod flow;
mod warehouse;

use obzenflow::application::{FlowApplication, LogLevel};

fn main() -> std::process::ExitCode {
    let config_file = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/examples/flash_sale_allocation/obzenflow.toml"
    );
    match FlowApplication::builder()
        .with_log_level(LogLevel::Info)
        .with_config_file(config_file)
        .run_blocking(flow::build_flow())
    {
        Ok(()) => std::process::ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("{error}");
            std::process::ExitCode::from(error.process_exit_code())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::domain::{
        AllocationInput, CancelIgnored, OrderId, OrderPlaced, ReservationFailed, Sku, SoldOut,
        StockReleased, StockReserved,
    };
    use super::warehouse::{WarehouseConfig, WarehouseTestFault};
    use super::{flow, warehouse};
    use obzenflow::application::FlowApplication;
    use obzenflow::application::{verify_run_dirs, VerifyOptions, VerifyOutcome};
    use obzenflow::journal::DiskJournal;
    use obzenflow::journal::Journal;
    use obzenflow::journal::JournalOwner;
    use obzenflow::journal::ProcessingStatus;
    use obzenflow::journal::StageFatalRecorded;
    use obzenflow::journal::{EffectOutcomePayload, EffectRecord};
    use obzenflow::schema::ChainEvent;
    use obzenflow::schema::{StageId, TypedPayload};
    use std::ffi::OsString;
    use std::path::{Path, PathBuf};
    use std::sync::Arc;

    fn latest_run_dir(base: &Path) -> PathBuf {
        let mut runs = std::fs::read_dir(base.join("flows"))
            .expect("flows directory")
            .map(|entry| entry.expect("flow directory entry").path())
            .filter(|path| path.join("run_manifest.json").exists())
            .collect::<Vec<_>>();
        runs.sort();
        runs.pop().expect("flow produced an archive")
    }

    fn archive_manifest(run_dir: &Path) -> serde_json::Value {
        serde_json::from_str(
            &std::fs::read_to_string(run_dir.join("run_manifest.json"))
                .expect("manifest is readable"),
        )
        .expect("manifest parses")
    }

    async fn allocation_journal_events(run_dir: &Path, manifest_field: &str) -> Vec<ChainEvent> {
        let manifest = archive_manifest(run_dir);
        let journal_file = manifest["stages"]["allocate_stock"][manifest_field]
            .as_str()
            .expect("allocator data journal");
        let journal = DiskJournal::<ChainEvent>::with_owner(
            run_dir.join(journal_file),
            JournalOwner::stage(StageId::new()),
        )
        .expect("allocator journal opens");
        journal
            .read_causally_ordered()
            .await
            .expect("allocator journal reads")
            .into_iter()
            .map(|record| record.into_authored())
            .collect()
    }

    async fn allocation_events(run_dir: &Path) -> Vec<ChainEvent> {
        allocation_journal_events(run_dir, "data_journal_file").await
    }

    async fn allocation_error_events(run_dir: &Path) -> Vec<ChainEvent> {
        allocation_journal_events(run_dir, "error_journal_file").await
    }

    fn count<T: TypedPayload>(events: &[ChainEvent]) -> usize {
        events
            .iter()
            .filter(|event| event.event_type() == T::event_type_name())
            .count()
    }

    async fn run_with_config(
        journal_root: PathBuf,
        replay_from: Option<&Path>,
        verify: bool,
        inputs: Vec<AllocationInput>,
        warehouse_config: WarehouseConfig,
        stats: Arc<warehouse::WarehouseStats>,
    ) -> Result<(), String> {
        let mut args = vec![OsString::from("flash_sale_allocation")];
        if let Some(archive) = replay_from {
            args.push(OsString::from("--replay-from"));
            args.push(archive.as_os_str().to_os_string());
            if verify {
                args.push(OsString::from("--verify"));
            }
        }
        FlowApplication::builder()
            .with_log_level(super::LogLevel::Error)
            .with_cli_args(args)
            .run_async(flow::assemble_flow(
                inputs,
                warehouse_config,
                stats,
                journal_root,
            ))
            .await
            .map_err(|error| error.to_string())
    }

    async fn run(
        journal_root: PathBuf,
        replay_from: Option<&Path>,
        stats: Arc<warehouse::WarehouseStats>,
    ) {
        run_with_config(
            journal_root,
            replay_from,
            true,
            flow::scripted_inputs(),
            WarehouseConfig::default(),
            stats,
        )
        .await
        .expect("allocation flow completes");
    }

    fn one_order() -> Vec<AllocationInput> {
        vec![AllocationInput::OrderPlaced(OrderPlaced {
            order_id: OrderId::from("failure-order"),
            sku: Sku::from("flash-sku"),
        })]
    }

    async fn propagated_failure_messages(run_dir: &Path) -> Vec<String> {
        let mut messages = Vec::new();
        for event in allocation_events(run_dir)
            .await
            .into_iter()
            .chain(allocation_error_events(run_dir).await)
        {
            if let ProcessingStatus::Error { message, .. } = &event.processing.status {
                messages.push(message.clone());
            }
            if let Some(fatal) = StageFatalRecorded::from_event(&event) {
                messages.push(fatal.detail);
            }
        }
        messages
    }

    async fn assert_propagated_without_reservation_failure(
        run_dir: &Path,
        expected_fragment: &str,
    ) {
        let events = allocation_events(run_dir).await;
        assert_eq!(
            count::<ReservationFailed>(&events),
            0,
            "non-policy failures must not become ReservationFailed"
        );
        let messages = propagated_failure_messages(run_dir).await;
        assert!(
            messages
                .iter()
                .any(|message| message.contains(expected_fragment)),
            "expected propagated failure containing {expected_fragment:?}, got {messages:?}"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn journal_is_the_oracle_and_strict_replay_calls_no_warehouse_port() {
        let temp = tempfile::tempdir().expect("tempdir");
        let journal_root = temp.path().join("journals");
        let live_stats = Arc::new(warehouse::WarehouseStats::default());

        run(journal_root.clone(), None, live_stats.clone()).await;
        assert_eq!(live_stats.reserve_calls(), 1);
        assert_eq!(
            live_stats.release_calls(),
            1,
            "release remains admitted while the reserve-only breaker is open"
        );

        let baseline = latest_run_dir(&journal_root);
        let events = allocation_events(&baseline).await;
        assert_eq!(count::<StockReserved>(&events), 1);
        assert_eq!(count::<StockReleased>(&events), 1);
        assert_eq!(count::<ReservationFailed>(&events), 2);
        assert_eq!(count::<SoldOut>(&events), 1);
        assert_eq!(count::<CancelIgnored>(&events), 1);

        // A boundary policy's refusal records the effect as rejected.
        let policy_rejections = events
            .iter()
            .filter(|event| event.event_type() == "effect.execution_rejected")
            .map(|event| {
                serde_json::from_value::<EffectRecord>(
                    event.payload.contract_body().expect("effect record body"),
                )
                .expect("valid effect record")
            })
            .filter(|record| {
                matches!(
                    &record.outcome,
                    EffectOutcomePayload::Failed {
                        cause: Some(cause),
                        ..
                    } if cause.source.as_str() == "circuit_breaker"
                        && cause.code.as_str() == "circuit_open"
                )
            })
            .count();
        assert_eq!(policy_rejections, 2);

        let mut allocated = 0_i32;
        for event in &events {
            if StockReserved::from_event(event).is_some() {
                allocated += 1;
            } else if StockReleased::from_event(event).is_some() {
                allocated -= 1;
            }
            assert!((0..=1).contains(&allocated));
        }
        assert_eq!(allocated, 0);

        let manifest_text =
            std::fs::read_to_string(baseline.join("run_manifest.json")).expect("manifest");
        assert!(!manifest_text.contains("AllocationOutput"));
        assert!(!manifest_text.contains("EffectFailure"));
        assert!(manifest_text.contains("reservation_failures"));

        let replay_stats = Arc::new(warehouse::WarehouseStats::default());
        run(journal_root.clone(), Some(&baseline), replay_stats.clone()).await;
        assert_eq!(replay_stats.reserve_calls(), 0);
        assert_eq!(replay_stats.release_calls(), 0);

        let candidate = latest_run_dir(&journal_root);
        assert_ne!(candidate, baseline);
        let outcome = verify_run_dirs(&baseline, &candidate, &VerifyOptions::default())
            .expect("verification runs");
        assert_eq!(outcome.exit_code(), 0);
        assert!(matches!(outcome, VerifyOutcome::Completed { .. }));

        let replay_events = allocation_events(&candidate).await;
        assert_eq!(count::<ReservationFailed>(&replay_events), 2);
        assert_eq!(count::<StockReleased>(&replay_events), 1);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn allocator_propagates_non_policy_failures_live_and_replay() {
        let temp = tempfile::tempdir().expect("tempdir");

        for (label, config, live_fragment) in [
            (
                "dependency",
                WarehouseConfig {
                    reserve_latency: std::time::Duration::ZERO,
                    reserve_unavailable: true,
                    ..WarehouseConfig::default()
                },
                "warehouse unavailable",
            ),
            (
                "validation",
                WarehouseConfig {
                    reserve_latency: std::time::Duration::ZERO,
                    reserve_test_fault: Some(WarehouseTestFault::Validation),
                    ..WarehouseConfig::default()
                },
                "invalid reservation input",
            ),
            (
                "domain",
                WarehouseConfig {
                    reserve_latency: std::time::Duration::ZERO,
                    reserve_test_fault: Some(WarehouseTestFault::Domain),
                    ..WarehouseConfig::default()
                },
                "refused the reservation",
            ),
            (
                "provenance",
                WarehouseConfig {
                    reserve_latency: std::time::Duration::ZERO,
                    reserve_test_fault: Some(WarehouseTestFault::Provenance),
                    ..WarehouseConfig::default()
                },
                "effect provenance mismatch",
            ),
            (
                "journal",
                WarehouseConfig {
                    reserve_latency: std::time::Duration::ZERO,
                    reserve_test_fault: Some(WarehouseTestFault::Journal),
                    ..WarehouseConfig::default()
                },
                "effect journal write failed",
            ),
        ] {
            let journal_root = temp.path().join(label);
            let live_stats = Arc::new(warehouse::WarehouseStats::default());
            run_with_config(
                journal_root.clone(),
                None,
                false,
                one_order(),
                config.clone(),
                live_stats.clone(),
            )
            .await
            .unwrap_or_else(|error| panic!("{label} live flow should settle its error: {error}"));
            assert_eq!(live_stats.reserve_calls(), 1);

            let baseline = latest_run_dir(&journal_root);
            assert_propagated_without_reservation_failure(&baseline, live_fragment).await;

            let replay_stats = Arc::new(warehouse::WarehouseStats::default());
            run_with_config(
                journal_root.clone(),
                Some(&baseline),
                false,
                one_order(),
                config,
                replay_stats.clone(),
            )
            .await
            .unwrap_or_else(|error| panic!("{label} replay should settle its error: {error}"));
            assert_eq!(
                replay_stats.reserve_calls(),
                0,
                "{label} replay must use the recorded failure"
            );

            let replay = latest_run_dir(&journal_root);
            assert_ne!(replay, baseline);
            assert_propagated_without_reservation_failure(&replay, "recorded effect failure").await;
        }
    }

    /// A config file for the shipped flow naming one warehouse.
    fn warehouse_settings(dir: &Path, id: &str) -> PathBuf {
        let path = dir.join(format!("{id}.toml"));
        std::fs::write(
            &path,
            format!(
                "[app.warehouse]\nid = \"{id}\"\nreserve_latency_ms = 0\nreserve_unavailable = false\n"
            ),
        )
        .expect("settings are written");
        path
    }

    async fn run_shipped(
        settings: &Path,
        journal_root: PathBuf,
        replay_from: Option<&Path>,
        stats: Arc<warehouse::WarehouseStats>,
    ) -> Result<(), String> {
        let mut args = vec![
            OsString::from("flash_sale_allocation"),
            OsString::from("--config"),
            settings.as_os_str().to_os_string(),
        ];
        if let Some(archive) = replay_from {
            args.push(OsString::from("--replay-from"));
            args.push(archive.as_os_str().to_os_string());
        }
        FlowApplication::builder()
            .with_log_level(super::LogLevel::Error)
            .with_cli_args(args)
            .run_async(flow::settings_flow(one_order(), stats, journal_root))
            .await
            .map_err(|error| error.to_string())
    }

    /// FLOWIP-010t criterion 9: the warehouse id is binding evidence, so a
    /// replay under another id fails the allocator at its first recorded
    /// effect and calls no warehouse, whatever the handler does.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_changed_warehouse_id_refuses_replay_without_calling_the_warehouse() {
        let temp = tempfile::tempdir().expect("tempdir");
        let journal_root = temp.path().join("journals");
        let recorded = warehouse_settings(temp.path(), "warehouse-a");
        let live_stats = Arc::new(warehouse::WarehouseStats::default());
        run_shipped(&recorded, journal_root.clone(), None, live_stats.clone())
            .await
            .expect("the live run completes");
        assert_eq!(live_stats.reserve_calls(), 1);
        let baseline = latest_run_dir(&journal_root);
        assert_eq!(
            archive_manifest(&baseline)["effective_config"]["app"]["warehouse"]["id"],
            "warehouse-a",
            "the run records the warehouse it used"
        );

        let moved = warehouse_settings(temp.path(), "warehouse-b");
        let replay_stats = Arc::new(warehouse::WarehouseStats::default());
        let refused = run_shipped(
            &moved,
            journal_root.clone(),
            Some(&baseline),
            replay_stats.clone(),
        )
        .await;
        assert!(refused.is_err(), "the replay fails its allocator");
        assert_eq!(replay_stats.reserve_calls(), 0);
        let replay = latest_run_dir(&journal_root);
        assert_ne!(replay, baseline);
        let events: Vec<ChainEvent> = allocation_events(&replay)
            .await
            .into_iter()
            .chain(allocation_error_events(&replay).await)
            .collect();
        assert!(
            events.iter().filter_map(StageFatalRecorded::from_event).any(
                |fatal| fatal.reason == obzenflow::error::StageFatalReason::DescriptorMismatch
            ),
            "the allocator fails with descriptor_mismatch"
        );
        assert_eq!(count::<StockReserved>(&events), 0, "no divergent fact");

        let reverted_stats = Arc::new(warehouse::WarehouseStats::default());
        run_shipped(
            &recorded,
            journal_root.clone(),
            Some(&baseline),
            reverted_stats.clone(),
        )
        .await
        .expect("the recorded warehouse replays");
        assert_eq!(reverted_stats.reserve_calls(), 0);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn allocator_propagates_binding_resolution_failure() {
        let temp = tempfile::tempdir().expect("tempdir");

        let binding_root = temp.path().join("binding");
        let binding_stats = Arc::new(warehouse::WarehouseStats::default());
        let binding_result = run_with_config(
            binding_root.clone(),
            None,
            false,
            one_order(),
            WarehouseConfig {
                reserve_latency: std::time::Duration::ZERO,
                reserve_test_fault: Some(WarehouseTestFault::BindingResolution),
                ..WarehouseConfig::default()
            },
            binding_stats.clone(),
        )
        .await;
        assert!(
            binding_result.is_err(),
            "binding authority failure must fail the stage"
        );
        assert_eq!(binding_stats.reserve_calls(), 0);
        let binding_run = latest_run_dir(&binding_root);
        assert_propagated_without_reservation_failure(&binding_run, "failed to resolve").await;
    }
}
