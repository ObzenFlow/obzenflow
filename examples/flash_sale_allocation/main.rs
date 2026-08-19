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

mod allocation;
mod domain;
mod flow;
mod warehouse;

use obzenflow_infra::application::{FlowApplication, LogLevel};

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
    use super::domain::{CancelIgnored, ReservationFailed, SoldOut, StockReleased, StockReserved};
    use super::{flow, warehouse};
    use obzenflow_core::event::{ChainEvent, ChainEventContent};
    use obzenflow_core::journal::journal_owner::JournalOwner;
    use obzenflow_core::journal::Journal;
    use obzenflow_core::{StageId, TypedPayload};
    use obzenflow_infra::application::FlowApplication;
    use obzenflow_infra::journal::DiskJournal;
    use obzenflow_infra::verify::{verify_run_dirs, VerifyOptions, VerifyOutcome};
    use obzenflow_runtime::effects::{
        EffectOutcomePayload, EffectRecord, EFFECT_RECORD_EVENT_TYPE,
    };
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

    async fn allocation_events(run_dir: &Path) -> Vec<ChainEvent> {
        let manifest = archive_manifest(run_dir);
        let journal_file = manifest["stages"]["allocate_stock"]["data_journal_file"]
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
            .map(|envelope| envelope.event)
            .collect()
    }

    fn count<T: TypedPayload>(events: &[ChainEvent]) -> usize {
        events
            .iter()
            .filter(|event| event.event_type() == T::versioned_event_type())
            .count()
    }

    async fn run(
        journal_root: PathBuf,
        replay_from: Option<&Path>,
        stats: Arc<warehouse::WarehouseStats>,
    ) {
        let mut args = vec![OsString::from("flash_sale_allocation")];
        if let Some(archive) = replay_from {
            args.push(OsString::from("--replay-from"));
            args.push(archive.as_os_str().to_os_string());
            args.push(OsString::from("--verify"));
        }
        FlowApplication::builder()
            .with_cli_args(args)
            .run_async(flow::assemble_flow(
                flow::scripted_inputs(),
                warehouse::WarehouseConfig::default(),
                stats,
                journal_root,
            ))
            .await
            .expect("allocation flow completes");
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

        let policy_rejections = events
            .iter()
            .filter_map(|event| match &event.content {
                ChainEventContent::Data {
                    event_type,
                    payload,
                } if event_type == EFFECT_RECORD_EVENT_TYPE => {
                    serde_json::from_value::<EffectRecord>(payload.clone()).ok()
                }
                _ => None,
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
}
