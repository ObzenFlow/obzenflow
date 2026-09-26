// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! FLOWIP-095j: the `obzenflow verify` subcommand's OS-level exit codes.
//!
//! The binary is a thin shell over `obzenflow_infra::verify`; this suite
//! asserts the process boundary: `0` certified match with the headline line
//! on stdout, `3` refusal for an unavailable archive. (Divergence and
//! uncertified verdicts are exercised in-process by the other verification
//! suites; the contract mapping itself is unit-tested in `verdict`.)

use obzenflow::application::FlowApplication;
use obzenflow::flow::{flow, sink, source, FlowDefinition};
use obzenflow::journal::disk_journals;
use obzenflow::schema::TypedPayload;
use obzenflow::stages::sinks::{DeliveryContext, SinkTyped};
use obzenflow::stages::sources::{SourceError, TypedFiniteSourceHandler};
use serde::{Deserialize, Serialize};
use std::ffi::OsString;
use std::path::{Path, PathBuf};
use std::process::Command;

#[path = "../examples/payment_gateway_resilience/support.rs"]
pub mod gateway_demo;

fn assert_json_summary(stdout: &str, stderr: &str) {
    use obzenflow::journal::read::{RunJournalKind, RunRecord, RunRecordData};
    use std::collections::BTreeMap;

    let rows: Vec<RunRecord> = stdout
        .lines()
        .map(|line| serde_json::from_str(line).expect("stdout contains only typed NDJSON records"))
        .collect();
    let summaries: Vec<serde_json::Value> = stderr
        .lines()
        .filter_map(|line| serde_json::from_str::<serde_json::Value>(line).ok())
        .filter(|row| row["event"] == "run_observation_summary")
        .collect();
    assert_eq!(summaries.len(), 1, "one exit summary on stderr: {stderr}");
    let summary = &summaries[0];
    assert_eq!(summary["records"], rows.len());
    let mut journals = BTreeMap::<String, usize>::new();
    let mut types = BTreeMap::<String, usize>::new();
    for row in rows {
        let stage = row
            .journal
            .stage
            .as_ref()
            .map_or("system", |s| s.key.as_str());
        let kind = match row.journal.kind {
            RunJournalKind::System => "pipeline",
            RunJournalKind::MetricsCoordination => "metrics/coordination",
            RunJournalKind::MetricsExport => "metrics/export",
            RunJournalKind::Data => "data",
            RunJournalKind::Error => "error",
        };
        *journals.entry(format!("{stage}/{kind}")).or_default() += 1;
        let event_type = match &row.record {
            RunRecordData::Chain(record) => record.event_type_name(),
            RunRecordData::System(record) => record.event_type_name(),
        };
        *types.entry(event_type.to_owned()).or_default() += 1;
    }
    assert_eq!(summary["journals"], serde_json::to_value(journals).unwrap());
    assert_eq!(summary["event_types"], serde_json::to_value(types).unwrap());
    assert_eq!(summary["other_event_types"], 0);
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct Tick {
    n: u64,
}

impl TypedPayload for Tick {
    const EVENT_TYPE: &'static str = "cli_verify.tick";
}

#[derive(Clone, Debug)]
struct Ticks {
    next: u64,
}

impl Ticks {
    fn new() -> Self {
        Self { next: 1 }
    }
}

impl TypedFiniteSourceHandler for Ticks {
    type Output = Tick;

    fn next(&mut self) -> Result<Option<Vec<Self::Output>>, SourceError> {
        if self.next > 3 {
            return Ok(None);
        }
        let n = self.next;
        self.next += 1;
        Ok(Some(vec![Tick { n }]))
    }
}

fn discard<T>() -> impl FnMut(T, DeliveryContext) -> std::future::Ready<()> + Send + Sync + Clone
where
    T: Clone + Send + Sync + 'static,
{
    move |_payload: T, _delivery| std::future::ready(())
}

fn build_flow(journal_base: PathBuf) -> FlowDefinition {
    FlowDefinition::materialize(move |_runtime_config| {
        let ticks_handler = Ticks::new();
        let out_handler = SinkTyped::with_delivery(discard::<Tick>()).idempotent();

        Ok(flow! {
            name: "cli_verify",
            journals: disk_journals(journal_base),

            stages: {
                ticks = source!(Tick => ticks_handler);
                out = sink!(Tick => out_handler);
            },

            topology: {
                ticks |> out;
            }
        })
    })
}

fn latest_run_dir(base: &Path) -> PathBuf {
    let flows_dir = base.join("flows");
    let mut entries: Vec<PathBuf> = std::fs::read_dir(&flows_dir)
        .expect("flows directory should exist")
        .map(|entry| entry.expect("flow dir entry").path())
        .filter(|path| path.join("run_manifest.json").exists())
        .collect();
    entries.sort();
    entries.pop().expect("run should have produced an archive")
}

#[cfg(feature = "web-host")]
#[tokio::test(flavor = "multi_thread")]
async fn runtime_writer_columns_use_journaled_registration() {
    use obzenflow::journal::read::{RunRecordData, SupervisionMode, SupervisorKind, SystemPayload};
    let temp = tempfile::tempdir().unwrap();
    FlowApplication::builder()
        .with_cli_args(["writer-registration", "--server", "--server-port", "0"])
        .run_async(build_flow(temp.path().to_owned()))
        .await
        .unwrap();
    let run = latest_run_dir(temp.path());
    let mut snapshot = obzenflow::journal::read::open_disk_run(&run).await.unwrap();
    let pipeline = snapshot.identity().pipeline_writer_id;
    let mut registered = std::collections::BTreeMap::new();
    while let Some(record) = snapshot.next().await.unwrap() {
        let (writer, descriptor) = match &record.record {
            RunRecordData::System(row) => (row.writer_id(), match &row.payload {
                SystemPayload::SupervisorRegistered { descriptor } => Some(descriptor),
                _ => None,
            }),
            RunRecordData::Chain(row) => (row.writer_id(), match &row.payload {
                obzenflow_core::event::ChainPayload::Execution(obzenflow_core::event::payloads::execution_payload::ExecutionPayload::SupervisorRegistered { descriptor }) => Some(descriptor),
                _ => None,
            }),
        };
        if let Some(descriptor) = descriptor {
            assert!(registered
                .insert(writer.to_string(), descriptor.clone())
                .is_none());
        }
    }
    assert_eq!(registered.len(), 4); // Two stages, pipeline, metrics aggregator.
    assert_eq!(
        registered[&pipeline.to_string()].kind,
        SupervisorKind::Pipeline
    );
    let metrics = registered
        .values()
        .find(|descriptor| descriptor.kind == SupervisorKind::MetricsAggregator)
        .unwrap();
    assert_eq!(metrics.name, "metrics_aggregator");
    assert_eq!(metrics.supervision, SupervisionMode::SelfSupervised);
    for width in [90, 40] {
        let shown = Command::new(env!("CARGO_BIN_EXE_obzenflow"))
            .args([
                "show",
                run.to_str().unwrap(),
                "--include-runtime",
                "--color",
                "never",
            ])
            .env("COLUMNS", width.to_string())
            .output()
            .unwrap();
        assert!(
            shown.status.success(),
            "{}",
            String::from_utf8_lossy(&shown.stderr)
        );
        let text = String::from_utf8(shown.stdout).unwrap();
        let summary = text.split_once("MANIFEST     run_manifest.json").unwrap().1;
        for line in summary.lines().skip(1) {
            assert!(
                line.chars().count() <= width,
                "summary exceeds {width} columns: {line}"
            );
        }
        let table = text.split_once("\nSYSTEM SUPERVISORS\n").unwrap().1;
        let (system, application) = table.split_once("\nAPPLICATION STAGES\n").unwrap();
        assert_eq!(
            system.matches("Supervisor: pipeline_supervisor\n").count(),
            1
        );
        assert_eq!(
            system.matches("Supervisor: metrics_aggregator\n").count(),
            1
        );
        assert!(!application.contains("Supervisor:"));
        let (pipeline, metrics) = system
            .split_once("Supervisor: metrics_aggregator\n")
            .unwrap();
        assert!(pipeline.contains("\n    system.log\n"));
        assert!(!pipeline.contains("system.metrics.exported"));
        assert!(metrics.contains("\n    metrics-coordination.log\n"));
        assert!(metrics.contains("\n    metrics-export.log\n"));
        assert_eq!(
            metrics
                .lines()
                .filter(|line| line.trim_start().starts_with("Count "))
                .count(),
            2
        );
        assert!(summary
            .split_whitespace()
            .collect::<Vec<_>>()
            .join(" ")
            .contains("across 7 journals."));
        if width == 90 {
            assert!(metrics
                .lines()
                .any(|line| line.contains("system.metrics.exported")
                    && line.contains("metrics_aggregator")
                    && line.contains("MetricsAggregator")));
            assert!(pipeline
                .lines()
                .any(|line| line.contains("system.metrics.drain_requested")
                    && line.contains("pipeline_supervisor")
                    && line.contains("Pipeline")));
        }
        assert!(!table.contains("Not recorded"));
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn cli_verify_exit_codes_follow_the_contract() {
    let temp = tempfile::tempdir().expect("tempdir");
    let journal_base = temp.path().join("journals");

    FlowApplication::builder()
        .with_cli_args(["obzenflow"])
        .run_async(build_flow(journal_base.clone()))
        .await
        .expect("live flow should complete");
    let baseline = latest_run_dir(&journal_base);
    let files_before: std::collections::BTreeMap<_, _> = std::fs::read_dir(&baseline)
        .unwrap()
        .map(Result::unwrap)
        .filter(|entry| entry.file_type().unwrap().is_file())
        .map(|entry| (entry.path(), std::fs::read(entry.path()).unwrap()))
        .collect();
    let mut expected = std::collections::BTreeMap::new();
    let mut snapshot = obzenflow::journal::read::open_disk_run(&baseline)
        .await
        .unwrap();
    while let Some(row) = snapshot.next().await.unwrap() {
        expected.insert(
            (row.journal.id, row.position),
            serde_json::to_value(row).unwrap(),
        );
    }

    let mut exported_records = Vec::new();
    // Both views consume the shared projection, including settlement evidence.
    for (follow, include_runtime) in [(false, true), (true, true), (false, false), (true, false)] {
        let mut command = tokio::process::Command::new(env!("CARGO_BIN_EXE_obzenflow"));
        command
            .arg("show")
            .arg(&baseline)
            .args(["--jsonl", "--color", "always"])
            .kill_on_drop(true);
        if follow {
            command.arg("--follow");
        }
        if include_runtime {
            command.arg("--include-runtime");
        }
        let output = tokio::time::timeout(std::time::Duration::from_secs(10), command.output())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            output.status.code(),
            Some(0),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        let stdout = String::from_utf8(output.stdout).unwrap();
        assert_json_summary(&stdout, &String::from_utf8_lossy(&output.stderr));
        assert!(
            !stdout.contains('\x1b'),
            "JSON never contains presentation escapes"
        );
        let rows: Vec<obzenflow::journal::read::RunRecord> = stdout
            .lines()
            .map(|line| serde_json::from_str(line).unwrap())
            .collect();
        assert!(!rows.is_empty());
        assert_eq!(
            rows.iter()
                .filter(|r| r.kind == obzenflow::journal::read::RunRecordKind::SourceFact)
                .count(),
            3
        );
        assert!(rows
            .iter()
            .all(|r| r.version == obzenflow::journal::read::RUN_RECORD_VERSION));
        let mut positions = std::collections::BTreeMap::new();
        for row in &rows {
            if include_runtime {
                let next = positions.entry(row.journal.id).or_insert(0);
                assert_eq!(
                    row.position.0, *next,
                    "no skipped or repeated logical records"
                );
                *next += 1;
            }
            assert_eq!(
                serde_json::to_value(row).unwrap(),
                expected[&(row.journal.id, row.position)]
            );
        }
        if !follow && include_runtime {
            assert_eq!(rows.len(), expected.len());
            exported_records = rows
                .iter()
                .map(|row| serde_json::to_value(&row.record).unwrap())
                .collect();
        }
        if !include_runtime {
            assert!(rows.len() < expected.len(), "runtime records are omitted");
        }
        if follow {
            assert!(String::from_utf8_lossy(&output.stderr).contains("run_observation_covered"));
        }
    }
    for (file, bytes) in files_before {
        assert_eq!(
            std::fs::read(file).unwrap(),
            bytes,
            "observation cannot change archive evidence"
        );
    }

    let human = Command::new(env!("CARGO_BIN_EXE_obzenflow"))
        .arg("show")
        .arg(&baseline)
        .args(["--color", "never", "--follow"])
        .output()
        .unwrap();
    assert!(human.status.success());
    assert!(
        human.stderr.is_empty(),
        "human settlement uses the footer, not raw diagnostics"
    );
    let human = String::from_utf8(human.stdout).unwrap();
    assert!(human.ends_with("Run completed. CLI reached the recorded end of execution.\n"));
    assert!(human.contains("cli_verify.tick.v1 ← ticks()"), "{human}");
    assert!(
        human.contains("sink.delivery ← out(cli_verify.tick.v1)"),
        "{human}"
    );
    assert!(human.contains("\"n\": 1") && human.contains("\"n\": 3"));

    let inspection = Command::new(env!("CARGO_BIN_EXE_obzenflow"))
        .arg("inspect")
        .arg(&baseline)
        .args(["--stage", "ticks", "--event-type", "cli_verify.tick.v1"])
        .output()
        .expect("archive inspection process");
    assert!(
        inspection.status.success(),
        "{}",
        String::from_utf8_lossy(&inspection.stderr)
    );
    let inspection = String::from_utf8(inspection.stdout).unwrap();
    assert!(inspection.contains("flow_name:  cli_verify"));
    assert!(inspection.contains("[ticks]") && !inspection.contains("[out]"));
    let listing: Vec<_> = inspection
        .lines()
        .filter(|line| line.starts_with("  "))
        .collect();
    assert_eq!(listing.len(), 3);
    for line in listing {
        let mut columns = line.split_whitespace();
        columns.next().unwrap().parse::<u64>().expect("byte offset");
        assert_eq!(columns.next(), Some("cli_verify.tick.v1"));
        assert!(columns.next().is_none());
    }

    // JSONL show retains every canonical envelope and payload. Only the outer
    // run/journal context and cross-journal presentation order differ.
    let canonical_export = temp.path().join("canonical.jsonl");
    obzenflow::journal::export_jsonl(&baseline, Some(&canonical_export)).unwrap();
    let records: Vec<serde_json::Value> = std::fs::read_to_string(canonical_export)
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str(line).expect("canonical record JSON"))
        .collect();
    assert!(!records.is_empty());
    let sorted_records = |rows: &[serde_json::Value]| {
        let mut records: Vec<_> = rows.iter().map(ToString::to_string).collect();
        records.sort_unstable();
        records
    };
    assert_eq!(
        sorted_records(&exported_records),
        sorted_records(&records),
        "show --jsonl --include-runtime exports every complete canonical record"
    );
    for record in &records {
        let roots = record.as_object().unwrap();
        assert_eq!(roots.len(), 2);
        assert!(roots.contains_key("envelope") && roots.contains_key("payload"));
        assert!(record["envelope"]["provenance"]["event"]["id"].is_string());
        assert!(record["envelope"]["provenance"]["journal"].is_object());
    }
    let facts: Vec<_> = records
        .iter()
        .filter(|record| record["envelope"]["provenance"]["event"]["event_kind"] == "fact")
        .collect();
    assert_eq!(facts.len(), 3);
    assert!(facts.iter().all(|record| record["payload"]["n"].is_u64()));

    FlowApplication::builder()
        .with_cli_args(vec![
            OsString::from("obzenflow"),
            OsString::from("--replay-from"),
            baseline.as_os_str().to_os_string(),
        ])
        .run_async(build_flow(journal_base.clone()))
        .await
        .expect("replay flow should complete");
    let candidate = latest_run_dir(&journal_base);

    // Certified match: exit 0 and the headline line on stdout.
    let output = Command::new(env!("CARGO_BIN_EXE_obzenflow"))
        .args(["verify", "--baseline"])
        .arg(&baseline)
        .arg("--candidate")
        .arg(&candidate)
        .output()
        .expect("obzenflow binary should run");
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert_eq!(
        output.status.code(),
        Some(0),
        "certified match exits 0: {stdout}"
    );
    assert!(
        stdout.contains("output matched the original run, 0 differences"),
        "the headline line prints on a certified match: {stdout}"
    );

    // Refusal: a missing baseline exits 3 with the reason on stdout.
    let output = Command::new(env!("CARGO_BIN_EXE_obzenflow"))
        .args(["verify", "--baseline"])
        .arg(temp.path().join("no-such-run"))
        .arg("--candidate")
        .arg(&candidate)
        .output()
        .expect("obzenflow binary should run");
    assert_eq!(output.status.code(), Some(3), "refusals exit 3");
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.contains("verification refused"),
        "the refusal names its reason: {stdout}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn teaching_view_distinguishes_effects_replay_causes_and_compact_output() {
    use gateway_demo::domain::{
        CustomerOrderPlaced, OrderChannel, PaymentMethodState, TrafficPhase,
    };
    use obzenflow::journal::read::{RunRecord, RunRecordData, RunRecordKind};

    fn build_gateway(root: PathBuf) -> FlowDefinition {
        let valid = CustomerOrderPlaced {
            order_id: "cli-teaching-valid".into(),
            customer_id: "cli-teaching-customer".into(),
            channel: OrderChannel::Web,
            amount_cents: 1_000,
            payment_method_state: PaymentMethodState::Valid,
            phase: TrafficPhase::Warmup,
        };
        let mut invalid = valid.clone();
        invalid.order_id = "cli-teaching-invalid".into();
        invalid.payment_method_state = PaymentMethodState::InvalidNumber;
        let mut declined = valid.clone();
        declined.order_id = "cli-teaching-declined".into();
        declined.payment_method_state = PaymentMethodState::AddressMismatch;
        gateway_demo::flow::assemble_flow(
            vec![valid, invalid, declined],
            Vec::new(),
            gateway_demo::gateway::GatewayTransform::default(),
            1_000.0,
            root,
        )
    }

    fn show(run: &Path, args: &[&str]) -> (String, String) {
        let output = Command::new(env!("CARGO_BIN_EXE_obzenflow"))
            .arg("show")
            .arg(run)
            .args(args)
            .env("NO_COLOR", "1")
            .env("COLUMNS", "160")
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        (
            String::from_utf8(output.stdout).unwrap(),
            String::from_utf8(output.stderr).unwrap(),
        )
    }

    fn journal_numbers(text: &str) -> std::collections::BTreeMap<String, usize> {
        let mut pieces = text.split("\x1b[");
        let mut plain = pieces.next().unwrap().to_owned();
        for piece in pieces {
            plain.push_str(piece.split_once('m').unwrap().1);
        }
        plain
            .split_once("Journal numbers:\n")
            .unwrap()
            .1
            .split_once("\n\n")
            .unwrap()
            .0
            .lines()
            .flat_map(|line| line.trim().split(" · "))
            .map(|field| {
                let (number, name) = field.split_once(' ').unwrap();
                (name.to_owned(), number.parse().unwrap())
            })
            .collect()
    }

    fn assert_palette(text: &str) {
        let numbers = journal_numbers(text);
        let manifest_heading = "\x1b[1;38;5;255mMANIFEST     run_manifest.json\x1b[0m";
        let (events, footer) = text.split_once(manifest_heading).unwrap();
        let facts: Vec<_> = text
            .lines()
            .filter(|line| {
                line.contains("mSOURCE (stage:")
                    || line.contains("mTRANSFORM (stage:")
                    || line.contains("mEFFECTFUL TRANSFORM (stage:")
            })
            .collect();
        assert!(!facts.is_empty());
        for line in facts {
            assert!(line.starts_with("\x1b[1;38;5;208m"));
        }
        let deliveries: Vec<_> = text
            .lines()
            .filter(|line| line.contains("mDELIVERY (stage:"))
            .collect();
        assert!(!deliveries.is_empty());
        for line in deliveries {
            assert!(line.starts_with("\x1b[38;5;217mDELIVERY (stage: "));
            assert!(line.ends_with(")\x1b[0m"));
        }
        for line in events.lines().filter(|line| !line.contains('⟨')) {
            assert!(
                line.matches('\x1b').count()
                    <= if line.contains("(stage:") {
                        6
                    } else if line.contains('←') {
                        4
                    } else {
                        2
                    },
                "headings emphasize the stage and equations emphasize the event: {line}"
            );
        }
        for (stage, heading, event, input, normal, reporter, output) in [
            (
                "web_orders",
                "SOURCE",
                "commerce.customer_order_placed.v1",
                "",
                "1;38;5;208",
                223,
                215,
            ),
            (
                "validate_order",
                "TRANSFORM",
                "payment.order_validated.v1",
                "commerce.customer_order_placed.v1",
                "1;38;5;208",
                223,
                215,
            ),
            (
                "authorize_payment",
                "EFFECTFUL TRANSFORM",
                "payment.authorized.v1",
                "payment.order_validated.v1",
                "1;38;5;208",
                223,
                215,
            ),
            (
                "paid_orders",
                "DELIVERY",
                "sink.delivery",
                "payment.authorized.v1",
                "38;5;217",
                231,
                224,
            ),
        ] {
            let number = numbers[stage];
            assert!(text.contains(&format!(
                "\x1b[{normal}m{heading} (stage: \x1b[0m\x1b[1;38;5;{reporter}m{stage}\x1b[0m\x1b[{normal}m, journal: {number})\x1b[0m\n\x1b[1;38;5;{output}m{event}\x1b[0m\x1b[{normal}m ← {stage}({input})"
            )));
        }
        assert!(
            text.contains("\x1b[1;4;38;5;215m") && text.contains("\x1b[1;4;38;5;224m"),
            "{text}"
        );
        for underlined in text.split("\x1b[1;4;38;5;").skip(1) {
            let digits = underlined
                .split_once('m')
                .unwrap()
                .1
                .split_once("\x1b[0m")
                .unwrap()
                .0;
            assert!(
                digits.chars().all(|c| c.is_ascii_digit()),
                "underline only digits: {digits:?}"
            );
        }
        for escape in footer.split("\x1b[").skip(1) {
            let code = escape.split_once('m').unwrap().0;
            assert!(
                matches!(
                    code,
                    "0" | "1;38;5;255" | "38;5;252" | "38;5;245" | "1;4;38;5;252"
                ),
                "the archive summary uses grayscale only: {code}"
            );
        }
    }

    fn assert_record_outputs(text: &str, rows: &[RunRecord]) {
        let mut expected = std::collections::BTreeMap::new();
        for row in rows {
            match row.kind {
                RunRecordKind::SourceFact
                | RunRecordKind::StageOutput
                | RunRecordKind::Effect
                | RunRecordKind::Delivery => {}
                _ => continue,
            };
            let RunRecordData::Chain(chain) = &row.record else {
                panic!("selected record must be a chain record");
            };
            *expected
                .entry(chain.envelope.provenance.event.event_type.clone())
                .or_insert(0) += 1;
        }
        let mut actual = std::collections::BTreeMap::new();
        for line in text.lines() {
            let Some((left, _)) = line.split_once(" ← ") else {
                continue;
            };
            if left.starts_with(' ') || left == "Output" {
                continue; // Payload values, detail JSON and the legend are not equations.
            }
            // Quiet mode prefixes the equation with its stage-kind heading.
            let output = left.split_whitespace().last().unwrap().to_owned();
            *actual.entry(output).or_insert(0) += 1;
        }
        assert_eq!(
            actual, expected,
            "the left side must name every recorded output exactly once"
        );
    }

    fn assert_fact_origins(text: &str) {
        let numbers = journal_numbers(text);
        for (event_type, input, stage, order) in [
            (
                "payment.order_validated.v1",
                "commerce.customer_order_placed.v1",
                "validate_order",
                "cli-teaching-valid",
            ),
            (
                "order.invalid.v1",
                "commerce.customer_order_placed.v1",
                "validate_order",
                "cli-teaching-invalid",
            ),
            (
                "order.cancelled.v1",
                "commerce.customer_order_placed.v1",
                "validate_order",
                "cli-teaching-invalid",
            ),
            (
                "payment.declined.v1",
                "payment.order_validated.v1",
                "authorize_payment",
                "cli-teaching-declined",
            ),
            (
                "order.cancelled.v1",
                "payment.order_validated.v1",
                "authorize_payment",
                "cli-teaching-declined",
            ),
        ] {
            let heading = if stage == "authorize_payment" {
                "EFFECTFUL TRANSFORM"
            } else {
                "TRANSFORM"
            };
            let block = text
                .split("\n\n")
                .find(|block| {
                    block.starts_with(&format!(
                        "{heading} (stage: {stage}, journal: {})\n{event_type} ← ",
                        numbers[stage]
                    )) && block.contains(&format!("\"order_id\": \"{order}\""))
                })
                .unwrap_or_else(|| {
                    panic!("missing {event_type} with its own payload for {order}: {text}")
                });
            let lines: Vec<_> = block.lines().collect();
            let clock = lines.iter().position(|line| line.starts_with('⟨')).unwrap();
            let expression = lines[1..clock].join(" ");
            assert!(
                expression.starts_with(&format!("{event_type} ← {stage}({input})")),
                "second line names the recorded output and its origin: {block}"
            );
            assert_eq!(lines[clock + 1], "{", "payload starts at the left margin");
            assert!(
                lines[clock + 2..]
                    .iter()
                    .all(|line| line.chars().count() <= 90),
                "{block}"
            );
        }
    }

    let temp = tempfile::tempdir().unwrap();
    FlowApplication::builder()
        .with_cli_args(["gateway"])
        .run_async(build_gateway(temp.path().to_path_buf()))
        .await
        .unwrap();
    let baseline = latest_run_dir(temp.path());
    let (human, _) = show(&baseline, &[]);
    let numbers = journal_numbers(&human);
    let (explicit, _) = show(&baseline, &["--explain", "--color", "never"]);
    assert!(explicit.contains("Effects are data:"));
    assert!(
        !human.contains("Effects are data:"),
        "default rows use operations and payloads"
    );
    assert!(
        !human.contains('\x1b'),
        "piped output and NO_COLOR use plain text"
    );
    for teaching in [
        "commerce.customer_order_placed.v1 ← web_orders()",
        "payment.order_validated.v1 ← validate_order(commerce.customer_order_placed.v1)",
        "payment.authorized.v1 ← authorize_payment(payment.order_validated.v1)",
        "sink.delivery ← paid_orders(payment.authorized.v1)",
        "Clocks ⟨",
        "\"reason\": \"InvalidPaymentMethod\"",
        "order.cancelled.v1 ← validate_order(commerce.customer_order_placed.v1)",
        "MANIFEST     run_manifest.json",
        "\nJOURNALS\n",
        "Each stage has separate data and error journal files.",
        "\nAPPLICATION STAGES\n",
        "\nSOURCE: web_orders\n  Reads from: —\n",
        "\nTRANSFORM: validate_order\n  Reads from: store_orders, web_orders\n",
        "\nEFFECTFUL TRANSFORM: authorize_payment\n  Reads from: validate_order\n",
        "\nSINK: paid_orders\n  Reads from: authorize_payment\n",
        "  Data subscribers: cancelled_orders, manual_review, paid_orders\n",
        "  Data subscribers: —\n",
        "  Owns:\n    ",
        "      Runtime readers: pipeline_supervisor\n",
        "Each stage writes business outputs to its own data journal for subscribers to read.",
        "- Forwarded control signals keep their original Author.",
        "- EOF from all required upstreams lets a supervisor drain and complete.",
        "Run completed. CLI reached the end of its snapshot.",
    ] {
        assert!(human.contains(teaching), "missing teaching cue {teaching}");
    }
    assert!(!human.contains("Read (subscribers) = stage(inputs)"));
    assert!(human.lines().any(|line| line
        == format!(
            "TRANSFORM (stage: validate_order, journal: {})",
            numbers["validate_order"]
        )));
    assert!(human.lines().any(|line| line
        == format!(
            "DELIVERY (stage: paid_orders, journal: {})",
            numbers["paid_orders"]
        )));
    assert!(!human.contains("RUNTIME"));
    assert!(!human.contains("system.metrics.exported"));
    let (verbose, _) = show(&baseline, &["--include-runtime"]);
    assert_eq!(journal_numbers(&verbose), numbers);
    assert!(verbose.contains("RUNTIME"));
    assert!(verbose.contains("control.eof") && !human.contains("control.eof"));
    // This demo's keyed effect records successful domain facts. Explicit
    // attempt-start records belong to affine effects, not every physical call.
    assert!(
        !human.contains("[read from journal]"),
        "live facts are not labeled replayed"
    );
    let (json, diagnostics) = show(
        &baseline,
        &["--jsonl", "--include-runtime", "--color", "always"],
    );
    assert_json_summary(&json, &diagnostics);
    let rows: Vec<RunRecord> = json
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
    fn clock_matrix(text: &str) -> &str {
        text.split_once("\nLAST OBSERVED JOURNAL CLOCKS\n")
            .or_else(|| text.split_once("\nFINAL JOURNAL CLOCKS\n"))
            .unwrap()
            .1
            .split_once("\nEvent counts cover displayed entries")
            .unwrap()
            .0
    }
    let matrix = clock_matrix(&human);
    assert_eq!(
        matrix,
        clock_matrix(&verbose),
        "hidden runtime records still supply the last clocks"
    );
    let (settled, _) = show(&baseline, &["--follow"]);
    assert_eq!(journal_numbers(&settled), numbers);
    assert!(settled.contains("\nFINAL JOURNAL CLOCKS\n"));
    assert_eq!(matrix, clock_matrix(&settled));
    assert!(human.find("\nJOURNALS\n").unwrap() < human.find(matrix).unwrap());
    assert!(human.find(matrix).unwrap() < human.find("\nSOURCE: web_orders\n").unwrap());
    let mut last_clocks = std::collections::BTreeMap::new();
    for row in &rows {
        let name = match (&row.journal.stage, row.journal.kind) {
            (Some(stage), obzenflow::journal::read::RunJournalKind::Error) => {
                format!("{}/error", stage.key)
            }
            (Some(stage), _) => stage.key.clone(),
            (_, obzenflow::journal::read::RunJournalKind::System) => "pipeline".into(),
            (_, obzenflow::journal::read::RunJournalKind::MetricsCoordination) => {
                "metrics/coordination".into()
            }
            (_, obzenflow::journal::read::RunJournalKind::MetricsExport) => "metrics/export".into(),
            _ => panic!("expected a named journal"),
        };
        let clock = match &row.record {
            RunRecordData::Chain(record) => &record.envelope.provenance.journal.vector_clock.clocks,
            RunRecordData::System(record) => {
                &record.envelope.provenance.journal.vector_clock.clocks
            }
        };
        last_clocks.insert(name, (row.journal.id, clock));
    }
    let matrix_rows: Vec<_> = matrix
        .lines()
        .filter_map(|line| {
            let cells: Vec<_> = line.split_whitespace().collect();
            cells.first()?.parse::<usize>().ok()?;
            Some(cells)
        })
        .collect();
    assert_eq!(matrix_rows.len(), last_clocks.len());
    let columns: Vec<_> = matrix
        .lines()
        .find(|line| line.trim_start().starts_with("#  Journal"))
        .unwrap()
        .split_whitespace()
        .skip(2)
        .collect();
    assert_eq!(
        columns,
        matrix_rows.iter().map(|row| row[0]).collect::<Vec<_>>()
    );
    for row in &matrix_rows {
        assert_eq!(row[0], numbers[row[1]].to_string());
        assert_eq!(row.len(), matrix_rows.len() + 2);
        let (_, expected) = last_clocks[row[1]];
        for (column, journal) in matrix_rows.iter().enumerate() {
            let coordinate = obzenflow_core::event::CausalCoordinate::new(
                obzenflow_core::event::JournalWriterId::from_journal_id(last_clocks[journal[1]].0),
            );
            assert_eq!(
                row[column + 2].parse::<u64>().unwrap(),
                expected.get(&coordinate).copied().unwrap_or(0)
            );
        }
    }
    for row in &rows {
        if let Some(stage) = &row.journal.stage {
            assert_eq!(stage.is_effectful, stage.key == "authorize_payment");
        }
    }
    assert_record_outputs(&human, &rows);
    assert_record_outputs(&explicit, &rows);
    assert_fact_origins(&human);
    assert!(!human.contains("← cause") && !human.contains("root (no recorded parent)"));
    assert!(human.ends_with(&format!(
        "CLI observed {} journal entries across 15 journals.\nRun completed. CLI reached the end of its snapshot.\n",
        rows.len()
    )));
    let manifest: serde_json::Value =
        serde_json::from_slice(&std::fs::read(baseline.join("run_manifest.json")).unwrap())
            .unwrap();
    assert!(human.contains(manifest["flow_name"].as_str().unwrap()));
    assert!(human.contains(manifest["flow_id"].as_str().unwrap()));
    let inventory = human.split_once("\nJOURNALS\n").unwrap().1;
    let system_line = inventory
        .lines()
        .find(|line| line.trim_start().starts_with("system.log"))
        .unwrap();
    let system_count = rows
        .iter()
        .filter(|row| row.journal.kind == obzenflow::journal::read::RunJournalKind::System)
        .count();
    assert_eq!(
        system_line.split_whitespace().last().unwrap(),
        system_count.to_string()
    );
    for stage in manifest["stages"].as_object().unwrap().keys() {
        let counts: Vec<_> = inventory
            .lines()
            .map(|line| line.split_whitespace().collect::<Vec<_>>())
            .find(|columns| columns.first().copied() == Some(stage.as_str()))
            .unwrap();
        for (column, kind) in [
            (1, obzenflow::journal::read::RunJournalKind::Data),
            (2, obzenflow::journal::read::RunJournalKind::Error),
        ] {
            let observed = rows
                .iter()
                .filter(|row| {
                    row.journal.kind == kind
                        && row
                            .journal
                            .stage
                            .as_ref()
                            .is_some_and(|owner| owner.key == *stage)
                })
                .count();
            assert_eq!(counts[column], observed.to_string(), "inventory includes all journal entries, even hidden runtime rows and empty journals");
        }
    }
    let selected = rows
        .iter()
        .filter(|row| {
            matches!(
                row.kind,
                RunRecordKind::SourceFact
                    | RunRecordKind::StageOutput
                    | RunRecordKind::Effect
                    | RunRecordKind::Delivery
            )
        })
        .count();
    assert!(selected < rows.len(), "fixture includes runtime evidence");
    fn event_table(
        text: &str,
        heading: &str,
    ) -> std::collections::BTreeMap<(String, String, String), usize> {
        let body = text.split_once(&format!("\n    {heading}\n")).unwrap().1;
        body.lines()
            .skip_while(|line| !line.trim_start().starts_with("Count "))
            .skip(1)
            .take_while(|line| !line.is_empty())
            .filter_map(|line| {
                let mut columns = line.split_whitespace();
                let count = columns.next()?.parse::<usize>().ok()?;
                let event_type = columns.next()?.to_owned();
                let writer = columns.next().unwrap().to_owned();
                let kind = columns.next().unwrap().to_owned();
                assert!(
                    columns.next().is_none(),
                    "four columns with a compact author type: {line}"
                );
                Some(((event_type, writer, kind), count))
            })
            .collect()
    }
    let mut expected_journals = std::collections::BTreeMap::<
        String,
        std::collections::BTreeMap<(String, String, String), usize>,
    >::new();
    let mut displayed_journals = expected_journals.clone();
    let registrations: std::collections::BTreeMap<_, _> = rows
        .iter()
        .filter_map(|row| match &row.record {
            RunRecordData::System(record) => match &record.payload {
                obzenflow::journal::read::SystemPayload::SupervisorRegistered { descriptor } => Some((record.writer_id().to_string(), descriptor)),
                _ => None,
            },
            RunRecordData::Chain(record) => match &record.payload {
                obzenflow_core::event::ChainPayload::Execution(obzenflow_core::event::payloads::execution_payload::ExecutionPayload::SupervisorRegistered { descriptor }) => Some((record.writer_id().to_string(), descriptor)),
                _ => None,
            },
        })
        .collect();
    assert_eq!(
        registrations.len(),
        8,
        "seven stage supervisors and pipeline register themselves"
    );
    let stage_names: std::collections::BTreeMap<_, _> = manifest["stages"]
        .as_object()
        .unwrap()
        .iter()
        .map(|(key, stage)| {
            (
                format!("writer_{}", stage["stage_id"].as_str().unwrap()),
                key.as_str(),
            )
        })
        .collect();
    for row in &rows {
        let heading = match row.journal.kind {
            obzenflow::journal::read::RunJournalKind::System => {
                manifest["system_journal_file"].as_str().unwrap().to_owned()
            }
            obzenflow::journal::read::RunJournalKind::MetricsCoordination => manifest
                ["metrics_journals"]["coordination_journal_file"]
                .as_str()
                .unwrap()
                .to_owned(),
            obzenflow::journal::read::RunJournalKind::MetricsExport => manifest["metrics_journals"]
                ["export_journal_file"]
                .as_str()
                .unwrap()
                .to_owned(),
            kind => {
                let stage = &row.journal.stage.as_ref().unwrap().key;
                let field = if kind == obzenflow::journal::read::RunJournalKind::Data {
                    "data_journal_file"
                } else {
                    "error_journal_file"
                };
                manifest["stages"][stage][field]
                    .as_str()
                    .unwrap()
                    .to_owned()
            }
        };
        let (event_type, writer) = match &row.record {
            RunRecordData::Chain(record) => {
                (record.event_type_name(), record.writer_id().to_string())
            }
            RunRecordData::System(record) => {
                (record.event_type_name(), record.writer_id().to_string())
            }
        };
        let descriptor = registrations[&writer];
        let writer_name = stage_names
            .get(&writer)
            .copied()
            .unwrap_or(&descriptor.name);
        let key = (
            event_type.to_owned(),
            writer_name.to_owned(),
            descriptor.kind.label().to_owned(),
        );
        *expected_journals
            .entry(heading.clone())
            .or_default()
            .entry(key.clone())
            .or_default() += 1;
        if matches!(
            row.kind,
            RunRecordKind::SourceFact
                | RunRecordKind::StageOutput
                | RunRecordKind::Effect
                | RunRecordKind::Delivery
        ) {
            *displayed_journals
                .entry(heading)
                .or_default()
                .entry(key)
                .or_insert(0usize) += 1;
        }
    }
    for (heading, expected) in &expected_journals {
        assert_eq!(
            &event_table(&verbose, heading),
            expected,
            "{heading} counts only its own journal entries, regardless of writer or payload stage"
        );
    }
    for (heading, expected) in &displayed_journals {
        assert_eq!(&event_table(&human, heading), expected);
    }
    assert_eq!(
        displayed_journals
            .values()
            .flat_map(|counts| counts.values())
            .sum::<usize>(),
        selected
    );
    assert!(
        !human.contains("\nSYSTEM SUPERVISORS\n"),
        "hidden journals need no event table"
    );
    for stage in manifest["stages"].as_object().unwrap().values() {
        let error_file = stage["error_journal_file"].as_str().unwrap();
        assert!(
            !verbose.contains(&format!("\n    {error_file}\n")),
            "empty journals stay in the inventory only"
        );
    }
    let system_counts = event_table(&verbose, "system.log");
    assert_eq!(system_counts.values().sum::<usize>(), system_count);
    for event_type in [
        "lifecycle.stage.running",
        "lifecycle.stage.completed",
        "execution.contract.pass",
    ] {
        assert_eq!(
            expected_journals
                .values()
                .flat_map(|counts| counts.iter())
                .filter(|((kind, _, _), _)| kind == event_type)
                .map(|(_, count)| count)
                .sum::<usize>(),
            7
        );
    }
    assert!(system_counts
        .keys()
        .all(|(_, _, author_type)| author_type == "Pipeline"));
    assert_eq!(
        system_counts[&(
            "system.pipeline.completed".into(),
            "pipeline_supervisor".into(),
            "Pipeline".into()
        )],
        1
    );
    let data_file = |stage: &str| {
        manifest["stages"][stage]["data_journal_file"]
            .as_str()
            .unwrap()
    };
    let manual_review = event_table(&verbose, data_file("manual_review"));
    for source in ["store_orders", "web_orders"] {
        assert_eq!(
            manual_review[&(
                "control.source_contract".into(),
                source.into(),
                "FiniteSource".into()
            )],
            1,
            "forwarded declarations retain their author, not the sink journal owner"
        );
    }
    assert_eq!(
        event_table(&verbose, data_file("validate_order"))[&(
            "order.cancelled.v1".into(),
            "validate_order".into(),
            "Transform".into()
        )],
        1
    );
    assert_eq!(
        event_table(&verbose, data_file("authorize_payment"))[&(
            "order.cancelled.v1".into(),
            "authorize_payment".into(),
            "Transform".into()
        )],
        1
    );
    assert!(
        !verbose.contains("By event type")
            && !verbose.contains("Stage journals")
            && !verbose.contains("System journals")
    );
    assert_eq!(
        human.lines().filter(|line| line.starts_with('⟨')).count(),
        selected,
        "one left-aligned output clock per selected record, including each grouped output"
    );
    assert!(human.contains(&format!(
        "{selected} displayed; {} runtime entries hidden",
        rows.len() - selected
    )));
    let (selected_jsonl, diagnostics) = show(&baseline, &["--jsonl"]);
    assert_json_summary(&selected_jsonl, &diagnostics);
    assert_eq!(selected_jsonl.lines().count(), selected);
    assert!(!verbose.contains("runtime entries hidden"));
    let (quiet, _) = show(&baseline, &["--compact"]);
    assert_record_outputs(&quiet, &rows);
    assert_eq!(
        quiet.lines().count(),
        selected + 2,
        "compact rows and two-line outcome summary"
    );
    assert!(quiet.contains("EFFECTFUL TRANSFORM  payment.authorized.v1 ←"));
    assert!(quiet
        .lines()
        .take(selected)
        .all(|line| line.chars().count() <= 90));
    assert!(!quiet.contains("Effects are data:") && !quiet.contains("MANIFEST"));
    assert!(!quiet.contains('⟨'), "quiet still omits clocks");
    let (quiet_verbose, _) = show(&baseline, &["--compact", "--include-runtime"]);
    assert_eq!(quiet_verbose.lines().count(), rows.len() + 2);
    let (detail, _) = show(&baseline, &["--full"]);
    assert_record_outputs(&detail, &rows);
    assert!(detail.contains("\"envelope\": {") && detail.contains("\"causality\": {"));
    assert_eq!(detail.matches("\"envelope\": {").count(), selected);
    let full_manifest = detail
        .split_once("MANIFEST     run_manifest.json\n")
        .unwrap()
        .1
        .split_once("\nJOURNALS\n")
        .unwrap()
        .0
        .trim();
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(full_manifest).unwrap(),
        manifest,
        "--full prints the complete recorded manifest, separate from journal counts"
    );
    let (detail_verbose, _) = show(&baseline, &["--full", "--include-runtime"]);
    assert_eq!(
        detail_verbose.matches("\"envelope\": {").count(),
        rows.len()
    );
    let (colored, _) = show(&baseline, &["--color", "always"]);
    assert_palette(&colored);

    FlowApplication::builder()
        .with_cli_args([
            OsString::from("gateway"),
            OsString::from("--replay-from"),
            baseline.as_os_str().to_owned(),
        ])
        .run_async(build_gateway(temp.path().to_path_buf()))
        .await
        .unwrap();
    let replay = latest_run_dir(temp.path());
    assert_ne!(baseline, replay);
    let (human, _) = show(&replay, &[]);
    assert_fact_origins(&human);
    assert!(
        human.split("\n\n").any(|block| {
            let text = block.split_whitespace().collect::<Vec<_>>().join(" ");
            text.contains("payment.authorized.v1 ← authorize_payment(payment.order_validated.v1)")
                && text.contains("[read from journal]")
        }),
        "{human}"
    );
    let (explained, _) = show(&replay, &["--explain"]);
    assert!(explained
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
        .contains("does not establish that the effect fired again"));
    let (detail, _) = show(&replay, &["--full"]);
    assert!(detail.contains("original_flow_id") && detail.contains("original_event_id"));
    let (colored, _) = show(&replay, &["--color", "always"]);
    assert_palette(&colored);
    let (json, diagnostics) = show(&replay, &["--jsonl"]);
    assert_json_summary(&json, &diagnostics);
    let rows: Vec<RunRecord> = json
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
    assert_record_outputs(&human, &rows);
}

#[test]
fn cli_version_commands_and_admission_errors_are_public() {
    let output = Command::new(env!("CARGO_BIN_EXE_obzenflow"))
        .arg("--version")
        .output()
        .unwrap();
    let version = String::from_utf8(output.stdout).unwrap();
    assert!(version.contains(env!("CARGO_PKG_VERSION")));
    assert!(version.contains(&format!(
        "journal schema {}",
        obzenflow::journal::JOURNAL_SCHEMA_VERSION
    )));
    let output = Command::new(env!("CARGO_BIN_EXE_obzenflow"))
        .arg("--help")
        .output()
        .unwrap();
    let help = String::from_utf8(output.stdout).unwrap();
    for command in ["show", "start", "journal", "verify"] {
        assert!(help.contains(command));
    }
    assert!(!help.contains("config"));
    let dir = tempfile::tempdir().unwrap();
    let output = Command::new(env!("CARGO_BIN_EXE_obzenflow"))
        .arg("show")
        .arg(dir.path())
        .arg("--jsonl")
        .output()
        .unwrap();
    assert_eq!(output.status.code(), Some(4));
    assert!(output.stdout.is_empty());
    assert_eq!(std::fs::read_dir(dir.path()).unwrap().count(), 0);
}

mod control_client {
    use super::*;
    use serde_json::{json, Value};
    use std::time::Duration;
    use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};

    fn target(host: &str) -> Value {
        json!({"runtime_instance_id":host,"pipeline_writer_id":{"type":"System","id":"01H00000000000000000000000"}})
    }

    fn discovery(archive: Value) -> Value {
        let value = json!({"protocol_version":1,"target":target("host-a"),"archive":archive});
        serde_json::from_value::<obzenflow::application::control::CurrentRunDiscovery>(
            value.clone(),
        )
        .expect("valid discovery fixture");
        value
    }

    fn result(host: &str, status: &str) -> Value {
        json!({"protocol_version":1,"target":target(host),"result":{"status":status,"message":"fixture control result","state":"ready_for_run"}})
    }

    struct RecordedRequest {
        request_line: String,
        body: Value,
        authorization: bool,
    }

    // A scripted HTTP peer tests transport failures at the executable boundary.
    // None drops the connection after receiving a request, modelling a lost reply.
    async fn peer(
        replies: Vec<(u16, Option<Value>)>,
    ) -> (String, tokio::task::JoinHandle<Vec<RecordedRequest>>) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let task = tokio::spawn(async move {
            let mut requests = vec![];
            let mut replies = replies.into_iter();
            while let Ok(Ok((stream, _))) =
                tokio::time::timeout(Duration::from_secs(1), listener.accept()).await
            {
                let mut reader = BufReader::new(stream);
                let mut first = String::new();
                reader.read_line(&mut first).await.unwrap();
                let mut length = 0;
                let mut authorization = false;
                loop {
                    let mut line = String::new();
                    reader.read_line(&mut line).await.unwrap();
                    if line == "\r\n" || line.is_empty() {
                        break;
                    }
                    if let Some((key, value)) = line.split_once(':') {
                        if key.eq_ignore_ascii_case("content-length") {
                            length = value.trim().parse().unwrap();
                        }
                        authorization |= key.eq_ignore_ascii_case("authorization");
                    }
                }
                let mut body = vec![0; length];
                reader.read_exact(&mut body).await.unwrap();
                requests.push(RecordedRequest {
                    request_line: first,
                    body: serde_json::from_slice(&body).unwrap_or(Value::Null),
                    authorization,
                });
                let (status, reply) = replies
                    .next()
                    .unwrap_or((500, Some(json!({"error":"unexpected extra request"}))));
                if let Some(reply) = reply {
                    let body = serde_json::to_vec(&reply).unwrap();
                    let header = format!("HTTP/1.1 {status} Fixture\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n", body.len());
                    let mut stream = reader.into_inner();
                    stream.write_all(header.as_bytes()).await.unwrap();
                    stream.write_all(&body).await.unwrap();
                    stream.shutdown().await.unwrap();
                }
            }
            requests
        });
        (url, task)
    }

    fn start_command(url: &str, follow: bool) -> tokio::process::Command {
        let mut command = tokio::process::Command::new(env!("CARGO_BIN_EXE_obzenflow"));
        command
            .args(["start", "--server", url, "--timeout-secs", "2"])
            .kill_on_drop(true);
        if follow {
            command.args(["--follow", "--jsonl"]);
        }
        command
    }

    async fn start(url: &str, follow: bool) -> std::process::Output {
        tokio::time::timeout(Duration::from_secs(5), start_command(url, follow).output())
            .await
            .unwrap()
            .unwrap()
    }

    #[tokio::test]
    async fn bare_start_accepts_ephemeral_and_sends_only_the_conditional_shape() {
        let (url, requests) = peer(vec![
            (
                200,
                Some(discovery(
                    json!({"kind":"unavailable","reason":"ephemeral"}),
                )),
            ),
            (200, Some(result("host-a", "accepted"))),
        ])
        .await;
        let output = start(&url, false).await;
        assert_eq!(
            output.status.code(),
            Some(0),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        let requests = requests.await.unwrap();
        assert_eq!(requests.len(), 2);
        assert!(requests[0].request_line.starts_with("GET /api/flow/run "));
        assert!(requests[1]
            .request_line
            .starts_with("POST /api/flow/control "));
        assert_eq!(requests[1].body["target"], target("host-a"));
        assert_eq!(requests[1].body["control"]["action"], "play");
        assert!(requests[1].body.get("action").is_none());
    }

    #[tokio::test]
    async fn local_control_ignores_credentials_and_bypasses_proxies() {
        let (url, requests) = peer(vec![
            (
                200,
                Some(discovery(
                    json!({"kind":"unavailable","reason":"ephemeral"}),
                )),
            ),
            (200, Some(result("host-a", "accepted"))),
        ])
        .await;
        let proxy = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let proxy_url = format!("http://{}", proxy.local_addr().unwrap());
        let mut command = start_command(&url, false);
        command.env("OBZENFLOW_CONTROL_AUTHORIZATION", "unused-test-credential");
        for name in ["HTTP_PROXY", "http_proxy", "ALL_PROXY", "all_proxy"] {
            command.env(name, &proxy_url);
        }
        command.env("NO_PROXY", "").env("no_proxy", "");
        let output = tokio::time::timeout(Duration::from_secs(5), command.output())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            output.status.code(),
            Some(0),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        let requests = requests.await.unwrap();
        assert_eq!(requests.len(), 2);
        assert!(requests.iter().all(|request| !request.authorization));
        assert!(
            tokio::time::timeout(Duration::from_millis(100), proxy.accept())
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn invalid_origin_is_refused_before_connecting() {
        let (url, requests) = peer(vec![]).await;
        let output = start(&format!("{url}/not-an-origin"), false).await;
        assert_eq!(output.status.code(), Some(4));
        assert!(String::from_utf8_lossy(&output.stderr).contains("loopback HTTP origin"));
        assert!(requests.await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn unavailable_follow_or_legacy_server_never_submits_play() {
        for (status, body, follow) in [
            (
                200,
                discovery(json!({"kind":"unavailable","reason":"unreadable"})),
                true,
            ),
            (404, json!({"error":"old server"}), false),
        ] {
            let (url, requests) = peer(vec![(status, Some(body))]).await;
            let output = start(&url, follow).await;
            assert_eq!(output.status.code(), Some(4));
            assert!(output.stdout.is_empty());
            assert_eq!(
                requests.await.unwrap().len(),
                1,
                "no legacy or unconditional Play fallback"
            );
        }
    }

    #[tokio::test]
    async fn stale_target_typed_rejection_and_lost_response_never_retry() {
        for (status, reply, diagnostic) in [
            (
                409,
                Some(result("host-b", "rejected")),
                "run_target_mismatch",
            ),
            (
                200,
                Some(result("host-a", "rejected")),
                "run_control_rejected",
            ),
            (200, None, "uncertain"),
        ] {
            let (url, requests) = peer(vec![
                (
                    200,
                    Some(discovery(
                        json!({"kind":"unavailable","reason":"ephemeral"}),
                    )),
                ),
                (status, reply),
            ])
            .await;
            let output = start(&url, false).await;
            assert_eq!(output.status.code(), Some(4));
            assert!(
                String::from_utf8_lossy(&output.stderr).contains(diagnostic),
                "{}",
                String::from_utf8_lossy(&output.stderr)
            );
            assert_eq!(
                requests.await.unwrap().len(),
                2,
                "control is never retried or retargeted"
            );
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn advertised_archive_identity_is_checked_before_play() {
        let dir = tempfile::tempdir().unwrap();
        FlowApplication::builder()
            .with_cli_args(["application"])
            .run_async(build_flow(dir.path().to_path_buf()))
            .await
            .unwrap();
        let path = latest_run_dir(dir.path());
        let encoded = obzenflow::application::control::NativeRunPath::encode(&path).unwrap();
        let (url, requests) = peer(vec![(200, Some(discovery(json!({"kind":"local_disk", "flow_id":"flow_01H000000000000000000000000", "path":encoded}))))]).await;
        let output = start(&url, true).await;
        assert_eq!(output.status.code(), Some(4));
        assert!(String::from_utf8_lossy(&output.stderr).contains("identity does not match"));
        assert_eq!(requests.await.unwrap().len(), 1);
    }
}

#[cfg(all(feature = "web-host", unix))]
mod hosted {
    use super::*;
    use obzenflow::application::control::{CurrentRunDiscovery, RunArchive, RUN_DISCOVERY_PATH};
    use obzenflow::journal::read::{open_disk_run, RunOutcome, RunRecord, RunRecordKind, TailRead};
    use std::process::Stdio;
    use std::time::Duration;
    use sysinfo::{Pid, PidExt, ProcessExt, Signal, System, SystemExt};

    const HOST_ROOT: &str = "OBZENFLOW_CLI_TEST_HOST_ROOT";
    const HOST_PORT: &str = "OBZENFLOW_CLI_TEST_HOST_PORT";

    #[test]
    #[ignore = "subprocess fixture launched by independent_lifetimes"]
    fn child_application() {
        let root = PathBuf::from(std::env::var_os(HOST_ROOT).expect("parent fixture root"));
        let port = std::env::var(HOST_PORT).unwrap();
        let definition = FlowDefinition::materialize(move |_| {
            let source = obzenflow::stages::sources::async_finite(|index| async move {
                tokio::time::sleep(Duration::from_millis(30)).await;
                Some(vec![Tick { n: index as u64 }])
            });
            let sink = SinkTyped::with_delivery(discard::<Tick>()).idempotent();
            Ok(flow! {
                name: "cli_process_lifetime", journals: disk_journals(root),
                stages: {
                    ticks = obzenflow::flow::async_source!(Tick => source);
                    out = sink!(Tick => sink);
                },
                topology: { ticks |> out; }
            })
        });
        FlowApplication::builder()
            .with_cli_args([
                "application",
                "--server",
                "--startup-mode",
                "manual",
                "--server-port",
                &port,
            ])
            .run_blocking(definition)
            .expect("application lifecycle");
    }

    fn signal(child: &tokio::process::Child, signal: Signal) {
        let pid = Pid::from_u32(child.id().unwrap());
        let mut system = System::new();
        assert!(system.refresh_process(pid));
        assert_eq!(system.process(pid).unwrap().kill_with(signal), Some(true));
    }

    async fn wait(child: &mut tokio::process::Child) -> std::process::ExitStatus {
        tokio::time::timeout(Duration::from_secs(15), child.wait())
            .await
            .expect("process exits within lifecycle budget")
            .unwrap()
    }

    async fn discover(
        client: &reqwest::Client,
        url: &str,
        child: &mut tokio::process::Child,
    ) -> CurrentRunDiscovery {
        tokio::time::timeout(Duration::from_secs(15), async {
            loop {
                assert!(
                    child.try_wait().unwrap().is_none(),
                    "application exited before admission"
                );
                if let Ok(response) = client
                    .get(format!("{url}{RUN_DISCOVERY_PATH}"))
                    .send()
                    .await
                {
                    assert_eq!(response.headers()["cache-control"], "no-store");
                    if let Ok(discovery) = response.json::<CurrentRunDiscovery>().await {
                        return discovery;
                    }
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("host discovery")
    }

    async fn first_fact(path: &Path, viewer: &mut tokio::process::Child) {
        tokio::time::timeout(Duration::from_secs(15), async {
            loop {
                assert!(
                    viewer.try_wait().unwrap().is_none(),
                    "viewer exited before source evidence"
                );
                let text = std::fs::read_to_string(path).unwrap();
                if text
                    .lines()
                    .filter_map(|line| serde_json::from_str::<RunRecord>(line).ok())
                    .any(|r| r.kind == RunRecordKind::SourceFact)
                {
                    return;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("follow emits committed source evidence");
    }

    // The application and viewer are independent children of this test. Neither
    // launches the other; these are real OS signals through the production host.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn independent_lifetimes_cover_detach_sigint_sigterm_and_sigkill() {
        for mode in ["detach", "reader_error", "sigint", "sigterm", "sigkill"] {
            let dir = tempfile::tempdir().unwrap();
            let reservation = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
            let port = reservation.local_addr().unwrap().port();
            drop(reservation);
            let url = format!("http://127.0.0.1:{port}");
            let host_log = dir.path().join("host.log");
            let mut application = tokio::process::Command::new(std::env::current_exe().unwrap())
                .args([
                    "--ignored",
                    "--exact",
                    "hosted::child_application",
                    "--nocapture",
                    "--test-threads=1",
                ])
                .env(HOST_ROOT, dir.path().join("journals"))
                .env(HOST_PORT, port.to_string())
                .stdout(Stdio::null())
                .stderr(std::fs::File::create(&host_log).unwrap())
                .kill_on_drop(true)
                .spawn()
                .unwrap();
            let client = reqwest::Client::builder()
                .timeout(Duration::from_secs(2))
                .build()
                .unwrap();
            let discovery = discover(&client, &url, &mut application).await;
            let RunArchive::LocalDisk { flow_id, path } = &discovery.archive else {
                panic!("host must admit its archive: {discovery:?}");
            };
            let path = path.decode().unwrap();
            let output = tokio::process::Command::new(env!("CARGO_BIN_EXE_obzenflow"))
                .arg("show")
                .arg(&path)
                .arg("--jsonl")
                .current_dir(dir.path())
                .output()
                .await
                .unwrap();
            assert_eq!(output.status.code(), Some(0));
            let mut snapshot = open_disk_run(&path).await.unwrap();
            assert_eq!(snapshot.identity().flow_id.to_string(), *flow_id);
            assert_eq!(
                snapshot.identity().pipeline_writer_id,
                discovery.target.pipeline_writer_id
            );
            while let Some(row) = snapshot.next().await.unwrap() {
                assert_ne!(
                    row.kind,
                    RunRecordKind::SourceFact,
                    "manual host must not run before Play"
                );
            }
            assert_eq!(
                client
                    .get(format!("{url}/ready"))
                    .send()
                    .await
                    .unwrap()
                    .status(),
                503
            );
            let rows = dir.path().join("viewer.jsonl");
            let diagnostics = dir.path().join("viewer.stderr");
            let mut viewer = tokio::process::Command::new(env!("CARGO_BIN_EXE_obzenflow"))
                .args([
                    "start",
                    "--server",
                    &url,
                    "--follow",
                    "--jsonl",
                    "--include-runtime",
                ])
                .stdout(std::fs::File::create(&rows).unwrap())
                .stderr(std::fs::File::create(&diagnostics).unwrap())
                .kill_on_drop(true)
                .spawn()
                .unwrap();
            first_fact(&rows, &mut viewer).await;
            assert_eq!(
                client
                    .get(format!("{url}/ready"))
                    .send()
                    .await
                    .unwrap()
                    .status(),
                200
            );
            let guidance = std::fs::read_to_string(&host_log).unwrap();
            let pid = application.id().unwrap();
            assert!(
                guidance.contains(&format!("kill -TERM {pid}"))
                    && guidance.contains(&format!("kill -KILL {pid}"))
            );
            match mode {
                "detach" => {
                    signal(&viewer, Signal::Interrupt);
                    assert_eq!(wait(&mut viewer).await.code(), Some(0));
                    assert!(application.try_wait().unwrap().is_none());
                    assert_eq!(
                        client
                            .get(format!("{url}/ready"))
                            .send()
                            .await
                            .unwrap()
                            .status(),
                        200
                    );
                    signal(&application, Signal::Term);
                }
                "sigint" => signal(&application, Signal::Interrupt),
                "reader_error" => {
                    let system = path.join("system.log");
                    let hidden = path.join("system.temporarily-unavailable");
                    let replacement = path.join("system.observer-copy");
                    // Replace the observer's admitted inode atomically while
                    // keeping the application's writer and live reads available.
                    // Removing its required journal also fails the application.
                    std::fs::copy(&system, &replacement).unwrap();
                    std::fs::hard_link(&system, &hidden).unwrap();
                    std::fs::rename(&replacement, &system).unwrap();
                    assert_eq!(wait(&mut viewer).await.code(), Some(4));
                    assert!(std::fs::read_to_string(&diagnostics)
                        .unwrap()
                        .contains("run_observation_failed"));
                    assert!(application.try_wait().unwrap().is_none());
                    assert_eq!(
                        client
                            .get(format!("{url}/ready"))
                            .send()
                            .await
                            .unwrap()
                            .status(),
                        200
                    );
                    std::fs::rename(&hidden, &system).unwrap();
                    signal(&application, Signal::Term);
                }
                "sigterm" => signal(&application, Signal::Term),
                "sigkill" => application.start_kill().unwrap(),
                _ => unreachable!(),
            }
            let _ = wait(&mut application).await;
            let mut tail = open_disk_run(&path).await.unwrap().into_tail();
            while matches!(tail.read_next().await.unwrap(), TailRead::Record(_)) {}
            if mode == "sigkill" {
                assert!(tail.progress().settled_prefix.is_none());
                tokio::time::sleep(Duration::from_millis(200)).await;
                assert!(
                    viewer.try_wait().unwrap().is_none(),
                    "SIGKILL cannot manufacture settlement"
                );
                signal(&viewer, Signal::Interrupt);
            } else {
                assert!(
                    tail.progress().settled_prefix.is_some(),
                    "{mode}: {:?}",
                    tail.progress()
                );
                if mode == "sigint" {
                    assert!(matches!(
                        tail.progress().outcome.as_ref().unwrap().outcome,
                        RunOutcome::Cancelled { .. }
                    ));
                }
            }
            if mode != "detach" && mode != "reader_error" {
                assert_eq!(
                    wait(&mut viewer).await.code(),
                    Some(0),
                    "{mode}: {}",
                    std::fs::read_to_string(&diagnostics).unwrap()
                );
            }
            let stdout = std::fs::read_to_string(&rows).unwrap();
            for line in stdout.lines() {
                serde_json::from_str::<RunRecord>(line).expect("stdout contains records only");
            }
            if mode != "reader_error" {
                assert_json_summary(&stdout, &std::fs::read_to_string(&diagnostics).unwrap());
            }
        }
    }
}
