#![cfg(skip)] // Disabled manual harness; kept as reference but not compiled as part of test suite

//! Manual harness for char_transform to verify graceful completion and contract evidence.
//! Run with: `cargo test --test char_transform_harness -- --ignored`

use anyhow::Result;
use async_trait::async_trait;
use obzenflow_core::{
    event::chain_event::{ChainEvent, ChainEventFactory},
    event::payloads::delivery_payload::{DeliveryMethod, DeliveryPayload},
    id::StageId,
    TypedPayload, WriterId,
};
use obzenflow_dsl_infra::{flow, sink, source, stateful, transform};
use obzenflow_infra::application::{FlowApplication, LogLevel};
use obzenflow_infra::journal::disk_journals;
use obzenflow_runtime_services::stages::common::handlers::{FiniteSourceHandler, SinkHandler};
use obzenflow_runtime_services::stages::stateful::strategies::accumulators::ReduceTyped;
use obzenflow_runtime_services::stages::transform::MapTyped;
use serde::{Deserialize, Serialize};
use std::fs;
use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

/// Source that emits individual characters from sample sentences
#[derive(Clone, Debug)]
struct TextCharSource {
    sentences: Vec<String>,
    current_sentence: usize,
    current_char: usize,
    writer_id: WriterId,
}

impl TextCharSource {
    fn new() -> Self {
        Self {
            sentences: vec![
                "hello 2024 world!".to_string(),
                "42 is the answer.".to_string(),
                "rust 1 python 0.".to_string(),
            ],
            current_sentence: 0,
            current_char: 0,
            writer_id: WriterId::from(StageId::new()),
        }
    }
}

impl FiniteSourceHandler for TextCharSource {
    fn next(
        &mut self,
    ) -> Result<
        Option<Vec<ChainEvent>>,
        obzenflow_runtime_services::stages::common::handlers::source::traits::SourceError,
    > {
        if self.current_sentence >= self.sentences.len() {
            return Ok(None);
        }

        let sentence = &self.sentences[self.current_sentence];
        let chars: Vec<char> = sentence.chars().collect();

        if self.current_char < chars.len() {
            let ch = chars[self.current_char];
            self.current_char += 1;

            let char_event = CharEvent {
                value: ch.to_string(),
                sentence_idx: self.current_sentence,
                char_idx: self.current_char - 1,
            };

            Ok(Some(vec![ChainEventFactory::typed_payload(
                self.writer_id,
                TypedPayload::new("char_event", char_event),
            )]))
        } else {
            self.current_sentence += 1;
            self.current_char = 0;
            self.next()
        }
    }
}

/// Typed character event
#[derive(Debug, Clone, Serialize, Deserialize)]
struct CharEvent {
    value: String,
    sentence_idx: usize,
    char_idx: usize,
}

/// Transformed character output
#[derive(Debug, Clone, Serialize, Deserialize)]
struct TransformedChar {
    value: String,
    is_newline: bool,
}

/// Stateful accumulator for transformed text
#[derive(Debug, Clone, Serialize, Deserialize)]
struct TextAccumulator {
    transformed_text: String,
    total_chars: usize,
}

impl TextAccumulator {
    fn apply(&mut self, ch: TransformedChar) {
        self.transformed_text.push_str(&ch.value);
        self.total_chars += 1;
    }
}

/// Sink that prints the final transformed text
#[derive(Clone)]
struct TextSink;

#[async_trait]
impl SinkHandler for TextSink {
    async fn consume(
        &mut self,
        event: ChainEvent,
    ) -> obzenflow_core::Result<DeliveryPayload> {
        // In the current delivery model, the accumulated state would be encoded
        // directly in the delivery payload, so for this harness we simply log
        // that we received a delivery and return a success receipt.
        println!("TextSink received event of type {}", event.event_type());

        Ok(DeliveryPayload::success(
            "stdout",
            DeliveryMethod::Custom("Print".to_string()),
            None,
        ))
    }
}

fn transform_char(ch: char) -> String {
    match ch {
        'a'..='z' => ch.to_ascii_uppercase().to_string(),
        '0' => "zero".to_string(),
        '1' => "one".to_string(),
        '2' => "two".to_string(),
        '3' => "three".to_string(),
        '4' => "four".to_string(),
        '5' => "five".to_string(),
        '6' => "six".to_string(),
        '7' => "seven".to_string(),
        '8' => "eight".to_string(),
        '9' => "nine".to_string(),
        _ => ch.to_string(),
    }
}

async fn run_char_transform(base: &Path) -> Result<()> {
    FlowApplication::builder()
        .with_log_level(LogLevel::Info)
        .run_async(async {
            flow! {
                name: "char_transform_harness",
                journals: disk_journals(base.to_path_buf()),
                middleware: [],

                stages: {
                    src = source!("source" => TextCharSource::new());

                    mapper = transform!("char_mapper" =>
                        MapTyped::new(|char_event: CharEvent| {
                            let ch = char_event.value.chars().next().unwrap_or(' ');
                            TransformedChar {
                                value: transform_char(ch),
                                is_newline: ch == '\n',
                            }
                        })
                    );

                    reducer = stateful!("text_accumulator" =>
                        ReduceTyped::new(
                            TextAccumulator {
                                transformed_text: String::new(),
                                total_chars: 0,
                            },
                            |state: &mut TextAccumulator, input: TransformedChar| {
                                state.apply(input);
                                None::<TransformedChar>
                            }
                        )
                    );

                    sink = sink!("stdout_sink" => TextSink);
                },

                links: {
                    src.output => mapper.input;
                    mapper.output => reducer.input;
                    reducer.output => sink.input;
                }
            }
        })
        .await
}

fn latest_system_log(base: &Path) -> Option<PathBuf> {
    let flows_dir = base.join("flows");
    let entries = fs::read_dir(&flows_dir).ok()?;
    let mut latest: Option<(SystemTime, PathBuf)> = None;
    for entry in entries.flatten() {
        let path = entry.path().join("system.log");
        if path.exists() {
            if let Ok(meta) = fs::metadata(&path) {
                if let Ok(modified) = meta.modified() {
                    if latest.is_none() || modified > latest.as_ref().unwrap().0 {
                        latest = Some((modified, path));
                    }
                }
            }
        }
    }
    latest.map(|(_, p)| p)
}

fn summarize_system_log(path: &Path) -> Result<LogSummary> {
    let file = fs::File::open(path)?;
    let reader = BufReader::new(file);
    let mut summary = LogSummary::default();
    for line in reader.lines() {
        let line = line?;
        if line.contains("\"contract_status\"") {
            summary.contract_total += 1;
            if line.contains("\"pass\":true") {
                summary.contract_pass += 1;
            } else {
                summary.contract_fail += 1;
            }
        }
        if line.contains("\"pipeline_lifecycle\":\"all_stages_completed\"") {
            summary.all_stages_completed += 1;
        }
    }
    Ok(summary)
}

#[derive(Default, Debug)]
struct LogSummary {
    contract_total: usize,
    contract_pass: usize,
    contract_fail: usize,
    all_stages_completed: usize,
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "Manual harness for char_transform; run with --ignored"]
async fn char_transform_completes_with_contracts() -> Result<()> {
    let suffix = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_else(|_| Duration::from_millis(0))
        .as_millis();
    let base = std::env::temp_dir().join(format!("char_transform_harness_{}", suffix));

    if base.exists() {
        fs::remove_dir_all(&base)?;
    }
    fs::create_dir_all(&base)?;

    run_char_transform(&base).await?;

    let system_log = latest_system_log(&base).expect("no system.log found");
    let summary = summarize_system_log(&system_log)?;

    assert!(
        summary.contract_fail == 0,
        "expected no contract failures, saw {}",
        summary.contract_fail
    );
    assert!(
        summary.contract_pass > 0,
        "expected contract passes, saw none"
    );
    assert!(
        summary.all_stages_completed > 0,
        "expected AllStagesCompleted in system.log"
    );

    Ok(())
}
