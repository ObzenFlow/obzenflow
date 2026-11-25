// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use anyhow::Result;
use obzenflow::commands::perform;
use obzenflow::effects::EffectError;
use obzenflow::queries::{query, FiniteRead, QueryContext, QueryError};
use obzenflow::schema::TypedPayload;
use obzenflow::systems::Port;
use obzenflow_dsl::{flow, sink, source, FlowDefinition};
use obzenflow_infra::journal::disk_journals;
use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime};

#[derive(Clone, Debug, Serialize, Deserialize)]
struct Position {
    next: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct Tick {
    n: u64,
}

impl TypedPayload for Tick {
    const EVENT_TYPE: &'static str = "query_line.tick";
}

struct Ticks;

#[obzenflow::port]
impl Ticks {
    #[query(type = "query_line.ticks.read")]
    async fn read(
        &self,
        from: &Position,
        _ctx: &QueryContext,
    ) -> Result<FiniteRead<Tick, Position>, QueryError> {
        Ok(match from.next {
            3 => FiniteRead::EndOfInput,
            n => FiniteRead::Batch {
                records: vec![Tick { n }],
                next: Position { next: n + 1 },
            },
        })
    }
}

#[derive(Clone, Debug, Serialize)]
struct Seen;

struct Counter {
    seen: Arc<AtomicU64>,
}

#[obzenflow::port]
impl Counter {
    #[effect(type = "query_line.seen", at_least_once, records = reply)]
    fn seen(&self, _: &Seen) -> Result<(), EffectError> {
        self.seen.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }
}

fn unique_journal_dir(prefix: &str) -> std::path::PathBuf {
    let suffix = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap_or_else(|_| Duration::from_millis(0))
        .as_nanos();
    std::path::PathBuf::from("target").join(format!("{prefix}_{suffix}"))
}

#[tokio::test]
async fn query_line_delivers_every_record() -> Result<()> {
    let seen = Arc::new(AtomicU64::new(0));
    let counted = perform(|_: &Tick| Seen).through(Port::to(Counter { seen: seen.clone() }));
    let journal_root = unique_journal_dir("query_line_source");

    let handle = FlowDefinition::materialize(move |_runtime_config| {
        let ticks = query(Position { next: 0 }).through(Port::to(Ticks));

        Ok(flow! {
            name: "query_line_source_test",
            journals: disk_journals(journal_root),

            stages: {
                ticks = source!(Tick => ticks);
                counted = sink!(Tick => counted);
            },

            topology: {
                ticks |> counted;
            }
        })
    })
    .build(crate::replay_testkit::unsynced_context())
    .await
    .map_err(|e| anyhow::anyhow!("Failed to create flow: {e:?}"))?;

    handle.run().await?;

    assert_eq!(seen.load(Ordering::Relaxed), 3);
    Ok(())
}
