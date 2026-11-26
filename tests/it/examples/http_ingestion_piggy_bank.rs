// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

#[path = "../../../examples/http_ingestion_piggy_bank_demo/support.rs"]
pub mod support;

use obzenflow::commands::perform;
use obzenflow::effects::EffectError;
use obzenflow::events::{source_channel, subscribe};
use obzenflow::systems::Port;
use obzenflow_dsl::{flow, sink, source, FlowDefinition};
use obzenflow_infra::journal::memory_journals;
use serde::Serialize;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;
use support::domain::AchDebit;
use support::flow::AchIngress;

#[derive(Clone, Debug, Serialize)]
struct Seen(AchDebit);

struct Debits {
    seen: Arc<Mutex<Vec<AchDebit>>>,
}

#[obzenflow::port]
impl Debits {
    #[effect(type = "piggy_bank.ach_debit_seen", at_least_once, records = reply)]
    fn seen(&self, seen: &Seen) -> Result<(), EffectError> {
        self.seen
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push(seen.0.clone());
        Ok(())
    }
}

fn debit(account: &str, amount: &str) -> AchDebit {
    serde_json::from_value(serde_json::json!({
        "account": account,
        "company_name": "NORTHSIDE FIT",
        "company_id": "1834577921",
        "entry_description": "RETRY PYMT",
        "amount": amount,
    }))
    .expect("an ACH debit decodes")
}

#[tokio::test]
async fn ach_line_over_channel_records_debits() {
    let seen = Arc::new(Mutex::new(Vec::new()));
    let output = perform(|debit: &AchDebit| Seen(debit.clone())).through(Port::to(Debits {
        seen: Arc::clone(&seen),
    }));
    let (sender, debits) = source_channel::<AchDebit>(4);
    let handle = FlowDefinition::materialize(move |_| {
        let live_ach = subscribe(AchIngress).through(Port::to(debits));
        Ok(flow! {
            name: "piggy_bank_ach_over_channel",
            journals: memory_journals(),
            stages: {
                live_ach = source!(AchDebit => live_ach);
                seen = sink!(AchDebit => output);
            },
            topology: {
                live_ach |> seen;
            }
        })
    })
    .build(obzenflow_runtime::run_context::FlowBuildContext::for_tests())
    .await
    .expect("the flow builds");
    handle.start().await.expect("the flow starts");

    let sent = [
        debit("chk-58213", "49.99"),
        debit("chk-58213", "0.00"),
        debit("chk-77120", "12.50"),
    ];
    for value in sent.iter().cloned() {
        sender.send(value).await.expect("the channel has space");
    }
    tokio::time::timeout(Duration::from_secs(10), async {
        while seen.lock().unwrap_or_else(PoisonError::into_inner).len() < 2 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("the line delivers both valid debits");
    handle.stop_cancel().await.expect("the flow stops");
    tokio::time::timeout(Duration::from_secs(10), handle.wait_for_completion())
        .await
        .expect("the flow completes")
        .expect("the flow ends cleanly");

    let delivered = seen.lock().unwrap_or_else(PoisonError::into_inner).clone();
    let expected = [&sent[0], &sent[2]].map(|debit| serde_json::to_value(debit).unwrap());
    let delivered = delivered
        .iter()
        .map(|debit| serde_json::to_value(debit).unwrap())
        .collect::<Vec<_>>();
    assert_eq!(delivered, expected, "the zero-amount debit is refused");
}
