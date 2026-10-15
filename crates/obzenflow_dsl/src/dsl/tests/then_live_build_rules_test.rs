// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use crate::dsl::composites::lower_composites;
use crate::dsl::composition::{FlowMember, IntoFlowMember};
use crate::dsl::stage_descriptor::{StageDescriptor, BINDING_DERIVED_NAME_SENTINEL};
use crate::dsl::then_live::{plan, Refusal, ThenLivePlan};
use crate::dsl::topology::AuthoredConnection;
use obzenflow_core::event::context::StageType;
use obzenflow_core::journal::archive::DeliveryOrder;
use obzenflow_core::{StageId, TypedPayload};
use obzenflow_runtime::id_conversions::StageIdExt;
use obzenflow_runtime::stages::merges;
use obzenflow_topology::{DirectedEdge, EdgeKind, StageInfo, Topology};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};

#[derive(Clone, Debug, Serialize, Deserialize)]
struct Account {
    key: String,
}
impl TypedPayload for Account {
    const EVENT_TYPE: &'static str = "test.then_live.account";
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct Entry {
    key: String,
}
impl TypedPayload for Entry {
    const EVENT_TYPE: &'static str = "test.then_live.entry";
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct Posted {
    key: String,
}
impl TypedPayload for Posted {
    const EVENT_TYPE: &'static str = "test.then_live.posted";
}

macro_rules! join_with {
    ($ctor:ident, $catalog:ident: $c:ident, $s:ident) => {{
        let handler = obzenflow_runtime::stages::join::$ctor(
            |catalog: &$c| catalog.key.clone(),
            |stream: &$s| stream.key.clone(),
            |_catalog: $c, stream: $s| Posted { key: stream.key },
        );
        crate::join!(catalog $catalog: $c, $s -> Posted => handler)
    }};
}

struct Flow {
    descriptors: HashMap<String, Box<dyn StageDescriptor>>,
    name_to_id: HashMap<String, StageId>,
    topology: Topology,
}

fn topology_type(stage_type: StageType) -> obzenflow_topology::StageType {
    match stage_type {
        StageType::FiniteSource => obzenflow_topology::StageType::FiniteSource,
        StageType::InfiniteSource => obzenflow_topology::StageType::InfiniteSource,
        StageType::Transform => obzenflow_topology::StageType::Transform,
        StageType::Stateful => obzenflow_topology::StageType::Stateful,
        StageType::Join => obzenflow_topology::StageType::Join,
        StageType::Merge => obzenflow_topology::StageType::Merge,
        StageType::Sink => obzenflow_topology::StageType::Sink,
    }
}

fn flow(members: Vec<(&str, Box<dyn StageDescriptor>)>, authored: Vec<AuthoredConnection>) -> Flow {
    let members: HashMap<String, FlowMember> = members
        .into_iter()
        .map(|(binding, mut descriptor)| {
            if descriptor.name() == BINDING_DERIVED_NAME_SENTINEL {
                descriptor.set_name(binding.to_string());
            }
            (binding.to_string(), descriptor.into_flow_member())
        })
        .collect();
    let lowered = lower_composites(members, authored).unwrap_or_else(|error| panic!("{error}"));
    let mut descriptors = lowered.stages;
    let name_to_id: HashMap<String, StageId> = descriptors
        .keys()
        .map(|binding| (binding.clone(), StageId::new()))
        .collect();
    for (join, catalog) in &lowered.join_catalogs {
        let id = name_to_id[catalog];
        descriptors
            .get_mut(join)
            .expect("lowered join")
            .set_reference_stage_id(id);
    }
    let stages = descriptors
        .iter()
        .map(|(binding, descriptor)| {
            StageInfo::new(
                name_to_id[binding].to_topology_id(),
                descriptor.name().to_string(),
                topology_type(descriptor.stage_type()),
            )
        })
        .collect();
    let edges = lowered
        .connections
        .iter()
        .map(|(from, to, kind)| {
            DirectedEdge::new(
                name_to_id[from].to_topology_id(),
                name_to_id[to].to_topology_id(),
                *kind,
            )
        })
        .collect();
    let topology =
        Topology::new_unvalidated(stages, edges).unwrap_or_else(|error| panic!("{error:?}"));
    Flow {
        descriptors,
        name_to_id,
        topology,
    }
}

impl Flow {
    fn plan(&self) -> Result<ThenLivePlan, Refusal> {
        plan(&self.topology, &self.descriptors, &self.name_to_id)
    }

    fn refusal(&self) -> String {
        match self.plan() {
            Ok(_) => panic!("the build should refuse this flow"),
            Err(error) => error.to_string(),
        }
    }

    fn key(&self, id: StageId) -> &str {
        self.name_to_id
            .iter()
            .find(|(_, candidate)| **candidate == id)
            .map(|(binding, _)| binding.as_str())
            .expect("known stage")
    }

    fn edges(&self, pairs: &[(&str, &str)]) -> HashSet<(StageId, StageId)> {
        pairs
            .iter()
            .map(|(from, to)| (self.name_to_id[*from], self.name_to_id[*to]))
            .collect()
    }

    fn deadlock(&self, enforced: &[(&str, &str)]) -> Result<(), Refusal> {
        self.plan()
            .expect("plannable flow")
            .refuse_deadlocked_holds(
                &self.topology,
                &self.descriptors,
                &self.name_to_id,
                &self.edges(enforced),
            )
    }
}

fn edge(from: &str, to: &str) -> AuthoredConnection {
    AuthoredConnection::edge(from, to, EdgeKind::Forward)
}

fn feedback(from: &str, to: &str) -> AuthoredConnection {
    AuthoredConnection::edge(from, to, EdgeKind::Backward)
}

fn join(catalog: &str, stream: &str, join: &str) -> AuthoredConnection {
    AuthoredConnection::join(catalog, stream, join)
}

fn batch_accounts() -> Box<dyn StageDescriptor> {
    crate::source!(Account => placeholder!())
}
fn live_accounts() -> Box<dyn StageDescriptor> {
    crate::infinite_source!(Account => placeholder!())
}
fn batch_tx() -> Box<dyn StageDescriptor> {
    crate::source!(Entry => placeholder!())
}
fn live_tx() -> Box<dyn StageDescriptor> {
    crate::infinite_source!(Entry => placeholder!())
}

fn piggy_bank() -> Flow {
    flow(
        vec![
            ("batch_accounts", batch_accounts()),
            ("live_accounts", live_accounts()),
            ("batch_tx", batch_tx()),
            ("live_tx", live_tx()),
            ("accounts", crate::merge!(Account => merges::then_live)),
            ("transactions", crate::merge!(Entry => merges::then_live)),
            (
                "posted",
                join_with!(inner_then_live, accounts: Account, Entry),
            ),
        ],
        vec![
            edge("batch_accounts", "accounts"),
            edge("live_accounts", "accounts"),
            edge("batch_tx", "transactions"),
            edge("live_tx", "transactions"),
            join("accounts", "transactions", "posted"),
        ],
    )
}

#[test]
fn the_piggy_bank_plans_its_witnesses_and_merge_producers() {
    let flow = piggy_bank();
    let plan = flow.plan().unwrap_or_else(|error| panic!("{error}"));

    let (binding, witnesses) = &plan.witnesses[0];
    assert_eq!(binding, "posted");
    let keys = |side: &[obzenflow_runtime::stages::join::PhaseWitness]| {
        side.iter()
            .map(|witness| witness.stage_key.as_str().to_string())
            .collect::<Vec<_>>()
    };
    assert_eq!(keys(&witnesses.catalog), ["batch_accounts"]);
    assert_eq!(keys(&witnesses.batch_stream), ["batch_tx"]);
    assert_eq!(
        witnesses.catalog[0].stage_id,
        flow.name_to_id["batch_accounts"]
    );

    let producers: HashMap<&str, (Vec<&str>, Vec<&str>)> = plan
        .merge_producers
        .iter()
        .map(|(binding, batch, live)| {
            (
                binding.as_str(),
                (
                    batch.iter().map(|id| flow.key(*id)).collect(),
                    live.iter().map(|id| flow.key(*id)).collect(),
                ),
            )
        })
        .collect();
    assert_eq!(
        producers["accounts"],
        (vec!["batch_accounts"], vec!["live_accounts"])
    );
    assert_eq!(
        producers["transactions"],
        (vec!["batch_tx"], vec!["live_tx"])
    );
}

#[test]
fn a_join_with_no_batch_stream_input_has_an_empty_batch_phase() {
    let flow = flow(
        vec![
            ("batch_accounts", batch_accounts()),
            ("live_accounts", live_accounts()),
            ("live_tx", live_tx()),
            ("accounts", crate::merge!(Account => merges::then_live)),
            (
                "posted",
                join_with!(inner_then_live, accounts: Account, Entry),
            ),
        ],
        vec![
            edge("batch_accounts", "accounts"),
            edge("live_accounts", "accounts"),
            join("accounts", "live_tx", "posted"),
        ],
    );
    let plan = flow.plan().unwrap_or_else(|error| panic!("{error}"));
    assert!(plan.witnesses[0].1.batch_stream.is_empty());
}

#[test]
fn a_merge_without_a_live_producer_is_refused() {
    let flow = flow(
        vec![
            ("left", batch_tx()),
            ("right", batch_tx()),
            ("transactions", crate::merge!(Entry => merges::then_live)),
        ],
        vec![edge("left", "transactions"), edge("right", "transactions")],
    );
    assert_eq!(
        flow.refusal(),
        "merge `transactions` uses then_live, but it has no live producer, so it would never go \
         live.\nA then_live merge needs at least one batch and one live producer."
    );
}

#[test]
fn a_merge_without_a_batch_producer_is_refused() {
    let flow = flow(
        vec![
            ("left", live_tx()),
            ("right", live_tx()),
            ("transactions", crate::merge!(Entry => merges::then_live)),
        ],
        vec![edge("left", "transactions"), edge("right", "transactions")],
    );
    assert!(flow
        .refusal()
        .starts_with("merge `transactions` uses then_live, but it has no batch producer"));
}

#[test]
fn a_merge_producer_that_mixes_batch_and_live_is_refused() {
    let flow = flow(
        vec![
            ("batch_tx", batch_tx()),
            ("live_tx", live_tx()),
            ("more_live", live_tx()),
            (
                "tagged",
                crate::transform!(Entry -> Entry => placeholder!()),
            ),
            ("transactions", crate::merge!(Entry => merges::then_live)),
        ],
        vec![
            edge("batch_tx", "tagged"),
            edge("live_tx", "tagged"),
            edge("tagged", "transactions"),
            edge("more_live", "transactions"),
        ],
    );
    assert_eq!(
        flow.refusal(),
        "merge `transactions` uses then_live, but its producer `tagged` mixes batch and live \
         sources (`batch_tx`, `live_tx`).\nMerge the batch and live producers directly, and \
         transform each before the merge."
    );
}

#[test]
fn a_mixed_stream_input_other_than_a_merge_is_refused() {
    let flow = flow(
        vec![
            ("batch_accounts", batch_accounts()),
            ("batch_tx", batch_tx()),
            ("live_tx", live_tx()),
            (
                "tagged",
                crate::transform!(Entry -> Entry => placeholder!()),
            ),
            (
                "posted",
                join_with!(inner_then_live, batch_accounts: Account, Entry),
            ),
        ],
        vec![
            edge("batch_tx", "tagged"),
            edge("live_tx", "tagged"),
            join("batch_accounts", "tagged", "posted"),
        ],
    );
    assert_eq!(
        flow.refusal(),
        "join `posted` uses inner_then_live, but its stream input `tagged` mixes batch and live \
         sources (`batch_tx`, `live_tx`).\nFeed batch and live rows to the join through \
         `merge!(Entry => merges::then_live)` so the batch phase can end."
    );
}

#[test]
fn batch_and_live_stream_inputs_side_by_side_are_refused() {
    let flow = flow(
        vec![
            ("batch_accounts", batch_accounts()),
            ("batch_tx", batch_tx()),
            ("live_tx", live_tx()),
            (
                "posted",
                join_with!(inner_then_live, batch_accounts: Account, Entry),
            ),
        ],
        vec![
            join("batch_accounts", "batch_tx", "posted"),
            join("batch_accounts", "live_tx", "posted"),
        ],
    );
    assert_eq!(
        flow.refusal(),
        "join `posted` uses inner_then_live, but its stream inputs `batch_tx` and `live_tx` mix \
         batch and live sources.\nCombine them with `merge!(Entry => merges::then_live)` and \
         feed the merge to the join."
    );
}

fn chained_on_the_stream(merchants: Box<dyn StageDescriptor>, live_catalog: bool) -> Flow {
    let mut members = vec![
        ("batch_accounts", batch_accounts()),
        ("live_accounts", live_accounts()),
        ("batch_tx", batch_tx()),
        ("live_tx", live_tx()),
        ("accounts", crate::merge!(Account => merges::then_live)),
        ("transactions", crate::merge!(Entry => merges::then_live)),
        (
            "posted",
            join_with!(inner_then_live, accounts: Account, Entry),
        ),
        ("merchants", merchants),
        (
            "merchant_posted",
            join_with!(inner_then_live, merchants: Account, Posted),
        ),
    ];
    let mut authored = vec![
        edge("batch_accounts", "accounts"),
        edge("live_accounts", "accounts"),
        edge("batch_tx", "transactions"),
        edge("live_tx", "transactions"),
        join("accounts", "transactions", "posted"),
        join("merchants", "posted", "merchant_posted"),
    ];
    if live_catalog {
        members.push(("batch_merchants", batch_accounts()));
        members.push(("live_merchants", live_accounts()));
        authored.push(edge("batch_merchants", "merchants"));
        authored.push(edge("live_merchants", "merchants"));
    }
    flow(members, authored)
}

#[test]
fn a_then_live_join_on_the_stream_side_is_refused_with_the_batch_remedy() {
    let flow = chained_on_the_stream(batch_accounts(), false);
    assert_eq!(
        flow.refusal(),
        "join `merchant_posted` uses inner_then_live, but its stream input `posted` is a \
         then-live join, whose batch and live rows cannot be told apart yet.\nIf `merchants` is \
         batch only, use inner for `merchant_posted`."
    );
}

#[test]
fn the_batch_remedy_is_offered_only_for_a_batch_only_catalog() {
    let flow = chained_on_the_stream(crate::merge!(Account => merges::then_live), true);
    let refusal = flow.refusal();
    assert!(
        refusal.contains("its stream input `posted` is a then-live join"),
        "{refusal}"
    );
    assert!(!refusal.contains("use inner for"), "{refusal}");
}

#[test]
fn a_then_live_join_as_a_catalog_is_refused() {
    let mut flow_members = vec![
        ("batch_accounts", batch_accounts()),
        ("live_accounts", live_accounts()),
        ("batch_tx", batch_tx()),
        ("live_tx", live_tx()),
        ("more_tx", batch_tx()),
        ("accounts", crate::merge!(Account => merges::then_live)),
        ("transactions", crate::merge!(Entry => merges::then_live)),
    ];
    flow_members.push((
        "sku_full_dim",
        join_with!(inner_then_live, accounts: Account, Entry),
    ));
    flow_members.push((
        "enriched_orders",
        join_with!(inner_then_live, sku_full_dim: Posted, Entry),
    ));
    let flow = flow(
        flow_members,
        vec![
            edge("batch_accounts", "accounts"),
            edge("live_accounts", "accounts"),
            edge("batch_tx", "transactions"),
            edge("live_tx", "transactions"),
            join("accounts", "transactions", "sku_full_dim"),
            join("sku_full_dim", "more_tx", "enriched_orders"),
        ],
    );
    assert_eq!(
        flow.refusal(),
        "join `enriched_orders` uses inner_then_live, but its catalog `sku_full_dim` is a \
         then-live join, whose batch and live rows cannot be told apart yet."
    );
}

#[test]
fn a_catalog_with_no_batch_source_is_refused() {
    let flow = flow(
        vec![
            ("live_accounts", live_accounts()),
            ("batch_tx", batch_tx()),
            (
                "posted",
                join_with!(inner_then_live, live_accounts: Account, Entry),
            ),
        ],
        vec![join("live_accounts", "batch_tx", "posted")],
    );
    assert_eq!(
        flow.refusal(),
        "join `posted` uses inner_then_live, but its catalog `live_accounts` has no batch \
         source, so it would never leave its first phase.\nUse inner_live for a catalog that is \
         live from the start."
    );
}

#[test]
fn a_join_with_no_live_input_is_refused() {
    let flow = flow(
        vec![
            ("batch_accounts", batch_accounts()),
            ("batch_tx", batch_tx()),
            (
                "posted",
                join_with!(strict_then_live, batch_accounts: Account, Entry),
            ),
        ],
        vec![join("batch_accounts", "batch_tx", "posted")],
    );
    assert_eq!(
        flow.refusal(),
        "join `posted` uses strict_then_live, but none of its inputs has a live source, so it \
         would never go live.\nUse strict for a join whose inputs are all batch."
    );
}

#[test]
fn a_stateful_stage_between_the_frontier_and_the_catalog_is_refused() {
    let flow = flow(
        vec![
            ("batch_accounts", batch_accounts()),
            ("live_accounts", live_accounts()),
            ("live_tx", live_tx()),
            (
                "account_book",
                crate::stateful!(Account -> Account => placeholder!()),
            ),
            (
                "posted",
                join_with!(inner_then_live, account_book: Account, Entry),
            ),
        ],
        vec![
            edge("batch_accounts", "account_book"),
            edge("live_accounts", "account_book"),
            join("account_book", "live_tx", "posted"),
        ],
    );
    assert_eq!(
        flow.refusal(),
        "join `posted` uses inner_then_live, but its catalog mixes batch and live sources in \
         `account_book`, a stateful stage, so the batch phase cannot tell when every batch row \
         has arrived.\nMerge batch and live rows with `merge!(Account => merges::then_live)`, \
         and keep stateful work on paths that are all batch or all live."
    );
}

#[test]
fn a_frontier_stage_with_two_paths_to_the_catalog_is_refused() {
    let flow = flow(
        vec![
            ("batch_accounts", batch_accounts()),
            ("live_accounts", live_accounts()),
            ("live_tx", live_tx()),
            (
                "accounts_clean",
                crate::transform!(Account -> Account => placeholder!()),
            ),
            (
                "by_region",
                crate::transform!(Account -> Account => placeholder!()),
            ),
            (
                "by_tier",
                crate::transform!(Account -> Account => placeholder!()),
            ),
            (
                "all_accounts",
                crate::transform!(Account -> Account => placeholder!()),
            ),
            (
                "posted",
                join_with!(inner_then_live, all_accounts: Account, Entry),
            ),
        ],
        vec![
            edge("batch_accounts", "accounts_clean"),
            edge("accounts_clean", "by_region"),
            edge("accounts_clean", "by_tier"),
            edge("live_accounts", "by_region"),
            edge("live_accounts", "by_tier"),
            edge("by_region", "all_accounts"),
            edge("by_tier", "all_accounts"),
            join("all_accounts", "live_tx", "posted"),
        ],
    );
    assert_eq!(
        flow.refusal(),
        "join `posted` uses inner_then_live, but batch rows from `accounts_clean` reach its \
         catalog by two paths (`by_region`, `by_tier`).\nMerge those branches before they meet \
         live sources."
    );
}

#[test]
fn a_diamond_above_the_frontier_is_admitted() {
    let flow = flow(
        vec![
            ("batch_accounts", batch_accounts()),
            ("live_accounts", live_accounts()),
            ("live_tx", live_tx()),
            (
                "by_region",
                crate::transform!(Account -> Account => placeholder!()),
            ),
            (
                "by_tier",
                crate::transform!(Account -> Account => placeholder!()),
            ),
            ("all_accounts", crate::merge!(Account => merges::then_live)),
            (
                "posted",
                join_with!(inner_then_live, all_accounts: Account, Entry),
            ),
        ],
        vec![
            edge("batch_accounts", "by_region"),
            edge("batch_accounts", "by_tier"),
            edge("by_region", "all_accounts"),
            edge("by_tier", "all_accounts"),
            edge("live_accounts", "all_accounts"),
            join("all_accounts", "live_tx", "posted"),
        ],
    );
    let plan = flow.plan().unwrap_or_else(|error| panic!("{error}"));
    let catalog: Vec<&str> = plan.witnesses[0]
        .1
        .catalog
        .iter()
        .map(|witness| witness.stage_key.as_str())
        .collect();
    assert_eq!(catalog, ["by_region", "by_tier"]);
}

#[test]
fn batch_and_live_sources_meeting_inside_a_cycle_are_refused() {
    let flow = flow(
        vec![
            ("batch_accounts", batch_accounts()),
            ("live_accounts", live_accounts()),
            ("live_tx", live_tx()),
            (
                "all_accounts",
                crate::transform!(Account -> Account => placeholder!()),
            ),
            (
                "reconcile",
                crate::transform!(Account -> Account => placeholder!()),
            ),
            (
                "posted",
                join_with!(inner_then_live, all_accounts: Account, Entry),
            ),
        ],
        vec![
            edge("batch_accounts", "all_accounts"),
            edge("live_accounts", "all_accounts"),
            edge("all_accounts", "reconcile"),
            feedback("reconcile", "all_accounts"),
            join("all_accounts", "live_tx", "posted"),
        ],
    );
    assert_eq!(
        flow.refusal(),
        "join `posted` uses inner_then_live, but its catalog's batch and live sources meet \
         inside a cycle (`all_accounts <| reconcile`).\nMerge batch and live rows outside the \
         cycle."
    );
}

fn export_feeds_both_sides() -> Flow {
    let handler = obzenflow_runtime::stages::join::inner(
        |account: &Account| account.key.clone(),
        |entry: &Entry| entry.key.clone(),
        |_account: Account, entry: Entry| Posted { key: entry.key },
    );
    flow(
        vec![
            ("export", batch_accounts()),
            (
                "catalog",
                crate::transform!(Account -> Account => placeholder!()),
            ),
            (
                "ledger_rows",
                crate::transform!(Account -> Entry => placeholder!()),
            ),
            (
                "posted",
                crate::join!(catalog catalog: Account, Entry -> Posted => handler),
            ),
        ],
        vec![
            edge("export", "catalog"),
            edge("export", "ledger_rows"),
            join("catalog", "ledger_rows", "posted"),
        ],
    )
}

#[test]
fn a_hydration_that_could_never_end_under_enforce_is_refused() {
    let flow = export_feeds_both_sides();
    let refusal = flow
        .deadlock(&[("export", "ledger_rows"), ("ledger_rows", "posted")])
        .expect_err("an enforced path from export to the held input deadlocks")
        .to_string();
    assert_eq!(
        refusal,
        "join `posted` would deadlock while hydrating under enforced backpressure.\n`export` \
         must keep flowing for hydration to finish, but it also feeds the held input \
         `ledger_rows` through enforced edges (export |> ledger_rows |> posted).\nOnce those \
         windows fill, `export` blocks and never reaches end of input.\nSet one edge on that \
         path to `runtime.backpressure.mode = \"track\"`,\nor feed the held input from a \
         separate source."
    );
}

#[test]
fn one_tracked_edge_on_the_path_admits_the_hold() {
    let flow = export_feeds_both_sides();
    flow.deadlock(&[("export", "ledger_rows")])
        .expect("a tracked edge into the join leaves room for hydration");
    flow.deadlock(&[("ledger_rows", "posted")])
        .expect("a tracked edge out of export leaves room for hydration");
}

#[test]
fn the_piggy_bank_never_deadlocks_under_enforce() {
    let flow = piggy_bank();
    flow.deadlock(&[
        ("batch_accounts", "accounts"),
        ("live_accounts", "accounts"),
        ("batch_tx", "transactions"),
        ("live_tx", "transactions"),
        ("accounts", "posted"),
        ("transactions", "posted"),
    ])
    .expect("separate sources feed each held input");
}

#[test]
fn a_merge_records_only_a_race_between_live_producers() {
    let flow = flow(
        vec![
            ("batch_a", batch_tx()),
            ("batch_b", batch_tx()),
            ("live_a", live_tx()),
            ("live_b", live_tx()),
            ("one_each", crate::merge!(Entry => merges::then_live)),
            ("two_batch", crate::merge!(Entry => merges::then_live)),
            ("two_live", crate::merge!(Entry => merges::then_live)),
        ],
        vec![
            edge("batch_a", "one_each"),
            edge("live_a", "one_each"),
            edge("batch_a", "two_batch"),
            edge("batch_b", "two_batch"),
            edge("live_a", "two_batch"),
            edge("batch_a", "two_live"),
            edge("live_a", "two_live"),
            edge("live_b", "two_live"),
        ],
    );
    let orders = crate::dsl::typing::derive_delivery_orders(
        &flow.topology,
        &flow.descriptors,
        &flow.name_to_id,
        &HashSet::new(),
    );
    let order = |binding: &str| orders[&flow.name_to_id[binding]];
    assert_eq!(order("one_each"), DeliveryOrder::Structural);
    assert_eq!(order("two_batch"), DeliveryOrder::Canonical);
    assert_eq!(order("two_live"), DeliveryOrder::Recorded);
}
