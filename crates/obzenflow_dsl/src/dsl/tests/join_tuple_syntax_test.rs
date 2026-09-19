// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! FLOWIP-133j: authoring evidence survives binding and composite lowering.

use crate::dsl::composites::lower_composites;
use crate::dsl::composition::{
    CompositeBuildContext, CompositeBuildError, CompositeDescriptor, FlowMember, IntoFlowMember,
};
use crate::dsl::topology::AuthoredConnection as Connection;
use obzenflow_core::TypedPayload;
use obzenflow_topology::EdgeKind;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Clone, Debug, Serialize, Deserialize)]
struct Row;
impl TypedPayload for Row {
    const EVENT_TYPE: &'static str = "test.join-tuple.row";
}
#[derive(Clone, Debug, Serialize, Deserialize)]
struct Reference;
impl TypedPayload for Reference {
    const EVENT_TYPE: &'static str = "test.join-tuple.reference";
}

fn members() -> HashMap<String, FlowMember> {
    HashMap::from([
        (
            "catalog".into(),
            crate::source!(Row => placeholder!()).into_flow_member(),
        ),
        (
            "stream".into(),
            crate::source!(Row => placeholder!()).into_flow_member(),
        ),
        (
            "other".into(),
            crate::source!(Row => placeholder!()).into_flow_member(),
        ),
        (
            "joined".into(),
            crate::join!(name: "display-name", catalog catalog: Row, Row -> Row => placeholder!())
                .into_flow_member(),
        ),
    ])
}

fn forward(from: &str, to: &str) -> Connection {
    Connection::edge(from, to, EdgeKind::Forward)
}

#[test]
fn tuples_retain_roles_and_only_shared_catalog_edges_coalesce_in_authored_order() {
    let mut authored = Vec::new();
    crate::parse_topology!(authored,
        (catalog, stream) |> joined;
        (catalog, other) |> joined;
    );
    assert_eq!(authored.len(), 2);
    let lowered = lower_composites(members(), authored).unwrap_or_else(|error| panic!("{error}"));
    assert_eq!(
        lowered.connections,
        vec![
            ("catalog".into(), "joined".into(), EdgeKind::Forward),
            ("stream".into(), "joined".into(), EdgeKind::Forward),
            ("other".into(), "joined".into(), EdgeKind::Forward),
        ]
    );
    assert_eq!(lowered.join_catalogs["joined"], "catalog");
    assert_eq!(lowered.stages["joined"].name(), "display-name");
}

#[test]
fn invalid_join_authoring_is_rejected_before_flattening() {
    for (authored, diagnostic) in [
        (vec![], "requires explicit"),
        (vec![forward("stream", "joined")], "plain edge"),
        (
            vec![forward("catalog", "joined"), forward("stream", "joined")],
            "plain edge",
        ),
        (
            vec![
                Connection::join("catalog", "stream", "joined"),
                forward("other", "joined"),
            ],
            "plain edge",
        ),
        (
            vec![Connection::join("stream", "catalog", "joined")],
            "declares catalog 'catalog'",
        ),
        (
            vec![Connection::join("other", "stream", "joined")],
            "declares catalog 'catalog'",
        ),
        (
            vec![Connection::join("catalog", "catalog", "joined")],
            "both catalog and stream",
        ),
        (
            vec![
                Connection::join("catalog", "stream", "joined"),
                Connection::join("catalog", "stream", "joined"),
            ],
            "duplicate join tuple",
        ),
        (
            vec![Connection::join("catalog", "stream", "other")],
            "not a declared join",
        ),
        (
            vec![Connection::join("catalog", "missing", "joined")],
            "unknown binding 'missing'",
        ),
        (
            vec![Connection::join("missing", "stream", "joined")],
            "unknown binding 'missing'",
        ),
        (
            vec![Connection::join("catalog", "stream", "missing")],
            "unknown binding 'missing'",
        ),
        (
            vec![Connection::edge("catalog", "joined", EdgeKind::Backward)],
            "requires explicit",
        ),
    ] {
        let error = match lower_composites(members(), authored) {
            Ok(_) => panic!("expected {diagnostic}"),
            Err(error) => error,
        };
        assert!(error.to_string().contains(diagnostic), "{error}");
    }
}

#[derive(Debug)]
struct Split;
impl CompositeDescriptor for Split {
    fn name(&self) -> &str {
        self.kind()
    }
    fn set_name(&mut self, _name: String) {}
    fn schema_version(&self) -> u32 {
        1
    }
    fn kind(&self) -> &'static str {
        "split"
    }
    fn expand(self: Box<Self>, ctx: &mut CompositeBuildContext) -> Result<(), CompositeBuildError> {
        ctx.member("input")
            .descriptor(crate::transform!(Row -> Row => placeholder!()));
        ctx.member("catalog")
            .descriptor(crate::transform!(Row -> Reference => placeholder!()));
        ctx.member("stream")
            .descriptor(crate::transform!(Row -> Row => placeholder!()));
        ctx.edge("input", "catalog");
        ctx.edge("input", "stream");
        ctx.boundary()
            .input("input", "input")
            .payload::<Row>()
            .output("catalog", "catalog")
            .payload::<Reference>()
            .output("stream", "stream")
            .payload::<Row>()
            .default();
        Ok(())
    }
}

#[test]
fn composite_catalog_and_stream_outputs_resolve_against_their_own_role_types() {
    let members = HashMap::from([
        ("split".into(), FlowMember::Composite(Box::new(Split))),
        (
            "joined".into(),
            crate::join!(catalog split: Reference, Row -> Row => placeholder!()).into_flow_member(),
        ),
    ]);
    let lowered = lower_composites(members, vec![Connection::join("split", "split", "joined")])
        .unwrap_or_else(|error| panic!("{error}"));
    assert_eq!(lowered.join_catalogs["joined"], "split__catalog");
    for (producer, port) in [("split__catalog", "catalog"), ("split__stream", "stream")] {
        assert!(lowered.connections.contains(&(
            producer.into(),
            "joined".into(),
            EdgeKind::Forward
        )));
        let binding = lowered
            .artifacts
            .boundary_edges
            .iter()
            .find(|edge| edge.from_stage == producer)
            .unwrap();
        assert_eq!(binding.ports.len(), 1);
        assert_eq!(binding.ports[0].port_name, port);
    }
}

#[test]
fn composite_same_resolved_producer_is_rejected() {
    let members = HashMap::from([
        ("split".into(), FlowMember::Composite(Box::new(Split))),
        (
            "joined".into(),
            crate::join!(catalog split: Row, Row -> Row => placeholder!()).into_flow_member(),
        ),
    ]);
    let error = lower_composites(members, vec![Connection::join("split", "split", "joined")])
        .err()
        .unwrap();
    assert!(
        error.to_string().contains("same producer 'split__stream'"),
        "{error}"
    );
}

#[derive(Debug)]
struct PrivateJoin {
    join_input: bool,
    explicit: bool,
}
impl CompositeDescriptor for PrivateJoin {
    fn name(&self) -> &str {
        self.kind()
    }
    fn set_name(&mut self, _name: String) {}
    fn schema_version(&self) -> u32 {
        1
    }
    fn kind(&self) -> &'static str {
        "private-join"
    }
    fn expand(self: Box<Self>, ctx: &mut CompositeBuildContext) -> Result<(), CompositeBuildError> {
        ctx.member("input")
            .descriptor(crate::transform!(Row -> Row => placeholder!()));
        ctx.member("catalog")
            .descriptor(crate::source!(Reference => placeholder!()));
        ctx.member("joined")
            .descriptor(crate::join!(catalog catalog: Reference, Row -> Row => placeholder!()));
        if self.explicit {
            ctx.join("catalog", "input", "joined");
        } else {
            ctx.edge("input", "joined");
        }
        ctx.boundary()
            .input("input", if self.join_input { "joined" } else { "input" })
            .payload::<Row>()
            .output("output", "joined")
            .payload::<Row>()
            .default();
        Ok(())
    }
}

fn private_members(join_input: bool, explicit: bool) -> HashMap<String, FlowMember> {
    HashMap::from([(
        "private".into(),
        FlowMember::Composite(Box::new(PrivateJoin {
            join_input,
            explicit,
        })),
    )])
}

#[test]
fn join_owned_composite_inputs_fail_even_with_explicit_internal_catalog_wiring() {
    for explicit in [false, true] {
        let error = lower_composites(private_members(true, explicit), vec![])
            .err()
            .unwrap();
        let message = error.to_string();
        assert!(
            message.contains("composite 'private'")
                && message.contains("port 'input'")
                && message.contains("join member 'private__joined'"),
            "{message}"
        );
    }
}

#[test]
fn private_joins_require_explicit_wiring_and_keep_local_catalog_identity() {
    let error = lower_composites(private_members(false, false), vec![])
        .err()
        .unwrap();
    assert!(error.to_string().contains("plain edge"), "{error}");
    let lowered = lower_composites(private_members(false, true), vec![])
        .unwrap_or_else(|error| panic!("{error}"));
    assert_eq!(lowered.join_catalogs["private__joined"], "private__catalog");
    assert_eq!(lowered.connections.len(), 2);
    for authored in [
        forward("private__joined", "private"),
        forward("private", "private__input"),
        Connection::join("private", "private", "private"),
    ] {
        assert!(lower_composites(private_members(false, true), vec![authored]).is_err());
    }
}
