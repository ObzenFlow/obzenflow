// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! Authored topology, before binding names or join roles can be erased.

use super::composites::LoweringArtifacts;
use super::stage_descriptor::StageDescriptor;
use obzenflow_core::event::context::StageType;
use obzenflow_topology::EdgeKind;
use std::collections::{HashMap, HashSet};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AuthoredConnection {
    Edge {
        from: String,
        to: String,
        kind: EdgeKind,
    },
    Join {
        catalog: String,
        stream: String,
        join: String,
    },
}

impl AuthoredConnection {
    pub fn edge(from: impl Into<String>, to: impl Into<String>, kind: EdgeKind) -> Self {
        Self::Edge {
            from: from.into(),
            to: to.into(),
            kind,
        }
    }

    pub fn join(
        catalog: impl Into<String>,
        stream: impl Into<String>,
        join: impl Into<String>,
    ) -> Self {
        Self::Join {
            catalog: catalog.into(),
            stream: stream.into(),
            join: join.into(),
        }
    }
}

/// Only lowering can construct this value. Stage descriptors, physical edges,
/// and the validated catalog identities move into the builder together.
pub struct LoweredFlow {
    pub(crate) stages: HashMap<String, Box<dyn StageDescriptor>>,
    pub(crate) connections: Vec<(String, String, EdgeKind)>,
    pub(crate) artifacts: LoweringArtifacts,
    pub(crate) join_catalogs: HashMap<String, String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum JoinRole {
    Catalog,
    Stream,
}

pub(crate) struct RoleEdge {
    pub from: String,
    pub to: String,
    pub kind: EdgeKind,
    pub role: Option<JoinRole>,
}

/// Logical binding validation is shared by public flows and private composite
/// expansions. A `None` descriptor denotes an outer composite binding.
pub(crate) fn validate_authored(
    connections: &[AuthoredConnection],
    members: &HashMap<&str, Option<&dyn StageDescriptor>>,
) -> Result<(), String> {
    let known = |name: &str| {
        if members.contains_key(name) {
            Ok(())
        } else {
            Err(format!("topology references unknown binding '{name}'"))
        }
    };
    let mut tuples = HashSet::new();
    let mut wired = HashSet::new();
    for connection in connections {
        match connection {
            AuthoredConnection::Edge { from, to, kind } => {
                known(from)?;
                known(to)?;
                if *kind == EdgeKind::Forward
                    && members[to.as_str()]
                        .is_some_and(|stage| stage.stage_type() == StageType::Join)
                {
                    return Err(format!("join '{to}' requires every forward input to use (catalog, stream) |> {to}; plain edge '{from} |> {to}' is not allowed"));
                }
            }
            AuthoredConnection::Join {
                catalog,
                stream,
                join,
            } => {
                known(catalog)?;
                known(stream)?;
                known(join)?;
                let descriptor = members[join.as_str()]
                    .filter(|stage| stage.stage_type() == StageType::Join)
                    .ok_or_else(|| format!("tuple target '{join}' is not a declared join"))?;
                let expected = descriptor
                    .reference_stage_name()
                    .ok_or_else(|| format!("join '{join}' has no catalog binding witness"))?;
                if catalog != expected {
                    return Err(format!("join '{join}' declares catalog '{expected}', but tuple catalog is '{catalog}'; use ({expected}, stream) |> {join}"));
                }
                if catalog == stream && members[catalog.as_str()].is_some() {
                    return Err(format!(
                        "join '{join}' cannot use producer '{catalog}' for both catalog and stream"
                    ));
                }
                if !tuples.insert((catalog, stream, join)) {
                    return Err(format!(
                        "duplicate join tuple ({catalog}, {stream}) |> {join}"
                    ));
                }
                wired.insert(join.as_str());
            }
        }
    }
    let mut names: Vec<_> = members.keys().copied().collect();
    names.sort_unstable();
    for name in names {
        if let Some(stage) = members[name].filter(|stage| stage.stage_type() == StageType::Join) {
            if let Some(catalog) = stage.reference_stage_name() {
                known(catalog)?;
            }
            if !wired.contains(name) {
                return Err(format!("join '{name}' requires explicit (catalog, stream) |> {name} topology; its catalog declaration does not create an edge"));
            }
        }
    }
    Ok(())
}

/// Flatten only after validation. Shared catalog edges from distinct tuples
/// coalesce at their first authored occurrence; no other edges are deduplicated.
pub(crate) fn role_edges(connections: Vec<AuthoredConnection>) -> Vec<RoleEdge> {
    let mut catalogs = HashSet::new();
    let mut result = Vec::new();
    for connection in connections {
        match connection {
            AuthoredConnection::Edge { from, to, kind } => result.push(RoleEdge {
                from,
                to,
                kind,
                role: None,
            }),
            AuthoredConnection::Join {
                catalog,
                stream,
                join,
            } => {
                if catalogs.insert((catalog.clone(), join.clone())) {
                    result.push(RoleEdge {
                        from: catalog,
                        to: join.clone(),
                        kind: EdgeKind::Forward,
                        role: Some(JoinRole::Catalog),
                    });
                }
                result.push(RoleEdge {
                    from: stream,
                    to: join,
                    kind: EdgeKind::Forward,
                    role: Some(JoinRole::Stream),
                });
            }
        }
    }
    result
}
