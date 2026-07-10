// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

//! First-class composite substrate (FLOWIP-128a).
//!
//! A composite is a named, typed graph fragment that expands into ordinary
//! stages before topology validation and runtime build. Flows collect
//! `FlowMember`s; `lower_composites` consumes them and returns pure stage
//! descriptors, so post-lowering code cannot name a composite. Expansion
//! output is validated here: role grammar and uniqueness, lane vocabulary,
//! exact event-typed feed payloads, edge endpoints against declared roles,
//! and boundary port rules. Nested composites are unrepresentable: members
//! are `StageDescriptor`s and `CompositeDescriptor` is a disjoint trait.

use crate::dsl::stage_descriptor::StageDescriptor;
use crate::dsl::typing::TypeHint;
use obzenflow_core::TypedPayload;
use obzenflow_runtime::feed_plan::{FactVisibility, FeedRole};
use std::fmt;

/// One entry in a flow's `stages:` block: an ordinary stage or a composite
/// awaiting expansion (FLOWIP-128a D5).
pub enum FlowMember {
    Stage(Box<dyn StageDescriptor>),
    Composite(Box<dyn CompositeDescriptor>),
}

impl FlowMember {
    pub fn name(&self) -> &str {
        match self {
            Self::Stage(descriptor) => descriptor.name(),
            Self::Composite(descriptor) => descriptor.name(),
        }
    }

    pub fn set_name(&mut self, name: String) {
        match self {
            Self::Stage(descriptor) => descriptor.set_name(name),
            Self::Composite(descriptor) => descriptor.set_name(name),
        }
    }
}

/// Conversion at the `flow!` collection seam. Stage macros produce boxed
/// `StageDescriptor`s; composite macros produce boxed `CompositeDescriptor`s;
/// both slot into a `stages:` block unchanged.
pub trait IntoFlowMember {
    fn into_flow_member(self) -> FlowMember;
}

impl IntoFlowMember for Box<dyn StageDescriptor> {
    fn into_flow_member(self) -> FlowMember {
        FlowMember::Stage(self)
    }
}

impl IntoFlowMember for Box<dyn CompositeDescriptor> {
    fn into_flow_member(self) -> FlowMember {
        FlowMember::Composite(self)
    }
}

/// A named, typed, reusable graph fragment. Expansion declares members,
/// internal edges and feeds, and the boundary through the build context;
/// `lower_composites` validates and merges the result.
pub trait CompositeDescriptor: Send + Sync {
    fn name(&self) -> &str;
    fn set_name(&mut self, name: String);
    /// Composite kind, e.g. `ai_map_reduce`. A durable identifier.
    fn kind(&self) -> &'static str;
    /// Manifest schema version for this composite's durable evidence.
    fn schema_version(&self) -> u32;
    fn expand(self: Box<Self>, ctx: &mut CompositeBuildContext) -> Result<(), CompositeBuildError>;
}

const GENERIC_LANES: &[&str] = &["data", "manifest", "status", "terminal"];

fn valid_role_id(role: &str) -> bool {
    let mut chars = role.chars();
    match chars.next() {
        Some(first) if first.is_ascii_lowercase() => {}
        _ => return false,
    }
    chars.all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_')
}

fn exact_hints_only(payloads: &[TypeHint]) -> bool {
    payloads
        .iter()
        .all(|hint| matches!(hint, TypeHint::Exact { .. }))
}

struct MemberDecl {
    role: String,
    descriptor: Option<Box<dyn StageDescriptor>>,
    class: Option<String>,
}

struct EdgeDecl {
    from_role: String,
    to_role: String,
    lane: String,
}

/// Authoring-side internal feed declaration (FLOWIP-128a D3): a selected
/// payload lane on an internal edge. Payloads must be exact event-typed or
/// the subscription layer would silently never deliver them.
pub struct InternalFeedSpec {
    pub from_role: String,
    pub to_role: String,
    pub lane: String,
    pub payloads: Vec<TypeHint>,
    pub feed_role: FeedRole,
    pub visibility: FactVisibility,
}

struct PortDecl {
    name: String,
    role: String,
    direction: PortDirection,
    payloads: Vec<TypeHint>,
    default: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PortDirection {
    Input,
    Output,
}

/// Build context a composite declares itself into during `expand`.
pub struct CompositeBuildContext {
    kind: &'static str,
    permitted_lanes: Vec<String>,
    permitted_classes: Vec<String>,
    members: Vec<MemberDecl>,
    edges: Vec<EdgeDecl>,
    feeds: Vec<InternalFeedSpec>,
    ports: Vec<PortDecl>,
    /// Declaration order across plain edges and feeds, preserved into the
    /// manifest's internal-edge list (presentation order is authored order).
    edge_decl_order: Vec<EdgeRef>,
}

#[derive(Clone, Copy)]
enum EdgeRef {
    Plain(usize),
    Feed(usize),
}

impl CompositeBuildContext {
    pub fn new(kind: &'static str) -> Self {
        Self {
            kind,
            permitted_lanes: Vec::new(),
            permitted_classes: Vec::new(),
            members: Vec::new(),
            edges: Vec::new(),
            feeds: Vec::new(),
            ports: Vec::new(),
            edge_decl_order: Vec::new(),
        }
    }

    /// Kind-scoped lane vocabulary beyond the generic set (D3).
    pub fn permit_lane(&mut self, lane: &str) -> &mut Self {
        self.permitted_lanes.push(lane.to_string());
        self
    }

    /// Kind-scoped class vocabulary (D2). Empty means classes are rejected.
    pub fn permit_class(&mut self, class: &str) -> &mut Self {
        self.permitted_classes.push(class.to_string());
        self
    }

    pub fn member(&mut self, role: &str) -> MemberBuilder<'_> {
        self.members.push(MemberDecl {
            role: role.to_string(),
            descriptor: None,
            class: None,
        });
        MemberBuilder {
            decl: self.members.last_mut().expect("member just pushed"),
        }
    }

    /// A plain internal data edge with no selected feed.
    pub fn edge(&mut self, from_role: &str, to_role: &str) -> EdgeBuilder<'_> {
        self.edges.push(EdgeDecl {
            from_role: from_role.to_string(),
            to_role: to_role.to_string(),
            lane: "data".to_string(),
        });
        self.edge_decl_order
            .push(EdgeRef::Plain(self.edges.len() - 1));
        EdgeBuilder {
            decl: self.edges.last_mut().expect("edge just pushed"),
        }
    }

    /// An internal edge carrying selected payload feeds (D3). Implies the
    /// edge; do not also declare `edge()` for the same pair.
    pub fn feed(&mut self, from_role: &str, to_role: &str) -> FeedBuilder<'_> {
        self.feeds.push(InternalFeedSpec {
            from_role: from_role.to_string(),
            to_role: to_role.to_string(),
            lane: "data".to_string(),
            payloads: Vec::new(),
            feed_role: FeedRole::Input,
            visibility: FactVisibility::Routable,
        });
        self.edge_decl_order
            .push(EdgeRef::Feed(self.feeds.len() - 1));
        FeedBuilder {
            spec: self.feeds.last_mut().expect("feed just pushed"),
        }
    }

    pub fn boundary(&mut self) -> BoundaryBuilder<'_> {
        BoundaryBuilder { ctx: self }
    }
}

pub struct MemberBuilder<'a> {
    decl: &'a mut MemberDecl,
}

impl MemberBuilder<'_> {
    pub fn descriptor(self, descriptor: Box<dyn StageDescriptor>) -> Self {
        self.decl.descriptor = Some(descriptor);
        self
    }

    pub fn class(self, class: &str) -> Self {
        self.decl.class = Some(class.to_string());
        self
    }
}

pub struct EdgeBuilder<'a> {
    decl: &'a mut EdgeDecl,
}

impl EdgeBuilder<'_> {
    pub fn lane(self, lane: &str) -> Self {
        self.decl.lane = lane.to_string();
        self
    }
}

pub struct FeedBuilder<'a> {
    spec: &'a mut InternalFeedSpec,
}

impl FeedBuilder<'_> {
    pub fn lane(self, lane: &str) -> Self {
        self.spec.lane = lane.to_string();
        self
    }

    pub fn payload<T: TypedPayload + 'static>(self) -> Self {
        self.spec.payloads.push(TypeHint::exact_payload::<T>());
        self
    }
}

pub struct BoundaryBuilder<'a> {
    ctx: &'a mut CompositeBuildContext,
}

impl<'a> BoundaryBuilder<'a> {
    pub fn input(self, name: &str, role: &str) -> PortBuilder<'a> {
        self.push_port(name, role, PortDirection::Input)
    }

    pub fn output(self, name: &str, role: &str) -> PortBuilder<'a> {
        self.push_port(name, role, PortDirection::Output)
    }

    fn push_port(self, name: &str, role: &str, direction: PortDirection) -> PortBuilder<'a> {
        self.ctx.ports.push(PortDecl {
            name: name.to_string(),
            role: role.to_string(),
            direction,
            payloads: Vec::new(),
            default: false,
        });
        let index = self.ctx.ports.len() - 1;
        PortBuilder {
            ctx: self.ctx,
            index,
        }
    }
}

pub struct PortBuilder<'a> {
    ctx: &'a mut CompositeBuildContext,
    index: usize,
}

impl<'a> PortBuilder<'a> {
    pub fn payload<T: TypedPayload + 'static>(self) -> Self {
        self.ctx.ports[self.index]
            .payloads
            .push(TypeHint::exact_payload::<T>());
        self
    }

    pub fn default(self) -> Self {
        self.ctx.ports[self.index].default = true;
        self
    }

    pub fn input(self, name: &str, role: &str) -> PortBuilder<'a> {
        BoundaryBuilder { ctx: self.ctx }.input(name, role)
    }

    pub fn output(self, name: &str, role: &str) -> PortBuilder<'a> {
        BoundaryBuilder { ctx: self.ctx }.output(name, role)
    }
}

/// One expanded member: an ordinary stage plus its manifest identity.
pub struct ExpandedMember {
    pub role: String,
    pub stage_name: String,
    pub class: Option<String>,
    pub order: u16,
    pub is_entry: bool,
    pub is_exit: bool,
    pub descriptor: Box<dyn StageDescriptor>,
}

impl fmt::Debug for ExpandedMember {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ExpandedMember")
            .field("role", &self.role)
            .field("stage_name", &self.stage_name)
            .field("class", &self.class)
            .field("order", &self.order)
            .field("is_entry", &self.is_entry)
            .field("is_exit", &self.is_exit)
            .field("descriptor", &self.descriptor.name())
            .finish()
    }
}

/// A boundary port resolved to its member stage name.
#[derive(Clone, Debug)]
pub struct ResolvedPort {
    pub name: String,
    pub role: String,
    pub stage_name: String,
    pub direction: PortDirection,
    pub payloads: Vec<TypeHint>,
    pub default: bool,
}

/// The composite boundary after expansion (FLOWIP-128a D1).
#[derive(Clone, Debug, Default)]
pub struct ResolvedBoundary {
    /// Stable manifest identity assigned by composite lowering.
    pub subgraph_id: String,
    pub inputs: Vec<ResolvedPort>,
    pub outputs: Vec<ResolvedPort>,
}

#[derive(Debug)]
pub enum PortResolveError {
    Ambiguous {
        ports: Vec<String>,
        input_display: String,
    },
    NoDefaultPort,
}

impl ResolvedBoundary {
    /// The single input port (validated at expansion).
    pub fn input(&self) -> &ResolvedPort {
        self.inputs
            .first()
            .expect("expansion validated one input port")
    }

    /// D1 resolution: unique typed match wins, default fallback, ambiguity
    /// errors. Matching is by `TypeId`, canonical within one compilation.
    pub fn resolve_output(
        &self,
        downstream_hint: Option<&TypeHint>,
    ) -> Result<&ResolvedPort, PortResolveError> {
        if let Some(TypeHint::Exact {
            type_id,
            display_name,
            ..
        }) = downstream_hint
        {
            let matches: Vec<&ResolvedPort> = self
                .outputs
                .iter()
                .filter(|port| {
                    port.payloads.iter().any(|payload| {
                        matches!(payload, TypeHint::Exact { type_id: port_type, .. } if port_type == type_id)
                    })
                })
                .collect();
            match matches.as_slice() {
                [single] => return Ok(single),
                [] => {}
                many => {
                    return Err(PortResolveError::Ambiguous {
                        ports: many.iter().map(|p| p.name.clone()).collect(),
                        input_display: display_name.clone(),
                    })
                }
            }
        }
        self.outputs
            .iter()
            .find(|port| port.default)
            .ok_or(PortResolveError::NoDefaultPort)
    }

    /// Human-readable port inventory for diagnostics.
    pub fn describe_outputs(&self) -> String {
        self.outputs
            .iter()
            .map(|port| {
                let payloads: Vec<&str> = port
                    .payloads
                    .iter()
                    .map(|hint| match hint {
                        TypeHint::Exact { display_name, .. } => display_name.as_str(),
                        TypeHint::Unspecified => "unspecified",
                    })
                    .collect();
                format!("{} ({})", port.name, payloads.join(", "))
            })
            .collect::<Vec<_>>()
            .join(", ")
    }
}

/// A member-name-resolved internal feed, consumed by feed-plan derivation.
#[derive(Clone, Debug)]
pub struct ResolvedInternalFeed {
    pub from_stage: String,
    pub to_stage: String,
    pub lane: String,
    pub payloads: Vec<TypeHint>,
    pub feed_role: FeedRole,
    pub visibility: FactVisibility,
}

/// Validated expansion output, all endpoints resolved to member stage names.
#[derive(Debug)]
pub struct CompositeExpansion {
    pub kind: &'static str,
    pub schema_version: u32,
    pub members: Vec<ExpandedMember>,
    /// Internal edges as `(from_stage, to_stage, lane)`.
    pub internal_edges: Vec<(String, String, String)>,
    pub feeds: Vec<ResolvedInternalFeed>,
    pub boundary: ResolvedBoundary,
}

#[derive(Debug)]
pub struct CompositeBuildError {
    message: String,
}

impl CompositeBuildError {
    fn new(message: String) -> Self {
        Self { message }
    }
}

impl fmt::Display for CompositeBuildError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for CompositeBuildError {}

impl CompositeBuildContext {
    /// Validate the declarations and resolve them against the binding.
    /// Member stage names derive as `{binding}__{role}` (D2), so duplicate
    /// members and duplicate roles are the same failure.
    pub fn finish(
        self,
        binding: &str,
        schema_version: u32,
    ) -> Result<CompositeExpansion, CompositeBuildError> {
        let composite = binding;
        let kind = self.kind;

        let declared_roles: Vec<String> = self.members.iter().map(|m| m.role.clone()).collect();
        let declared_list = declared_roles.join(", ");

        // Role grammar and uniqueness (A1 via derived names).
        let mut seen = std::collections::HashSet::new();
        for member in &self.members {
            if !valid_role_id(&member.role) {
                return Err(CompositeBuildError::new(format!(
                    "composite '{composite}': invalid role id '{}'; roles match [a-z][a-z0-9_]*",
                    member.role
                )));
            }
            if !seen.insert(member.role.clone()) {
                return Err(CompositeBuildError::new(format!(
                    "composite '{composite}': duplicate role '{}'; roles must be unique within a composite",
                    member.role
                )));
            }
        }

        let known = |role: &str| declared_roles.iter().any(|r| r == role);
        let unknown_role = |clause: &str, role: &str| {
            CompositeBuildError::new(format!(
                "composite '{composite}': unknown role '{role}' in {clause}; declared roles: {declared_list}"
            ))
        };

        // Members complete; classes against kind vocabulary (D2).
        for member in &self.members {
            if member.descriptor.is_none() {
                return Err(CompositeBuildError::new(format!(
                    "composite '{composite}': role '{}' declares no member descriptor",
                    member.role
                )));
            }
            if let Some(class) = &member.class {
                if !self.permitted_classes.iter().any(|c| c == class) {
                    let allowed = if self.permitted_classes.is_empty() {
                        format!("{kind} declares no classes")
                    } else {
                        format!("{kind} classes are {}", self.permitted_classes.join(", "))
                    };
                    return Err(CompositeBuildError::new(format!(
                        "composite '{composite}': invalid class '{class}' on role '{}'; {allowed}",
                        member.role
                    )));
                }
            }
        }

        // Lane vocabulary (D3) over edges and feeds.
        let lane_ok = |lane: &str| {
            GENERIC_LANES.contains(&lane) || self.permitted_lanes.iter().any(|l| l == lane)
        };
        let lane_error = |lane: &str| {
            CompositeBuildError::new(format!(
                "composite '{composite}': unknown lane '{lane}'; generic lanes are {} and kind '{kind}' declares [{}]",
                GENERIC_LANES.join(", "),
                self.permitted_lanes.join(", ")
            ))
        };

        // Endpoints reference declared roles (A5, expansion half); one edge
        // per member pair.
        let mut edge_pairs = std::collections::HashSet::new();
        for edge in &self.edges {
            if !known(&edge.from_role) {
                return Err(unknown_role("internal edge", &edge.from_role));
            }
            if !known(&edge.to_role) {
                return Err(unknown_role("internal edge", &edge.to_role));
            }
            if !lane_ok(&edge.lane) {
                return Err(lane_error(&edge.lane));
            }
            if !edge_pairs.insert((edge.from_role.clone(), edge.to_role.clone())) {
                return Err(CompositeBuildError::new(format!(
                    "composite '{composite}': duplicate internal edge '{}' -> '{}'",
                    edge.from_role, edge.to_role
                )));
            }
        }
        for feed in &self.feeds {
            if !known(&feed.from_role) {
                return Err(unknown_role("internal feed", &feed.from_role));
            }
            if !known(&feed.to_role) {
                return Err(unknown_role("internal feed", &feed.to_role));
            }
            if !lane_ok(&feed.lane) {
                return Err(lane_error(&feed.lane));
            }
            if feed.payloads.is_empty() {
                return Err(CompositeBuildError::new(format!(
                    "composite '{composite}': internal feed '{}' -> '{}' declares no payloads",
                    feed.from_role, feed.to_role
                )));
            }
            if !exact_hints_only(&feed.payloads) {
                return Err(CompositeBuildError::new(format!(
                    "composite '{composite}': internal feed '{}' -> '{}' has a payload without an exact event type; the subscription layer cannot select it",
                    feed.from_role, feed.to_role
                )));
            }
            if !edge_pairs.insert((feed.from_role.clone(), feed.to_role.clone())) {
                return Err(CompositeBuildError::new(format!(
                    "composite '{composite}': duplicate internal edge '{}' -> '{}'",
                    feed.from_role, feed.to_role
                )));
            }
        }

        // Boundary ports (D1): reference declared roles, exact payloads,
        // unique names, exactly one input port, exactly one default output.
        let mut port_names = std::collections::HashSet::new();
        for port in &self.ports {
            if !known(&port.role) {
                return Err(unknown_role("boundary port", &port.role));
            }
            if port.payloads.is_empty() || !exact_hints_only(&port.payloads) {
                return Err(CompositeBuildError::new(format!(
                    "composite '{composite}': boundary port '{}' must declare exact event-typed payloads",
                    port.name
                )));
            }
            if !port_names.insert(port.name.clone()) {
                return Err(CompositeBuildError::new(format!(
                    "composite '{composite}': duplicate boundary port name '{}'",
                    port.name
                )));
            }
        }
        let input_count = self
            .ports
            .iter()
            .filter(|p| p.direction == PortDirection::Input)
            .count();
        if input_count != 1 {
            return Err(CompositeBuildError::new(format!(
                "composite '{composite}': exactly one input boundary port is required, found {input_count}"
            )));
        }
        let default_outputs = self
            .ports
            .iter()
            .filter(|p| p.direction == PortDirection::Output && p.default)
            .count();
        let output_count = self
            .ports
            .iter()
            .filter(|p| p.direction == PortDirection::Output)
            .count();
        if output_count == 0 || default_outputs != 1 {
            return Err(CompositeBuildError::new(format!(
                "composite '{composite}': boundary needs at least one output port with exactly one default, found {output_count} outputs ({default_outputs} default)"
            )));
        }

        // Resolve to member stage names.
        let stage_name = |role: &str| format!("{binding}__{role}");

        let entry_roles: std::collections::HashSet<&str> = self
            .ports
            .iter()
            .filter(|p| p.direction == PortDirection::Input)
            .map(|p| p.role.as_str())
            .collect();
        let exit_roles: std::collections::HashSet<&str> = self
            .ports
            .iter()
            .filter(|p| p.direction == PortDirection::Output)
            .map(|p| p.role.as_str())
            .collect();

        let mut members = Vec::with_capacity(self.members.len());
        for (index, member) in self.members.into_iter().enumerate() {
            let role = member.role;
            let name = stage_name(&role);
            let mut descriptor = member.descriptor.expect("validated above");
            // Member stage names are derived, never author-chosen (D2).
            descriptor.set_name(name.clone());
            members.push(ExpandedMember {
                stage_name: name,
                class: member.class,
                order: index as u16,
                is_entry: entry_roles.contains(role.as_str()),
                is_exit: exit_roles.contains(role.as_str()),
                descriptor,
                role,
            });
        }

        let mut internal_edges: Vec<(String, String, String)> = Vec::new();
        for edge_ref in &self.edge_decl_order {
            let (from_role, to_role, lane) = match edge_ref {
                EdgeRef::Plain(index) => {
                    let edge = &self.edges[*index];
                    (&edge.from_role, &edge.to_role, &edge.lane)
                }
                EdgeRef::Feed(index) => {
                    let feed = &self.feeds[*index];
                    (&feed.from_role, &feed.to_role, &feed.lane)
                }
            };
            internal_edges.push((stage_name(from_role), stage_name(to_role), lane.clone()));
        }

        let feeds = self
            .feeds
            .into_iter()
            .map(|feed| ResolvedInternalFeed {
                from_stage: stage_name(&feed.from_role),
                to_stage: stage_name(&feed.to_role),
                lane: feed.lane,
                payloads: feed.payloads,
                feed_role: feed.feed_role,
                visibility: feed.visibility,
            })
            .collect();

        let mut boundary = ResolvedBoundary::default();
        for port in self.ports {
            let resolved = ResolvedPort {
                stage_name: stage_name(&port.role),
                name: port.name,
                role: port.role,
                direction: port.direction,
                payloads: port.payloads,
                default: port.default,
            };
            match resolved.direction {
                PortDirection::Input => boundary.inputs.push(resolved),
                PortDirection::Output => boundary.outputs.push(resolved),
            }
        }

        Ok(CompositeExpansion {
            kind,
            schema_version,
            members,
            internal_edges,
            feeds,
            boundary,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dsl::stage_descriptor::TransformDescriptor;
    use obzenflow_runtime::stages::common::handler_error::HandlerError;
    use obzenflow_runtime::stages::common::handlers::TransformHandler;
    use serde::{Deserialize, Serialize};

    #[derive(Debug, Clone)]
    struct Noop;

    #[async_trait::async_trait]
    impl TransformHandler for Noop {
        fn process(
            &self,
            event: obzenflow_core::ChainEvent,
        ) -> Result<Vec<obzenflow_core::ChainEvent>, HandlerError> {
            Ok(vec![event])
        }

        async fn drain(&mut self) -> Result<(), HandlerError> {
            Ok(())
        }
    }

    fn noop_descriptor(name: &str) -> Box<dyn StageDescriptor> {
        Box::new(TransformDescriptor {
            name: name.to_string(),
            handler: Noop,
            middleware: vec![],
            backpressure: None,
        })
    }

    #[derive(Debug, Clone, Serialize, Deserialize)]
    struct PayloadA;
    impl TypedPayload for PayloadA {
        const EVENT_TYPE: &'static str = "test.composition.a";
    }

    #[derive(Debug, Clone, Serialize, Deserialize)]
    struct PayloadB;
    impl TypedPayload for PayloadB {
        const EVENT_TYPE: &'static str = "test.composition.b";
    }

    fn two_member_ctx() -> CompositeBuildContext {
        let mut ctx = CompositeBuildContext::new("test_kind");
        ctx.member("first").descriptor(noop_descriptor("first"));
        ctx.member("second").descriptor(noop_descriptor("second"));
        ctx.edge("first", "second");
        ctx.boundary()
            .input("in", "first")
            .payload::<PayloadA>()
            .default()
            .output("out", "second")
            .payload::<PayloadB>()
            .default();
        ctx
    }

    #[test]
    fn finish_resolves_derived_member_names_and_flags() {
        let expansion = two_member_ctx().finish("bind", 1).expect("valid expansion");
        assert_eq!(expansion.members.len(), 2);
        assert_eq!(expansion.members[0].stage_name, "bind__first");
        assert_eq!(expansion.members[1].stage_name, "bind__second");
        assert!(expansion.members[0].is_entry && !expansion.members[0].is_exit);
        assert!(expansion.members[1].is_exit && !expansion.members[1].is_entry);
        assert_eq!(expansion.members[1].order, 1);
        assert_eq!(
            expansion.internal_edges,
            vec![(
                "bind__first".to_string(),
                "bind__second".to_string(),
                "data".to_string()
            )]
        );
    }

    #[test]
    fn duplicate_role_fails_with_locked_diagnostic() {
        let mut ctx = CompositeBuildContext::new("test_kind");
        ctx.member("same").descriptor(noop_descriptor("a"));
        ctx.member("same").descriptor(noop_descriptor("b"));
        let err = ctx.finish("bind", 1).expect_err("duplicate role");
        assert_eq!(
            err.to_string(),
            "composite 'bind': duplicate role 'same'; roles must be unique within a composite"
        );
    }

    #[test]
    fn invalid_role_grammar_fails() {
        let mut ctx = CompositeBuildContext::new("test_kind");
        ctx.member("Bad-Role").descriptor(noop_descriptor("a"));
        let err = ctx.finish("bind", 1).expect_err("bad grammar");
        assert!(err.to_string().contains("invalid role id 'Bad-Role'"));
    }

    #[test]
    fn unknown_role_in_feed_lists_declared_roles() {
        let mut ctx = two_member_ctx();
        ctx.feed("first", "third")
            .lane("manifest")
            .payload::<PayloadA>();
        let err = ctx.finish("bind", 1).expect_err("unknown role");
        assert_eq!(
            err.to_string(),
            "composite 'bind': unknown role 'third' in internal feed; declared roles: first, second"
        );
    }

    #[test]
    fn unknown_lane_fails_naming_vocabulary() {
        let mut ctx = two_member_ctx();
        ctx.feed("first", "second")
            .lane("sideband")
            .payload::<PayloadA>();
        let err = ctx.finish("bind", 1).expect_err("unknown lane");
        assert!(err.to_string().contains("unknown lane 'sideband'"), "{err}");
    }

    #[test]
    fn kind_declared_lane_and_class_are_permitted() {
        let mut ctx = CompositeBuildContext::new("test_kind");
        ctx.permit_lane("compensation_trigger");
        ctx.permit_class("driver");
        ctx.member("first")
            .descriptor(noop_descriptor("a"))
            .class("driver");
        ctx.member("second").descriptor(noop_descriptor("b"));
        ctx.feed("first", "second")
            .lane("compensation_trigger")
            .payload::<PayloadA>();
        ctx.boundary()
            .input("in", "first")
            .payload::<PayloadA>()
            .default()
            .output("out", "second")
            .payload::<PayloadB>()
            .default();
        let expansion = ctx.finish("bind", 1).expect("valid");
        assert_eq!(expansion.members[0].class.as_deref(), Some("driver"));
        assert_eq!(expansion.feeds[0].lane, "compensation_trigger");
    }

    #[test]
    fn invalid_class_fails_with_kind_vocabulary() {
        let mut ctx = two_member_ctx();
        ctx.member("third")
            .descriptor(noop_descriptor("c"))
            .class("undoable");
        let err = ctx.finish("bind", 1).expect_err("invalid class");
        assert!(
            err.to_string()
                .contains("invalid class 'undoable' on role 'third'"),
            "{err}"
        );
    }

    #[test]
    fn boundary_requires_one_input_and_one_default_output() {
        let mut ctx = CompositeBuildContext::new("test_kind");
        ctx.member("only").descriptor(noop_descriptor("a"));
        let err = ctx.finish("bind", 1).expect_err("no ports");
        assert!(err.to_string().contains("exactly one input boundary port"));
    }

    #[test]
    fn resolve_output_matches_unique_type_falls_back_and_rejects_ambiguity() {
        let mut ctx = CompositeBuildContext::new("test_kind");
        ctx.member("first").descriptor(noop_descriptor("a"));
        ctx.member("second").descriptor(noop_descriptor("b"));
        ctx.member("third").descriptor(noop_descriptor("c"));
        ctx.edge("first", "second");
        ctx.edge("first", "third");
        ctx.boundary()
            .input("in", "first")
            .payload::<PayloadA>()
            .default()
            .output("out", "second")
            .payload::<PayloadA>()
            .default()
            .output("failed", "third")
            .payload::<PayloadB>();
        let expansion = ctx.finish("bind", 1).expect("valid");

        // Unique typed match wins over the default.
        let hit = expansion
            .boundary
            .resolve_output(Some(&TypeHint::exact_payload::<PayloadB>()))
            .expect("unique match");
        assert_eq!(hit.name, "failed");
        assert_eq!(hit.stage_name, "bind__third");

        // No match falls back to the default port.
        #[derive(Debug, Clone, Serialize, Deserialize)]
        struct Unrelated;
        impl TypedPayload for Unrelated {
            const EVENT_TYPE: &'static str = "test.composition.unrelated";
        }
        let fallback = expansion
            .boundary
            .resolve_output(Some(&TypeHint::exact_payload::<Unrelated>()))
            .expect("default fallback");
        assert_eq!(fallback.name, "out");

        // Unspecified hint also binds the default.
        let unhinted = expansion
            .boundary
            .resolve_output(None)
            .expect("default fallback");
        assert_eq!(unhinted.name, "out");
    }

    #[test]
    fn resolve_output_ambiguity_is_an_error() {
        let mut ctx = CompositeBuildContext::new("test_kind");
        ctx.member("first").descriptor(noop_descriptor("a"));
        ctx.member("second").descriptor(noop_descriptor("b"));
        ctx.boundary()
            .input("in", "first")
            .payload::<PayloadA>()
            .default()
            .output("out", "second")
            .payload::<PayloadB>()
            .default()
            .output("also", "second")
            .payload::<PayloadB>();
        let expansion = ctx.finish("bind", 1).expect("valid");
        let err = expansion
            .boundary
            .resolve_output(Some(&TypeHint::exact_payload::<PayloadB>()))
            .expect_err("ambiguous");
        match err {
            PortResolveError::Ambiguous { ports, .. } => {
                assert_eq!(ports, vec!["out".to_string(), "also".to_string()]);
            }
            other => panic!("expected ambiguity, got {other:?}"),
        }
    }
}
