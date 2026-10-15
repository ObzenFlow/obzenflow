// SPDX-License-Identifier: MIT OR Apache-2.0
// SPDX-FileCopyrightText: 2025-2026 ObzenFlow Contributors
// https://obzenflow.dev

use std::collections::{BTreeSet, HashMap, HashSet, VecDeque};

use obzenflow_core::event::context::StageType;
use obzenflow_core::{StageId, StageKey};
use obzenflow_runtime::stages::join::{JoinReferenceMode, PhaseWitness, PhaseWitnesses};
use obzenflow_runtime::stages::merges::MergeStrategyKind;
use obzenflow_topology::{EdgeKind, Topology};

use crate::dsl::stage_descriptor::{JoinShape, StageDescriptor};
use crate::dsl::typing::TypeHint;
use crate::dsl::FlowBuildError;

#[derive(Default)]
pub(crate) struct ThenLivePlan {
    pub(crate) witnesses: Vec<(String, PhaseWitnesses)>,
    pub(crate) merge_producers: Vec<(String, Vec<StageId>, Vec<StageId>)>,
    holds: Vec<Hold>,
}

struct Hold {
    holder: StageId,
    state: HoldState,
    must_flow: Vec<StageId>,
    held: Vec<StageId>,
}

#[derive(Clone, Copy)]
enum HoldState {
    Hydrating,
    Enriching,
    MergeBatch,
}

#[derive(Default)]
struct Roots {
    finite: BTreeSet<StageId>,
    live: BTreeSet<StageId>,
}

impl Roots {
    fn mixed(&self) -> bool {
        !self.finite.is_empty() && !self.live.is_empty()
    }

    fn has_live(&self) -> bool {
        !self.live.is_empty()
    }

    fn all(&self) -> impl Iterator<Item = StageId> + '_ {
        self.finite.iter().chain(&self.live).copied()
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum StreamInput {
    Finite,
    Live,
    ThenLiveMerge,
}

struct Graph<'a> {
    descriptors: HashMap<StageId, &'a dyn StageDescriptor>,
    inbound: HashMap<StageId, Vec<(StageId, EdgeKind)>>,
    outbound: HashMap<StageId, Vec<(StageId, EdgeKind)>>,
}

impl<'a> Graph<'a> {
    fn new(
        topology: &Topology,
        descriptors: &'a HashMap<String, Box<dyn StageDescriptor>>,
        name_to_id: &HashMap<String, StageId>,
    ) -> Self {
        let mut graph = Self {
            descriptors: HashMap::new(),
            inbound: HashMap::new(),
            outbound: HashMap::new(),
        };
        for (binding, descriptor) in descriptors {
            if let Some(&id) = name_to_id.get(binding) {
                graph.descriptors.insert(id, descriptor.as_ref());
            }
        }
        for edge in topology.edges() {
            let from = StageId::from_ulid(edge.from.ulid());
            let to = StageId::from_ulid(edge.to.ulid());
            graph.inbound.entry(to).or_default().push((from, edge.kind));
            graph
                .outbound
                .entry(from)
                .or_default()
                .push((to, edge.kind));
        }
        graph
    }

    fn name(&self, id: StageId) -> &str {
        self.descriptors
            .get(&id)
            .map_or("<unknown>", |descriptor| descriptor.name())
    }

    fn stage_type(&self, id: StageId) -> Option<StageType> {
        self.descriptors
            .get(&id)
            .map(|descriptor| descriptor.stage_type())
    }

    fn sorted(&self, ids: impl IntoIterator<Item = StageId>) -> Vec<StageId> {
        let mut ids: Vec<StageId> = ids.into_iter().collect();
        ids.sort_by(|left, right| self.name(*left).cmp(self.name(*right)));
        ids.dedup();
        ids
    }

    fn inputs(&self, id: StageId) -> Vec<StageId> {
        self.sorted(
            self.inbound
                .get(&id)
                .into_iter()
                .flatten()
                .map(|(from, _)| *from),
        )
    }

    fn outputs(&self, id: StageId) -> impl Iterator<Item = StageId> + '_ {
        self.outbound
            .get(&id)
            .into_iter()
            .flatten()
            .map(|(to, _)| *to)
    }

    fn upstream_closure(&self, ids: impl IntoIterator<Item = StageId>) -> HashSet<StageId> {
        let mut seen = HashSet::new();
        let mut queue: VecDeque<StageId> = ids.into_iter().collect();
        while let Some(id) = queue.pop_front() {
            if seen.insert(id) {
                queue.extend(
                    self.inbound
                        .get(&id)
                        .into_iter()
                        .flatten()
                        .map(|(from, _)| *from),
                );
            }
        }
        seen
    }

    fn downstream_closure(&self, ids: impl IntoIterator<Item = StageId>) -> HashSet<StageId> {
        let mut seen = HashSet::new();
        let mut queue: VecDeque<StageId> = ids.into_iter().collect();
        while let Some(id) = queue.pop_front() {
            if seen.insert(id) {
                queue.extend(self.outputs(id));
            }
        }
        seen
    }

    fn roots(&self, id: StageId) -> Roots {
        let mut roots = Roots::default();
        for stage in self.upstream_closure([id]) {
            match self.stage_type(stage) {
                Some(StageType::FiniteSource) => {
                    roots.finite.insert(stage);
                }
                Some(StageType::InfiniteSource) => {
                    roots.live.insert(stage);
                }
                _ => {}
            }
        }
        roots
    }

    fn quoted_roots(&self, roots: &Roots) -> String {
        let finite = self.sorted(roots.finite.iter().copied());
        let live = self.sorted(roots.live.iter().copied());
        finite
            .into_iter()
            .chain(live)
            .map(|id| format!("`{}`", self.name(id)))
            .collect::<Vec<_>>()
            .join(", ")
    }

    fn is_then_live_join(&self, id: StageId) -> bool {
        self.descriptors.get(&id).is_some_and(|descriptor| {
            descriptor
                .join_shape()
                .is_some_and(|shape| shape.mode == JoinReferenceMode::FiniteThenLive)
        })
    }

    fn is_then_live_merge(&self, id: StageId) -> bool {
        self.descriptors.get(&id).is_some_and(|descriptor| {
            descriptor.merge_strategy() == Some(MergeStrategyKind::ThenLive)
        })
    }

    fn witness(&self, id: StageId) -> PhaseWitness {
        PhaseWitness {
            stage_key: StageKey::from(self.name(id)),
            stage_id: id,
        }
    }

    fn must_flow(
        &self,
        holder: StageId,
        ending_roots: impl IntoIterator<Item = StageId>,
        read: &[StageId],
    ) -> Vec<StageId> {
        let down = self.downstream_closure(ending_roots);
        let up = self.upstream_closure(read.iter().copied());
        self.sorted(
            down.intersection(&up)
                .copied()
                .chain(std::iter::once(holder)),
        )
    }
}

#[derive(Debug)]
pub(crate) struct Refusal(String);

impl std::fmt::Display for Refusal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl From<Refusal> for FlowBuildError {
    fn from(refusal: Refusal) -> Self {
        FlowBuildError::ThenLiveTopology(refusal.0)
    }
}

struct Variants {
    then_live: String,
    live: String,
    batch: String,
}

impl Variants {
    fn of(shape: JoinShape) -> Self {
        match shape.label {
            Some(label) => Self {
                then_live: format!("{label}_then_live"),
                live: format!("{label}_live"),
                batch: label.to_string(),
            },
            None => Self {
                then_live: "JoinReferenceMode::FiniteThenLive".to_string(),
                live: "JoinReferenceMode::Live".to_string(),
                batch: "JoinReferenceMode::FiniteEof".to_string(),
            },
        }
    }
}

fn short_type_name(hint: &TypeHint) -> String {
    let TypeHint::Exact { display_name, .. } = hint else {
        return "T".to_string();
    };
    let mut out = String::new();
    let mut segment_start = 0;
    let mut chars = display_name.chars().peekable();
    while let Some(c) = chars.next() {
        if c == ':' && chars.peek() == Some(&':') {
            chars.next();
            out.truncate(segment_start);
            continue;
        }
        out.push(c);
        if !(c.is_alphanumeric() || c == '_') {
            segment_start = out.len();
        }
    }
    out
}

fn stage_kind(stage_type: Option<StageType>) -> &'static str {
    match stage_type {
        Some(StageType::FiniteSource | StageType::InfiniteSource) => "source",
        Some(StageType::Transform) => "transform",
        Some(StageType::Stateful) => "stateful",
        Some(StageType::Join) => "join",
        Some(StageType::Merge) => "merge",
        Some(StageType::Sink) => "sink",
        None => "unknown",
    }
}

pub(crate) fn plan(
    topology: &Topology,
    descriptors: &HashMap<String, Box<dyn StageDescriptor>>,
    name_to_id: &HashMap<String, StageId>,
) -> Result<ThenLivePlan, Refusal> {
    let graph = Graph::new(topology, descriptors, name_to_id);
    let mut bindings: Vec<&String> = descriptors.keys().collect();
    bindings.sort();

    let mut plan = ThenLivePlan::default();
    for binding in bindings {
        let Some(&id) = name_to_id.get(binding) else {
            continue;
        };
        let descriptor = descriptors[binding].as_ref();
        if descriptor.merge_strategy() == Some(MergeStrategyKind::ThenLive) {
            let (batch, live) = merge_producers(&graph, id)?;
            let mut ending = BTreeSet::new();
            for producer in &batch {
                ending.extend(graph.roots(*producer).finite);
            }
            plan.holds.push(Hold {
                holder: id,
                state: HoldState::MergeBatch,
                must_flow: graph.must_flow(id, ending, &batch),
                held: live.clone(),
            });
            plan.merge_producers.push((binding.clone(), batch, live));
        }
        let Some(shape) = descriptor.join_shape() else {
            continue;
        };
        let Some(catalog) = descriptor.reference_stage_id() else {
            continue;
        };
        let streams: Vec<StageId> = graph
            .inputs(id)
            .into_iter()
            .filter(|input| *input != catalog)
            .collect();
        match shape.mode {
            JoinReferenceMode::FiniteThenLive => {
                let witnesses = then_live_join(&graph, id, shape, catalog, &streams)?;
                let catalog_roots = graph.roots(catalog);
                plan.holds.push(Hold {
                    holder: id,
                    state: HoldState::Hydrating,
                    must_flow: graph.must_flow(id, catalog_roots.finite, &[catalog]),
                    held: streams.clone(),
                });
                if !witnesses.batch_stream.is_empty() {
                    let mut ending = BTreeSet::new();
                    for stream in &streams {
                        ending.extend(graph.roots(*stream).finite);
                    }
                    plan.holds.push(Hold {
                        holder: id,
                        state: HoldState::Enriching,
                        must_flow: graph.must_flow(id, ending, &streams),
                        held: vec![catalog],
                    });
                }
                plan.witnesses.push((binding.clone(), witnesses));
            }
            JoinReferenceMode::FiniteEof => {
                let ending: Vec<StageId> = graph.roots(catalog).all().collect();
                plan.holds.push(Hold {
                    holder: id,
                    state: HoldState::Hydrating,
                    must_flow: graph.must_flow(id, ending, &[catalog]),
                    held: streams,
                });
            }
            JoinReferenceMode::Live => {}
        }
    }
    Ok(plan)
}

fn merge_producers(
    graph: &Graph<'_>,
    merge: StageId,
) -> Result<(Vec<StageId>, Vec<StageId>), Refusal> {
    let name = graph.name(merge);
    let mut batch = Vec::new();
    let mut live = Vec::new();
    for producer in graph.inputs(merge) {
        let roots = graph.roots(producer);
        if roots.mixed() {
            return Err(Refusal(format!(
                "merge `{name}` uses then_live, but its producer `{}` mixes batch and live \
                 sources ({}).\nMerge the batch and live producers directly, and transform each \
                 before the merge.",
                graph.name(producer),
                graph.quoted_roots(&roots),
            )));
        }
        if roots.has_live() {
            live.push(producer);
        } else {
            batch.push(producer);
        }
    }
    if live.is_empty() {
        return Err(Refusal(format!(
            "merge `{name}` uses then_live, but it has no live producer, so it would never go \
             live.\nA then_live merge needs at least one batch and one live producer."
        )));
    }
    if batch.is_empty() {
        return Err(Refusal(format!(
            "merge `{name}` uses then_live, but it has no batch producer, so its batch phase \
             would never end.\nA then_live merge needs at least one batch and one live producer."
        )));
    }
    Ok((batch, live))
}

fn then_live_join(
    graph: &Graph<'_>,
    join: StageId,
    shape: JoinShape,
    catalog: StageId,
    streams: &[StageId],
) -> Result<PhaseWitnesses, Refusal> {
    let name = graph.name(join);
    let variants = Variants::of(shape);
    let uses = &variants.then_live;
    let metadata = graph
        .descriptors
        .get(&join)
        .and_then(|descriptor| descriptor.typing_metadata());
    let catalog_type = metadata.map_or("T".to_string(), |m| short_type_name(&m.reference_type));
    let stream_type = metadata.map_or("T".to_string(), |m| short_type_name(&m.stream_type));
    let catalog_name = graph.name(catalog);

    if graph.is_then_live_join(catalog) {
        return Err(Refusal(format!(
            "join `{name}` uses {uses}, but its catalog `{catalog_name}` is a then-live join, \
             whose batch and live rows cannot be told apart yet."
        )));
    }
    let catalog_roots = graph.roots(catalog);
    if catalog_roots.finite.is_empty() {
        return Err(Refusal(format!(
            "join `{name}` uses {uses}, but its catalog `{catalog_name}` has no batch source, so \
             it would never leave its first phase.\nUse {} for a catalog that is live from the \
             start.",
            variants.live
        )));
    }
    let catalog_witnesses = if catalog_roots.has_live() {
        catalog_frontier(graph, name, uses, &catalog_type, catalog)?
    } else {
        vec![catalog]
    };

    for stream in streams {
        if graph.is_then_live_join(*stream) {
            let remedy = if catalog_roots.has_live() {
                String::new()
            } else {
                format!(
                    "\nIf `{catalog_name}` is batch only, use {} for `{name}`.",
                    variants.batch
                )
            };
            return Err(Refusal(format!(
                "join `{name}` uses {uses}, but its stream input `{}` is a then-live join, whose \
                 batch and live rows cannot be told apart yet.{remedy}",
                graph.name(*stream)
            )));
        }
    }

    let mut classes = Vec::with_capacity(streams.len());
    for stream in streams {
        let roots = graph.roots(*stream);
        let class = if roots.mixed() {
            if !graph.is_then_live_merge(*stream) {
                return Err(Refusal(format!(
                    "join `{name}` uses {uses}, but its stream input `{}` mixes batch and live \
                     sources ({}).\nFeed batch and live rows to the join through \
                     `merge!({stream_type} => merges::then_live)` so the batch phase can end.",
                    graph.name(*stream),
                    graph.quoted_roots(&roots),
                )));
            }
            StreamInput::ThenLiveMerge
        } else if roots.has_live() {
            StreamInput::Live
        } else {
            StreamInput::Finite
        };
        classes.push((*stream, class));
    }
    let uniform = |class| classes.iter().all(|(_, c)| *c == class);
    if classes.len() > 1 && !uniform(StreamInput::Finite) && !uniform(StreamInput::Live) {
        let batch = classes
            .iter()
            .find(|(_, class)| *class != StreamInput::Live)
            .map(|(stream, _)| *stream);
        let live = classes
            .iter()
            .find(|(stream, class)| *class != StreamInput::Finite && Some(*stream) != batch)
            .map(|(stream, _)| *stream);
        if let (Some(batch), Some(live)) = (batch, live) {
            return Err(Refusal(format!(
                "join `{name}` uses {uses}, but its stream inputs `{}` and `{}` mix batch and \
                 live sources.\nCombine them with `merge!({stream_type} => merges::then_live)` \
                 and feed the merge to the join.",
                graph.name(batch),
                graph.name(live),
            )));
        }
    }

    if !catalog_roots.has_live() && classes.iter().all(|(_, c)| *c == StreamInput::Finite) {
        return Err(Refusal(format!(
            "join `{name}` uses {uses}, but none of its inputs has a live source, so it would \
             never go live.\nUse {} for a join whose inputs are all batch.",
            variants.batch
        )));
    }

    let mut batch_stream = Vec::new();
    for (stream, class) in &classes {
        match class {
            StreamInput::Finite => batch_stream.push(*stream),
            StreamInput::Live => {}
            StreamInput::ThenLiveMerge => batch_stream.extend(
                graph
                    .inputs(*stream)
                    .into_iter()
                    .filter(|producer| !graph.roots(*producer).has_live()),
            ),
        }
    }

    Ok(PhaseWitnesses {
        catalog: catalog_witnesses
            .into_iter()
            .map(|id| graph.witness(id))
            .collect(),
        batch_stream: graph
            .sorted(batch_stream)
            .into_iter()
            .map(|id| graph.witness(id))
            .collect(),
    })
}

fn catalog_frontier(
    graph: &Graph<'_>,
    name: &str,
    uses: &str,
    catalog_type: &str,
    catalog: StageId,
) -> Result<Vec<StageId>, Refusal> {
    let up = graph.upstream_closure([catalog]);
    let frontier: Vec<StageId> = graph.sorted(up.iter().copied().filter(|stage| {
        let roots = graph.roots(*stage);
        !roots.finite.is_empty()
            && !roots.has_live()
            && graph
                .outputs(*stage)
                .any(|next| up.contains(&next) && graph.roots(next).has_live())
    }));
    let reached = graph.downstream_closure(frontier.iter().copied());
    let span: HashSet<StageId> = up.intersection(&reached).copied().collect();

    for from in graph.sorted(span.iter().copied()) {
        for (to, kind) in graph.outbound.get(&from).into_iter().flatten() {
            if *kind == EdgeKind::Backward && span.contains(to) {
                return Err(Refusal(format!(
                    "join `{name}` uses {uses}, but its catalog's batch and live sources meet \
                     inside a cycle (`{} <| {}`).\nMerge batch and live rows outside the cycle.",
                    graph.name(*to),
                    graph.name(from),
                )));
            }
        }
    }

    for stage in graph.sorted(span.iter().copied()) {
        if frontier.contains(&stage) {
            continue;
        }
        let stage_type = graph.stage_type(stage);
        if !matches!(stage_type, Some(StageType::Transform | StageType::Merge)) {
            let kind = stage_kind(stage_type);
            return Err(Refusal(format!(
                "join `{name}` uses {uses}, but its catalog mixes batch and live sources in `{}`, \
                 a {kind} stage, so the batch phase cannot tell when every batch row has \
                 arrived.\nMerge batch and live rows with `merge!({catalog_type} => \
                 merges::then_live)`, and keep {kind} work on paths that are all batch or all \
                 live.",
                graph.name(stage),
            )));
        }
    }

    let mut memo = HashMap::new();
    for stage in &frontier {
        if paths_to(graph, *stage, catalog, &span, &mut memo) > 1 {
            let (left, right) = first_fork(graph, *stage, catalog, &span, &mut memo);
            return Err(Refusal(format!(
                "join `{name}` uses {uses}, but batch rows from `{}` reach its catalog by two \
                 paths (`{}`, `{}`).\nMerge those branches before they meet live sources.",
                graph.name(*stage),
                graph.name(left),
                graph.name(right),
            )));
        }
    }
    Ok(frontier)
}

fn paths_to(
    graph: &Graph<'_>,
    stage: StageId,
    target: StageId,
    span: &HashSet<StageId>,
    memo: &mut HashMap<StageId, u8>,
) -> u8 {
    if stage == target {
        return 1;
    }
    if let Some(count) = memo.get(&stage) {
        return *count;
    }
    memo.insert(stage, 0);
    let next: Vec<StageId> = graph.outputs(stage).filter(|n| span.contains(n)).collect();
    let mut count = 0u8;
    for n in next {
        count = count
            .saturating_add(paths_to(graph, n, target, span, memo))
            .min(2);
    }
    memo.insert(stage, count);
    count
}

fn first_fork(
    graph: &Graph<'_>,
    stage: StageId,
    target: StageId,
    span: &HashSet<StageId>,
    memo: &mut HashMap<StageId, u8>,
) -> (StageId, StageId) {
    let mut current = stage;
    loop {
        let branches: Vec<StageId> = graph
            .sorted(graph.outputs(current).filter(|n| span.contains(n)))
            .into_iter()
            .filter(|n| paths_to(graph, *n, target, span, memo) > 0)
            .collect();
        match branches.as_slice() {
            [only] if *only != target => current = *only,
            [left, right, ..] => return (*left, *right),
            _ => return (current, target),
        }
    }
}

impl ThenLivePlan {
    pub(crate) fn refuse_deadlocked_holds(
        &self,
        topology: &Topology,
        descriptors: &HashMap<String, Box<dyn StageDescriptor>>,
        name_to_id: &HashMap<String, StageId>,
        enforced: &HashSet<(StageId, StageId)>,
    ) -> Result<(), Refusal> {
        let graph = Graph::new(topology, descriptors, name_to_id);
        for hold in &self.holds {
            for blocked in &hold.must_flow {
                for held in &hold.held {
                    if !enforced.contains(&(*held, hold.holder)) {
                        continue;
                    }
                    let Some(mut path) = enforced_path(&graph, enforced, *blocked, *held) else {
                        continue;
                    };
                    path.push(hold.holder);
                    return Err(Refusal(deadlock_message(
                        &graph, hold, *blocked, *held, &path,
                    )));
                }
            }
        }
        Ok(())
    }
}

fn enforced_path(
    graph: &Graph<'_>,
    enforced: &HashSet<(StageId, StageId)>,
    from: StageId,
    to: StageId,
) -> Option<Vec<StageId>> {
    let mut previous: HashMap<StageId, StageId> = HashMap::new();
    let mut seen = HashSet::from([from]);
    let mut queue = VecDeque::from([from]);
    while let Some(stage) = queue.pop_front() {
        if stage == to {
            let mut path = vec![to];
            let mut current = to;
            while let Some(prior) = previous.get(&current) {
                path.push(*prior);
                current = *prior;
            }
            path.reverse();
            return Some(path);
        }
        for next in graph.sorted(graph.outputs(stage)) {
            if enforced.contains(&(stage, next)) && seen.insert(next) {
                previous.insert(next, stage);
                queue.push_back(next);
            }
        }
    }
    None
}

fn deadlock_message(
    graph: &Graph<'_>,
    hold: &Hold,
    blocked: StageId,
    held: StageId,
    path: &[StageId],
) -> String {
    let holder = graph.name(hold.holder);
    let blocked_name = graph.name(blocked);
    let held_name = graph.name(held);
    let (opening, phase) = match hold.state {
        HoldState::Hydrating => (
            format!("join `{holder}` would deadlock while hydrating"),
            "hydration",
        ),
        HoldState::Enriching => (
            format!("join `{holder}` would deadlock while enriching the batch"),
            "the batch",
        ),
        HoldState::MergeBatch => (
            format!("merge `{holder}` would deadlock in its batch phase"),
            "its batch phase",
        ),
    };
    let path = path
        .iter()
        .map(|id| graph.name(*id))
        .collect::<Vec<_>>()
        .join(" |> ");
    let feeds = if blocked == held {
        format!("but it is also the held input `{held_name}`, on an enforced edge ({path})")
    } else {
        format!("but it also feeds the held input `{held_name}` through enforced edges ({path})")
    };
    format!(
        "{opening} under enforced backpressure.\n`{blocked_name}` must keep flowing for {phase} \
         to finish, {feeds}.\nOnce those windows fill, `{blocked_name}` blocks and never reaches \
         end of input.\nSet one edge on that path to `runtime.backpressure.mode = \"track\"`,\nor \
         feed the held input from a separate source."
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn exact(display_name: &str) -> TypeHint {
        TypeHint::Exact {
            type_id: std::any::TypeId::of::<()>(),
            display_name: display_name.to_string(),
            event_type: None,
            event_kind: None,
            schema_version: None,
        }
    }

    #[test]
    fn short_type_names_drop_module_paths() {
        assert_eq!(
            short_type_name(&exact("bank::events::AccountOpened")),
            "AccountOpened"
        );
        assert_eq!(
            short_type_name(&exact("a::Wrapper<b::c::Entry>")),
            "Wrapper<Entry>"
        );
        assert_eq!(short_type_name(&TypeHint::Unspecified), "T");
    }
}
