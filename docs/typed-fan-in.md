# Authoring Multi-Input Stages

After FLOWIP-114c, every public DSL-authored stage carries exact typing
metadata. This page is the practical guide for writing flows where one
downstream stage receives input from more than one upstream.

## The invariant

Every stage except joins is homogeneous at its fan-in. Two upstreams of
different concrete types into the same non-join slot is a build error
(`FlowBuildError::EdgeTypingMismatch`). The fix is one of three patterns
below.

## Pattern 1 — Two typed inputs of different types: use a join

A `join!` is the canonical typed two-input operator in ObzenFlow. The
`reference` and `stream` slots are independently typed. If you have two
upstreams of different concrete types and they need to combine into one
event, that is a join.

```rust
enrich = join!(
    catalog promotions: Promotion,
    EnrichedOrder -> EnrichedOrderWithPromo => promo_enrich_handler
);
```

This matches Apache Flink's `CoMapFunction`, Apache Beam's `CoGroupByKey`,
and Akka Streams' typed two-input shapes.

## Pattern 2 — Three or more inputs: per-branch alignment transforms

When three or more upstream sources need to feed one downstream, insert a
small transform stage on each branch that maps the source-specific type to
a common envelope type. The fan-in itself then becomes homogeneous.

```text
source_a : TypeA   ->  align_a : TypeA   -> Envelope  ┐
source_b : TypeB   ->  align_b : TypeB   -> Envelope  ┼-> aggregator : Envelope -> Result -> sink
source_c : TypeC   ->  align_c : TypeC   -> Envelope  ┘
```

Every edge carries one type. Every stage has one input. Every alignment is
visible in the topology graph, so a reader of `/api/topology` or the Studio
canvas can see exactly where each upstream is being normalised.

The canonical worked example lives at
`examples/multi_source_ingest_demo.rs`. Read it first if this is your first
time writing N-input typed fan-in.

This pattern is the dominant idiom in Flink, Beam, and Kafka Streams: align
with `map` first, then `union` on the aligned streams.

## Pattern 3 — Allowed but not idiomatic: enum or envelope payload

A stage can declare its `output_type` as a sealed enum (e.g. `RoutedEvent`)
whose variants carry the original payload shapes, and emit different variants
on different events. All readers observe a single stream of one type, so
every downstream's fan-in remains homogeneous at the type level.

```rust
#[derive(Clone, Debug, Serialize, Deserialize)]
enum RoutedEvent {
    Low(LowPriorityPayload),
    Medium(MediumPriorityPayload),
    High(HighPriorityPayload),
}
```

This pattern works and is occasionally appropriate, particularly when the
upstream itself is a single source that genuinely produces multiple shapes
that are downstream-by-construction one logical event family. It is not the
recommended default because:

- The alignment is hidden inside one stage rather than expressed as a
  visible per-branch transform in the topology graph.
- Growing the variant set requires editing the upstream stage rather than
  adding a new branch with its own alignment transform.
- The downstream still has to switch on the variant tag at runtime.

ObzenFlow does not block this pattern at flow build because the type-level
rule is satisfied. Authors are expected to prefer per-branch alignment
unless the upstream source is naturally union-shaped.

## What you'll see if you violate the rule

A typed flow that wires two upstreams of differing `Exact` types into the
same non-join downstream slot fails at flow build with:

```
FlowBuildError::EdgeTypingMismatch {
    upstream_stage: "...",
    downstream_stage: "...",
    role: "Input" | "Reference" | "Stream",
    expected_type: "...",
    actual_type: "...",
    kind: SingleEdge | HeterogeneousFanIn { other_upstream_stages, other_actual_types },
    suggested_fix: "Insert an alignment transform, use a join for two-input typed fan-in, ...",
}
```

The error names every conflicting stage and type, plus the suggested fix.

## Why the runtime is still type-erased

ObzenFlow's runtime operates on `ChainEvent` (a type-erased wire shape) for
durability, replay, and provenance. The typing metadata declared at the
macro site is a *fingerprint* (in the Apache Flink `TypeInformation` /
Apache Beam `TypeDescriptor` / Kafka Streams `Serde` sense), not a Rust
type-system constraint applied to the runtime. The fingerprint is canonical
within one compilation via `std::any::TypeId`, so two stages declared with
the same Rust type but different syntactic spellings compare as equal.

See FLOWIP-114b "Runtime invariant: one stage = one journal = one output
contract" for the architectural reason this is the right model under the
current runtime.
