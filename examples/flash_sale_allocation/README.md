# Flash-sale allocation

This example is the canonical narrow use for `effectful_stateful!`: a serial
Process Manager whose external command depends on current state and whose
next decision depends on the committed outcome of an earlier command.

The allocator owns a capacity of one unit for `flash-sku` and the
order-to-warehouse-hold map. An `OrderPlaced` calls `fx.reserve_stock(...)`
only while folded allocation is below capacity. A successful `StockReserved`
fact occupies the slot. An `OrderCancelled` calls `fx.release_stock(...)` only
when that order has a folded hold, and the committed `StockReleased` fact
frees the slot. `apply` performs no I/O; it rebuilds this authoritative state
only from committed domain facts.

## Why this is a Process Manager

Use the smallest feed-forward shape that preserves domain authority:

| | Outcome does not influence a later decision | Outcome influences a later decision |
|---|---|---|
| Command independent of current state | `effectful_transform!` | transform, then stateful projection |
| Command depends on current state | stateful command fact, then transform/outbox | `effectful_stateful!` Process Manager |

This example is the bottom-right cell. The same order-placement input issues a
reserve command when capacity is free and no command when capacity is full.
The reserve outcome then changes whether a later order can reserve and whether
a later cancellation must release a hold.

A join can correlate inputs, commands, and outcomes, but it cannot close this
outcome-to-next-decision loop without feeding the result back to the state
authority. If a warehouse service owned capacity instead, unconditional
reservation would be an ordinary `effectful_transform!`; that is a valid but
different ownership model.

The tradeoff is deliberate head-of-line blocking. The allocator processes its
authority serially, so an admitted warehouse call holds that lane until the
effect settles. Remote enrichment and independent writes should stay in
effectful transforms.

## Policy and domain facts

The stage uses two exact named bindings. `reserve_resilience` protects only
`ReserveStock`; `ReleaseStock` remains callable while the reserve breaker is
open. The runtime records a rejected reserve as framework effect evidence. The
user-owned allocator recognizes only the structured circuit-breaker cause and
authors the flat `ReservationFailed { order_id, sku }` fact. That fact is
journalled and routed directly to `sink!(ReservationFailed => ...)`; no
framework error or compiler carrier enters its type or payload.

`AllocationOutput` is merely Rust proof that every `apply` input belongs to the
flat stage arrow. It is not a `TypedPayload`, is not in topology metadata, and
is never journalled.

## Run and prove replay

```text
cargo run -p obzenflow --example flash_sale_allocation
cargo run -p obzenflow --example flash_sale_allocation -- \
  --replay-from target/flash-sale-allocation-logs/flows/<flow_id> --verify
```

The scripted live run performs one slow reserve, observes `SoldOut` while that
hold occupies capacity, releases it through the unprotected release binding,
then records two reserve-policy rejections as two `ReservationFailed` facts.
Strict replay reconstructs the same journal with zero physical warehouse
calls. The journal and `--verify` verdict are the oracle; console output is
only a teaching aid.

