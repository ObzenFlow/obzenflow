# Flash-sale allocation

Reserve and release limited stock with a stateful handler whose warehouse
effects are recorded for replay.

Run from the repository root. The warehouse is simulated; no external service
or extra Cargo feature is required.

```sh
cargo run -p obzenflow --example flash_sale_allocation
```

The scripted orders exercise successful reservations, sold-out stock,
cancellation, and refused reservations. To replay without calling the warehouse,
use the archive path printed by the live run:

```sh
cargo run -p obzenflow --example flash_sale_allocation -- \
  --replay-from <archive> --verify
```

The simulated warehouse is configured under `[app.warehouse]` in
[`obzenflow.toml`](obzenflow.toml), and the run prints these settings at
startup. The warehouse id is binding evidence for every warehouse effect. Copy
the file, change `id`, and replay the same archive with the copy:

```sh
cargo run -p obzenflow --example flash_sale_allocation -- \
  --config <copy.toml> --replay-from <archive>
```

The replay names `app.warehouse.id` as changed, then fails the allocator with
`descriptor_mismatch` before it calls the warehouse. Replaying with the
original file succeeds.

## Crash and resume

Staff also take three reservations by phone. A reservation desk gives every
reservation, phone or online, the next confirmation number in the order
reservations reach it. To crash a run mid-allocation, copy the file, set
`reserve_latency_ms = 0` and `reservation_pace_ms = 1000`, and run with the
copy:

```sh
cargo run -p obzenflow --example flash_sale_allocation -- --config <copy.toml>
```

Kill the process with `kill -9` while it is still allocating, about two seconds
in, then resume the killed run with the same copy:

```sh
cargo run -p obzenflow --example flash_sale_allocation -- \
  --config <copy.toml> --resume-from <archive>
```

The resume reproduces every order the killed run settled without calling the
warehouse, then allocates the rest live. An order that finds the stock sold out
settles without a call. Replay the resumed run with `--verify`, and every
reservation keeps the confirmation number the resumed run issued:

```sh
cargo run -p obzenflow --example flash_sale_allocation -- \
  --config <copy.toml> --replay-from <resumed-archive> --verify
```

`show` marks a confirmation `[replay]` when the desk computed it from a
reservation the killed run recorded, even one the desk had not reached before
the kill. The `confirmations` sink still delivers that confirmation live the
first time it sees it.

Source: [flow](flow.rs), [allocation handler](allocation.rs) and
[reservation desk](desk.rs).
See the [examples index](../README.md) for published tutorials.
