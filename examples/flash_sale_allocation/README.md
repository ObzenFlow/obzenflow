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

Source: [flow](flow.rs) and [allocation handler](allocation.rs).
See the [examples index](../README.md) for published tutorials.
