# PostgreSQL payment sink

Deliver typed payment events to PostgreSQL with an UPSERT keyed by `payment_id`,
so repeated delivery updates the same row.

Run from the repository root. The optional local setup requires Docker Compose
on a trusted developer machine; [the PostgreSQL guide](../../dev/postgres/README.md)
covers its access boundary, connection settings, and shutdown.

```sh
cargo xtask postgres up
cargo xtask postgres run -- \
  cargo run -p obzenflow --features postgres --example postgres_sink_payments
```

To replay and verify, use the archive path printed by the live run:

```sh
cargo xtask postgres run -- \
  cargo run -p obzenflow --features postgres --example postgres_sink_payments -- \
  --replay-from <archive> --verify
```

For your own PostgreSQL 17 service, provision [the payments table](../../dev/postgres/fixtures/payments.sql),
set `OBZENFLOW_POSTGRES_URL`, and run the inner `cargo run` command directly.
Connections default to verified TLS and require `sslmode=verify-full`;
use `sslrootcert` for a private certificate authority.
`OBZENFLOW_POSTGRES_SCHEMA` defaults to `obzenflow_example`.

Source: [flow](flow.rs) and [payment parameter binding](domain.rs).
See the [examples index](../README.md) for published tutorials.
