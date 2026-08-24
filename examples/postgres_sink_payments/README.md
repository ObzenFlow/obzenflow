# PostgreSQL payment sink

This example maps `PaymentAuthorized` domain events into a PostgreSQL table. It
shows the application-facing connector surface only: typed parameter binding,
a fixed insert statement, batching, redelivery safety, and flow topology.

Start the repository-managed PostgreSQL 17 service:

```console
cargo xtask postgres up
```

The command provisions the table from
[`dev/postgres/fixtures/payments.sql`](../../dev/postgres/fixtures/payments.sql)
and keeps the connection on loopback with hostname-verified TLS. Run the
example inside that environment:

```console
cargo xtask postgres run -- \
  cargo run -p obzenflow --features postgres --example postgres_sink_payments
```

`OBZENFLOW_POSTGRES_URL` is required when the example is launched directly. It
must select `sslmode=verify-full`; private certificate authorities can be
provided with the URL's `sslrootcert` option. `OBZENFLOW_POSTGRES_SCHEMA`
defaults to `obzenflow_example`, and `OBZENFLOW_JOURNAL_ROOT` defaults to
`target/postgres-sink-payments`.

The sink uses `payment_id` as its conflict target. Re-delivering an archived
event therefore updates the same logical payment and converges on the same
destination state. That is the reason the flow can declare `SafeToRepeat`.

To inspect the local destination without putting database access into the
application itself:

```console
cargo xtask postgres run -- sh -c \
  'psql "$OBZENFLOW_POSTGRES_URL" -c "TABLE obzenflow_example.payments"'
```

Stop the service while retaining its data with `cargo xtask postgres down`, or
remove the local data volume with `cargo xtask postgres down --volumes`.
