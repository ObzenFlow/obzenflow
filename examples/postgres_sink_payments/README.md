# PostgreSQL payment sink

This example maps `PaymentAuthorized` domain events into a PostgreSQL table. It
shows the application-facing connector surface only: typed parameter binding,
a fixed insert statement, batching, redelivery safety, and flow topology.

The application accepts any PostgreSQL 17 backing service through configuration:

```console
export PGPASSFILE='/secure/path/to/pgpass'
export OBZENFLOW_POSTGRES_URL='postgresql://app_user@database.example:5432/app?sslmode=verify-full&sslrootcert=/path/to/ca.crt'
export OBZENFLOW_POSTGRES_SCHEMA='payments'
cargo run -p obzenflow --features postgres --example postgres_sink_payments
```

The backing service is not launched or owned by the application. An absent
`OBZENFLOW_POSTGRES_TRANSPORT` defaults to `verified-tls`, so the URL must select
`sslmode=verify-full`; private certificate authorities can be provided with its
`sslrootcert` option. An application may explicitly set the transport to
`externally-protected-plaintext` only when another local boundary such as loopback,
a Unix socket, sidecar, or tunnel protects the connection.
`OBZENFLOW_POSTGRES_SCHEMA` defaults to `obzenflow_example`, and
`OBZENFLOW_JOURNAL_ROOT` defaults to `target/postgres-sink-payments`.

For local development, an optional repository-managed PostgreSQL 17 service is
available:

```console
cargo xtask postgres up
cargo xtask postgres connection
```

The command provisions the table from
[the repository payments fixture](https://github.com/obzenflow/obzenflow/blob/main/dev/postgres/fixtures/payments.sql)
and keeps the connection on loopback with a generated password and explicit
externally protected plaintext transport. It generates no local certificates.
Its first start allocates an available port and normal restarts retain it.
`connection` prints the non-secret fields, managed pgpass path, and a copyable
password-free `psql` command. To allocate a new automatic port, discard the
session explicitly with `cargo xtask postgres down --volumes`, then run `up`
again.

Run the example inside that optional local environment:

```console
cargo xtask postgres run -- \
  cargo run -p obzenflow --features postgres --example postgres_sink_payments
```

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
remove the local data volume with `cargo xtask postgres down --volumes`. Normal
shutdown retains the endpoint, Compose project, named volume, generated
credentials, and rows; `status` verifies the retained endpoint and Docker
authority before presenting the service.
