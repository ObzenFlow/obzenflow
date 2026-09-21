# Local PostgreSQL development service

This optional repository tooling runs PostgreSQL 17 for development and supplies
a connection profile to applications through `OBZENFLOW_POSTGRES_URL`.
Applications can also use their own PostgreSQL service.

## Development boundary

The persistent service uses passwordless PostgreSQL `trust` authentication and
explicit plaintext transport, with its host port bound to `127.0.0.1`.
Use it only on a trusted developer machine. Any process that can reach the port
or the container's Docker network can connect without proving its identity.
Loopback is not a boundary between local users.

Use your own authenticated service for shared machines, remote access,
production, or sensitive data. The checked-in [authentication policy](pg_hba.conf)
and [Compose configuration](compose.yml) define the development setup.

## Start and connect

Docker Compose is required. Run these commands from the repository root:

```console
cargo xtask postgres up
cargo xtask postgres status
cargo xtask postgres connection
```

The first start allocates an available loopback port. Later starts retain the
port, Compose project, named volume, and rows. `connection` prints the profile
and a copyable `psql` command; no credential setup is needed.

Run an application with that profile. For example, the HN digest can write to
PostgreSQL (its configured AI provider must also be available):

```console
cargo xtask postgres run -- \
  cargo run -p obzenflow --example hn_ai_digest_demo --features "http-pull ai postgres" -- \
  --config examples/hn_ai_digest_demo/obzenflow.postgres.toml
```

The wrapper supplies `OBZENFLOW_POSTGRES_URL`, `OBZENFLOW_POSTGRES_SCHEMA`, and
the explicit loopback transport selection. It removes inherited `PGPASSWORD`,
`PGPASSFILE`, internal session inputs, and disposable-test inputs before launch.
Applications launched directly use their own connection configuration.

## Stop or reset

```console
# Stop the service and retain its data and assigned port.
cargo xtask postgres down

# Delete the owned volume and lifecycle state.
cargo xtask postgres down --volumes
```

If the retained port is occupied, `up` fails rather than choosing a different
one. To allocate a new port, run `down --volumes` and then `up`; this also
discards the development data.

Lifecycle state lives in `.obzenflow/postgres/development/state.tsv` and contains
no credentials or TLS material. If that state is missing while the volume
remains, xtask refuses to adopt or delete it. Restore the matching state or
remove the exact Docker resources manually.

## Disposable acceptance tests

```console
cargo xtask postgres test
```

This command requires OpenSSL as well as Docker Compose. Each run provisions
an isolated service with generated credentials and verified TLS, then removes
its resources when the suite finishes. Temporary state, credentials, and
certificate material live under `target/postgres-sessions/<run-id>`.

The acceptance service is separate from the persistent development service.
It tests authenticated connector behavior without changing the local
development profile.

## Implementation

[SQL fixtures](fixtures/) define the payment, digest, and inventory tables.
[The Rust xtask](../../xtask/src/postgres/mod.rs) manages Compose, lifecycle
state, and fixture installation.
