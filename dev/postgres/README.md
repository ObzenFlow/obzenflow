# Local PostgreSQL development service

ObzenFlow applications can use any compatible PostgreSQL service through
`OBZENFLOW_POSTGRES_URL`. This directory contains optional repository tooling:
one persistent PostgreSQL 17 service for local development and a separate,
disposable service for acceptance tests.

## Persistent development session

Docker Compose and OpenSSL are required. Start or inspect the service with:

```console
cargo xtask postgres up
cargo xtask postgres status
cargo xtask postgres connection
```

The first `up` asks Docker for an available `127.0.0.1` port and retains that
port, the Compose project, and the named volume under the ignored
`.obzenflow/postgres/development` directory. It also creates an independent
checkout-local password as two owner-only files:

- `password`, mounted into the official image through `POSTGRES_PASSWORD_FILE`;
- `pgpass`, containing the exact retained host, port, database, and user.

The generated CA certificate is public, so `up`, `status`, and `connection`
publish a gitignored copy at `dev/postgres/local-ca.crt`. This visible path can
be selected directly in GUI clients such as TablePlus; secret and state files
remain under `.obzenflow`.

The password is never printed or placed in a connection URL or ordinary child
environment value. `connection` prints the non-secret profile and a copyable
`psql` command that removes inherited `PGPASSWORD` and selects the managed
`PGPASSFILE`.

The assigned port is automatic and stable. If another process occupies it after
a normal shutdown, `up` fails instead of silently rebinding. To request a new
automatic endpoint, explicitly discard the development session and start again:

```console
cargo xtask postgres down --volumes
cargo xtask postgres up
```

## Running applications

Run an application with the managed profile:

```console
cargo xtask postgres run -- \
  cargo run -p obzenflow --features postgres --example postgres_sink_payments
```

The child receives a password-free `OBZENFLOW_POSTGRES_URL`, the exact
`PGPASSFILE`, and `OBZENFLOW_POSTGRES_SCHEMA`. The wrapper removes inherited
`PGPASSWORD`, internal Compose/session inputs, and all disposable-test inputs
before launch. It validates both credential files and their endpoint match first,
so SQLx cannot fall through to an unrelated default pgpass file.

This wrapper is optional. Applications launched directly own their externally
supplied URL, trust configuration, and credential mechanism; they never consult
xtask state.

## Shutdown, reset, and interrupted setup

```console
# Stop the container; retain port, volume, credentials, and rows.
cargo xtask postgres down

# Delete the exact owned volume, credentials, certificates, visible CA copy, and state.
cargo xtask postgres down --volumes
```

First startup is transactional: provisional state and the raw credential are
written before Compose starts; the exact pgpass and ready state are committed
only after Docker reports the published port and named volume. If setup is
interrupted, or retained credentials/certificates are missing or malformed, the
tool refuses to regenerate them against retained data. Use the explicit
`down --volumes` reset shown above.

## Disposable acceptance suite

```console
cargo xtask postgres test
```

Each run creates its own project, dynamic port, volume, state directory,
credential, pgpass, TLS material, and schemas under
`target/postgres-sessions/<run-id>`. The coordinator supplies test-only inputs to
its named children and removes the disposable project and files when the suite
finishes.
