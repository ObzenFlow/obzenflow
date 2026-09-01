# Local PostgreSQL development service

ObzenFlow applications can use any compatible PostgreSQL service through
`OBZENFLOW_POSTGRES_URL`. This directory contains optional repository tooling:
one persistent PostgreSQL 17 service for local development and a separate,
disposable service for acceptance tests.

## Security boundary

The persistent service is deliberately passwordless. PostgreSQL uses `trust`
authentication and Docker publishes its only host port on `127.0.0.1`. It also
uses plaintext transport through ObzenFlow's explicit
`ExternallyProtectedPlaintext` mode. No password, pgpass file, CA, certificate,
or private key is created for development.

This is appropriate only on a trusted, contributor-controlled development
machine, such as a developer laptop where the local users and running software
are trusted. It is not appropriate on a shared workstation, an untrusted host,
a remotely reachable Docker daemon, or for production or sensitive data.
PostgreSQL `trust` authentication means that any process able to reach the
published endpoint, or a container deliberately given access to its Docker
network, can connect as any database role without proving its identity. The
loopback bind limits exposure; it does not create a local-user authentication
boundary.

This trade-off is intentional. A retained plaintext password file would protect
against another local principal that could reach the port but could not read the
file, while providing no useful protection from software running as the
contributor or from Docker-daemon administrators. That narrower multi-user
boundary is outside this development workflow. Deployments that need it must use
their own authenticated PostgreSQL service and credential store.

The checked-in `pg_hba.conf` makes the development-only policy explicit and
reviewable on every container start. `compose.yml` also supplies
`POSTGRES_HOST_AUTH_METHOD=trust` for first-time image initialisation and mounts
that policy for retained volumes. The loopback publication and trust policy are
tested together so neither can drift independently.

The disposable `cargo xtask postgres test` service is different: it retains an
independent generated credential and real verified TLS because it is the
acceptance proof for authenticated connector behaviour.

## Persistent development session

Docker Compose is required. Start or inspect the service with:

```console
cargo xtask postgres up
cargo xtask postgres status
cargo xtask postgres connection
```

The first `up` asks Docker for an available `127.0.0.1` port and retains that
port, the Compose project, and the named volume. The ignored
`.obzenflow/postgres/development` directory contains only non-secret lifecycle
state in `state.tsv`. It contains no database credential or TLS material.

The assigned port is automatic and stable. If another process occupies it after
a normal shutdown, `up` fails instead of silently rebinding. To request a new
automatic endpoint, explicitly discard the development session and start again:

```console
cargo xtask postgres down --volumes
cargo xtask postgres up
```

`connection` prints the passwordless profile and a plain, copyable `psql`
command. No credential setup is required.

## Running applications

Run an application with the managed profile:

```console
cargo xtask postgres run -- \
  cargo run -p obzenflow --features postgres --example postgres_sink_payments
```

The child receives a password-free `OBZENFLOW_POSTGRES_URL`,
`OBZENFLOW_POSTGRES_SCHEMA`, and the explicit loopback transport selection. The
wrapper removes inherited `PGPASSWORD`, `PGPASSFILE`, internal Compose/session
inputs, and all disposable-test inputs before launch.

This wrapper is optional. Applications launched directly own their externally
supplied URL, authentication policy, transport protection, and credential
mechanism; they never consult xtask state.

## Shutdown, reset, and retained state

```console
# Stop the container; retain the port, volume, lifecycle state, and rows.
cargo xtask postgres down

# Delete the exact owned volume and lifecycle state.
cargo xtask postgres down --volumes
```

If checkout state is missing while the deterministic volume remains, xtask
refuses to adopt or delete the volume. Restore the matching non-secret state or
remove the exact Docker resources manually. This keeps lifecycle and deletion
authority narrow even though authentication is passwordless.

During the transition from the earlier generated-password prototype, the next
successful `up` reconciles the running container to the checked-in trust policy
and removes the obsolete `password` and `pgpass` files from this checkout while
retaining the named volume and its rows.

## Disposable acceptance suite

```console
cargo xtask postgres test
```

Each run creates its own project, dynamic port, data and TLS volumes, state
directory, credential, pgpass, short-lived TLS material, and schemas under
`target/postgres-sessions/<run-id>`. OpenSSL is required only for this explicit
acceptance command. The unpublished xtask Compose profile prepares the server
certificate and key through a disposable setup service, retains FLOWIP-083c's
real TLS proof, and removes the project and files when the suite finishes.
