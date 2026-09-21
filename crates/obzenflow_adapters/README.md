# ObzenFlow Adapters

Adapters provides concrete sources, sinks, policies, observers, and reporting
projections. Application authors use them through `obzenflow::stages`,
`obzenflow::middleware`, and the other facade modules.

This layer depends on Core and Runtime. Infra assembles adapters with network
clients, configuration, and hosting.

## Responsibilities

- **Sources:** CSV input, HTTP pull and polling with pluggable decoders, and hosted ingress sources.
- **Sinks:** console formatting, CSV output, and optional PostgreSQL delivery.
- **Policies:** rate limiting, circuit breaking, and retry within effect resilience.
- **Observers:** passive callbacks at source, handler, stateful, join, effect, delivery, and lifecycle boundaries.
- **Prometheus:** a read model and projection that turn measurement snapshots into metrics.
- **Studio:** projections that turn journal records into status messages for connected clients.

Runtime owns execution and measurement collection. Infra owns listeners and
background-task cleanup. Reporting projections do not acquire execution authority.

See the [middleware guide](https://github.com/obzenflow/obzenflow/blob/main/crates/obzenflow_adapters/src/middleware/README.md)
for policy boundaries and an application-owned observer.

## License

Dual-licensed under MIT OR Apache-2.0. See `LICENSE-MIT` and `LICENSE-APACHE`.
