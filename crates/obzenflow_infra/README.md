# ObzenFlow Infrastructure

Infra assembles the framework into a running application. Application authors
use `obzenflow::application::FlowApplication`, `obzenflow::application::ingress`,
`obzenflow::journal`, and the facade's capability features.

This outer layer depends on Core, Runtime, Adapters, and flow construction.
The direct crate APIs and feature names below are for framework integration.

## Responsibilities

- Application configuration, tracing, CLI arguments, startup, and shutdown.
- Disk and memory journals, archive loading, inspection, and replay verification.
- Managed HTTP hosting, ingress, operational endpoints, and authentication.
- Prometheus hosting and Studio connections, using Adapter projections.
- Outbound HTTP clients, AI provider bindings, and tokenization.
- Typed environment parsing with errors that distinguish missing and malformed values.

`FlowApplication::run(flow).await` is the usual entry point. Its builder supplies
presentation, web surfaces, and other application options. The application owns
the listener and background tasks and joins them during shutdown.

## Integration features

No features are enabled by default. Compiling a capability does not enable its
configured service.

| Infra feature | Capability |
| --- | --- |
| `warp-server` | Managed HTTP host. |
| `reqwest-client` | Default outbound HTTP client. |
| `prometheus` | Prometheus reporting integration. |
| `studio-registration` | Studio registration, with HTTP and Prometheus support. |
| `ai-rig` | AI provider bindings. |
| `ai-tiktoken` | Tiktoken token counting. |
| `tokio-console` | Tokio Console instrumentation. |
| `test-support` | Framework integration-test support. |

Applications select the corresponding public features on `obzenflow`, such as
`web-host`, `http-pull`, `ai`, `prometheus`, and `studio`.

## References

- [Managed web authentication and lifecycle](https://github.com/obzenflow/obzenflow/blob/main/crates/obzenflow_infra/src/web/README.md)
- [Journal format](https://github.com/obzenflow/obzenflow/blob/main/crates/obzenflow_infra/src/journal/disk/codec/README.md)
- [Application configuration](https://github.com/obzenflow/obzenflow/blob/main/crates/obzenflow_infra/src/application/config.rs)

## License

Dual-licensed under MIT OR Apache-2.0. See `LICENSE-MIT` and `LICENSE-APACHE`.
