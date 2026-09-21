# Managed web authentication and lifecycle

`FlowApplication` owns the managed HTTP listener. It defaults to loopback.
Restrict network access to the callers that need it and use deployment
infrastructure for protected transport and broader identity integration.
A private address does not authenticate callers; API keys and HMAC do not
encrypt connections.

## Authentication scopes

- **Control plane:** built-in operational routes, including `/metrics`,
  `/api/topology`, `/api/config/*`, `/api/flow/*`, and SSE. Non-loopback
  exposure requires API-key or HMAC authentication. `/health` and `/ready`
  are exempt.
- **Managed surfaces and routes:** application-authored endpoints. A protected
  surface applies its policy to every route beneath it, including custom health routes.
- **Ingress-local authentication:** independently configured for ingestion.
  A control-plane key does not protect ingestion POSTs or other attached routes.

| Surface policy | Route policy | Result |
| --- | --- | --- |
| Protected | Unset or identical | Inherit the surface policy. |
| Protected | `None` or different | Startup error. |
| Unset or `None` | Protected | Use the route policy. |
| Unset or `None` | Unset or `None` | No managed authentication. |

Protected means `ApiKey` or `HmacSha256`. Identical declarations match every
field, including header spelling and environment-reference names; equal secret
values under different names are insufficient. Request-header matching remains
case-insensitive. Leave route auth unset to inherit a protected surface.

## Configure access

Local development can leave authentication unset when all loopback callers are
trusted. Any explicitly configured policy still requires valid credentials.
For private operational access, provision a credential out of band and reference it:

```toml
[server]
enabled = true
host = "10.20.0.15" # Replace with an assigned private interface.

[server.control_plane_auth]
mode = "api_key"
value_env = "OBZENFLOW_CONTROL_PLANE_AUTH"
```

The environment value is the complete expected header value. The default header
is `Authorization`; the framework neither adds `Bearer` nor trims the value.
HMAC clients must implement the configured signing and timestamp protocol.

Use separate protected surfaces for distinct credentials. This separates access
policies, not tenant state inside a flow. Rotate credentials by changing the next
process's environment and restarting. Live environment mutation is unsupported;
SSE authentication happens when the connection opens.

## Authentication failures

| Condition | Result |
| --- | --- |
| Missing, empty, or non-Unicode configured secret; conflicting policies | Startup fails before listening or automatic execution. |
| Invalid configured material encountered during a request | HTTP 500. |
| Missing, malformed, or incorrect caller credentials | HTTP 401. |

Rejected requests do not invoke the endpoint. Diagnostics identify the
configuration problem without exposing credential values.

## Startup and shutdown

The application validates authentication and CORS, then binds the socket before
releasing automatic execution. Invalid configuration or an occupied port fails
startup and stops and joins the materialised flow. Logs and Studio registration
use the actual bound address. `/ready` returns 200 only while the pipeline is
`Running`.

Unexpected listener completion or panic fails the application and initiates
bounded flow shutdown. That host failure retains precedence over later
execution or cleanup results. Runtime owns stop admission and its deadline;
repeating a graceful stop does not extend that deadline.

| Flow outcome | Terminal record |
| --- | --- |
| Successful graceful drain with only finite sources | `pipeline_completed` |
| Graceful stop with an infinite source, explicit cancellation, or stop timeout | `pipeline_cancelled` |
| Accepted execution failure | `pipeline_failed` |

Finite-source completion after a stop means admitted work drained; unread source
input may remain. Already-polled output is published before graceful EOF, subject
to the existing cancellation bounds. A later shutdown signal cannot turn an
accepted execution failure into success. Intentional cancellation is a
successful application teardown.

The host stays available through normal drain and terminal publication.
Shutdown then stops accepting requests, allows five seconds for existing
responses, and aborts and joins remaining connection and SSE tasks.
Restarting requires another `FlowApplication` invocation.

`FlowHandle::run()` and `wait_for_completion()` report execution results,
including failure and acknowledged cancellation. Resource joins alone do not
prove success: missing required terminal evidence, journal failure, panic, and
unexpected task abortion remain errors. Implementation details live in
[the application lifecycle](../application/managed_lifecycle/mod.rs).

## Endpoint integration

Integration authors implement `HttpEndpoint` or attach a `WebSurface` through
the application builder. Listener construction belongs to Infra.

`HttpEndpoint::handle` and `RouteHandler::handle` return
`Result<ManagedResponse, EndpointError>`. Return intentional HTTP responses,
including 4xx and 5xx, as `Ok(ManagedResponse::Unary(response))`. Use
`EndpointError` when the handler cannot produce a response; its authored context
should be static and free of credentials or request values.

Endpoint errors and invalid unary response metadata produce HTTP 500 with a
`text/plain` body of `Internal Server Error`. The host records the selected
response once and logs the registered route and safe context. It does not retry
the handler or fail the host. Error display omits the underlying source, which
remains available through `std::error::Error::source` for explicit diagnostics.
