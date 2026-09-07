# Managed web authentication

ObzenFlow is a durable execution framework intended for deliberately restricted network exposure.
The managed listener defaults to loopback. Restrict network access to the operators and services
that need it; a private IP address alone does not authenticate callers. Use deployment infrastructure,
such as an authenticating gateway, for broader identity integration and protected transport.
API keys and HMAC do not encrypt connections.

## Choose the authentication scope

- **Control-plane authentication** protects built-in operational routes such as `/metrics`,
  `/api/topology`, `/api/config/*`, and `/api/flow/*`, including SSE. Non-loopback exposure of these
  routes requires an API-key or HMAC policy. Built-in `/health` and `/ready` are exempt.
- **Managed surface/route authentication** protects application-authored routes. A protected surface
  fixes the authentication requirement for every route beneath it. A custom `/reports/health`
  route inherits protection just like its siblings.
- **Ingress-local semantic authentication** is configured independently for HTTP ingestion.
  A control-plane key does not protect ingestion POSTs or other attached application routes.

Surface and route declarations follow this matrix:

| Surface policy | Route policy | Result |
|---|---|---|
| Protected | Unset or identical | Use surface policy |
| Protected | `None` or different | Startup configuration error |
| Unset or `None` | Protected | Use route policy |
| Unset or `None` | Unset or `None` | No managed authentication |

Protected means `ApiKey` or `HmacSha256`. Unset means no declaration; `None` means the explicit
`AuthPolicy::None` variant. Identical means equality of the complete declaration, including header
spelling, environment-reference names, and all HMAC fields. Equal secret values under different
names do not make declarations identical. Request-header matching remains case-insensitive.
Prefer inheritance by leaving route auth unset beneath a protected surface.

## Common deployment choices

For local development, leave authentication unset on loopback when all local callers are trusted.
Explicitly configured authentication requires valid material even on loopback.

For private operational access, restrict the listener's network exposure and configure a control-plane
API key. Provision the referenced environment variable out of band before starting the application:

```toml
[server]
enabled = true
host = "10.20.0.15" # An illustrative assigned private interface.

[server.control_plane_auth]
mode = "api_key"
value_env = "OBZENFLOW_CONTROL_PLANE_AUTH"
```

The environment value is the complete expected header value. The default API-key header is
`Authorization`; ObzenFlow does not prepend `Bearer` or trim the secret. Clients must send the
same full value. HMAC clients must implement the configured signature and optional timestamp
protocol; a static-header operational client usually fits an API key.

For integrations needing different credentials, author separate protected surfaces. New routes
then inherit their surface's requirement. Under an unprotected surface, every protected route
needs its own declaration, and a newly added route with no declaration is unprotected.
Separating credentials does not establish tenant isolation inside the flow.

## Failures and upgrades

Missing, non-Unicode, or empty configured secrets and conflicting declarations fail admission
before the listener task is spawned or automatic execution starts. Invalid material encountered
at request time returns 500. Missing, malformed, or wrong caller credentials against valid material
return 401. Rejected requests never invoke their endpoint. Diagnostics identify the configuration
problem without exposing credentials.

Rotate environment-backed credentials by updating the next process's environment and restarting.
The per-request lookup provides defensive validation; live process-environment mutation is not a
supported rotation mechanism. SSE authentication occurs when a stream opens.

On upgrade, provision non-empty material and remove route declarations that replace a protected
surface's policy. Inherit the surface requirement or regroup routes into separately protected surfaces.

## Managed host lifecycle

`FlowApplication` validates the route policies and CORS origins, then binds the real socket before
reporting startup or releasing automatic `Run`. Invalid CORS origins and occupied ports return
startup errors and leave sources unstarted; the materialised flow is stopped and joined. CORS
validation uses Warp's origin representation and normalised header conversion, preserving valid
allow-list behaviour. Diagnostics name the invalid configuration entry without echoing its value.
Startup logs and Studio registration use the actual bound address. `/ready` reads Runtime's current
pipeline state: only `Running` returns 200.

Unexpected listener completion or panic makes the application fail. That failure stays primary even
if the flow finishes successfully or a shutdown signal arrives concurrently. A live flow receives a
bounded graceful stop; an existing stop keeps its admitted deadline. Runtime owns stop admission,
cancellation and terminal journal publication. Repeating a graceful stop changes neither its deadline
nor its actions. Cancellation is absorbing; timeout cancellation requires an expired graceful stop.
`FlowHandle::stop_status_receiver()` observes admission, while a successful stop call only queues a
request.

Runtime retains the first accepted execution failure independently of stop admission. A later
SIGINT or SIGTERM cannot turn that failure into successful application exit or a cancelled journal
outcome. The application interprets Runtime's retained outcome only after the supervisor joins;
append failure, missing terminal publication for an executed flow, panic and task abortion remain
errors. Host failures retain precedence over execution or cleanup failures.

A successful graceful drain of a started flow with only finite sources publishes
`pipeline_completed`. This means admitted work drained, not that unread source input was exhausted.
Finite and infinite sources, both synchronous and asynchronous, publish already-polled output
before their graceful EOF. Waiting for output credit still uses the existing cancellation bounds.
Graceful stop with an infinite source, explicit cancellation and timeout escalation publish
`pipeline_cancelled`. Accepted execution failure publishes `pipeline_failed` in every case.
Intentional cancellation is a successful application teardown. Once execution is admitted,
`FlowHandle::run()` uses the same Runtime outcome, so acknowledged cancellation returns `Ok(())`
instead of being mistaken for failure because cancellation shares the historical `Failed` FSM
cleanup state.

The cross-crate `FlowHandle::wait_for_termination()` seam is now `#[doc(hidden)] pub` and is not a
supported public completion-observer API. Internally, all completion paths share one physical join
and retain its result. Concurrent, dropped and repeated waits cannot consume another caller's
completion capability. `SupervisorHandle::wait_for_completion()` remains a physical task join;
it does not interpret the pipeline's execution outcome. `HandleError` adds `SupervisorAborted`,
so exhaustive downstream matches need that case. Emergency `abort_and_wait()` accepts confirmed
abortion as successful teardown, while ordinary completion observation still reports it as an error.

The listener remains available through normal drain and terminal publication. Closing then stops
new requests, allows five seconds for existing responses, and aborts and joins remaining connection,
HTTP/2 and framework SSE tasks. The same ownership applies when using `run_async` inside a runtime
that continues after the application exits. Restart requires another `FlowApplication` invocation.

## Endpoint errors and API migration

Implement `HttpEndpoint` or attach a `WebSurface` through `FlowApplication`; listener construction is
private to Infra. The former Core server SPI, server configuration and TLS placeholder types, Infra
server factory, concrete Warp server export and standalone start functions have been removed without
compatibility aliases. Existing application server configuration keys remain supported.

Both `HttpEndpoint::handle` and `RouteHandler::handle` return
`Result<ManagedResponse, EndpointError>`. Return intentional HTTP outcomes, including 4xx and 5xx,
as `Ok(ManagedResponse::Unary(response))`. Use `EndpointError::new("Static authored context")` or
`EndpointError::with_source("Static authored context", error)` when a handler cannot produce a
response. `Display` and `Debug` omit the source; `std::error::Error::source` retains it for explicit
diagnostic access. Keep the context free of request values and credentials.

An endpoint error or invalid unary status/header metadata selects a 500 response with
`Content-Type: text/plain` and body `Internal Server Error`. Infra logs the method, registered route and safe context,
then records the selected response once in the existing surface metrics. It does not retry the endpoint
or fail the host. Intentional responses, authentication refusals, 504 timeouts, `Retry-After`, and SSE
error-frame behaviour retain their existing meanings. `HttpEndpoint::is_healthy` has been removed;
readiness belongs to the pipeline state observation.
