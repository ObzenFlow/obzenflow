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
