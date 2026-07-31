---
title: Security
description: External auth, local principals, TLS/mTLS listeners, and rate limiting
---

# Security Configuration

**Last Updated:** 2026-07-29

## External Auth Callout

FluxMQ delegates authentication and authorization to an external service via
gRPC or HTTP callout. When `auth.external.url` is set, client connections on
enabled protocols are
verified against the external service before being accepted.

```yaml
auth:
  external:
    url: "https://auth-service.internal:7016"
    transport: "grpc"     # "grpc" (default) or "http"
    timeout: 5s
```

The former flat keys such as `auth.url` are not accepted. This is an
intentional breaking configuration change; move every external-callout field
under `auth.external` before upgrading.

Authentication requests contain client credentials. Protect the callout hop
with server-authenticated HTTPS or a service mesh that provides mTLS. The
built-in HTTPS client uses the host trust store and has no client-certificate
configuration; deployments that require mTLS must terminate it in a sidecar
or proxy. Plain HTTP is suitable only for a loopback hop already protected by
that mesh, never for cross-host traffic.

### Per-Protocol Auth

By default, all protocols require auth when `auth.external.url` is set. The
`protocols` map lets you selectively enable or disable the external callout per
protocol. Internal AMQP services that need local authentication use
`server.amqp091.internal`; they do not disable authentication on a remote
listener.

```yaml
auth:
  external:
    url: "https://auth-service.internal:7016"
    transport: "grpc"
    timeout: 5s
    protocols:
      mqtt: true
      http: true
      coap: true
      amqp: true
      amqp091: true
```

Valid protocol keys: `mqtt`, `amqp`, `amqp091`, `http`, `coap`.

When the `protocols` map is omitted or empty, all protocols require external
auth. When the map is present, only protocols set to `true` get external auth;
all others allow connections without external authentication. Do not disable
external auth merely to carry internal traffic. Use an internal listener with
a local principal instead.

## Internal AMQP Local Principals

AMQP 0.9.1 can expose a separate mTLS-only listener for tightly scoped service
identities. This listener does not call external auth or blocking hooks and
does not fall back to external auth for unknown identities.

This is a service-to-service path for a fixed set of first-party producers
declared in FluxMQ's own configuration — audit and event streams, internal
telemetry, and similar broker-adjacent pipelines. Principals are static: adding
one is a configuration and secret-provisioning change followed by `SIGHUP`,
never a runtime registration. There is no dynamic, per-tenant, or per-user
identity here, and each entry grants exactly one publish target. Remote
clients, devices, and tenants authenticate through `auth.external`.

```yaml
server:
  amqp091:
    internal:
      addr: ":5683"
      max_connections: 32
      cert_file: "/run/secrets/fluxmq_server_cert"
      key_file: "/run/secrets/fluxmq_server_key"
      ca_file: "/run/secrets/atom_client_ca"
      client_auth: "require"
      min_version: "TLS1.2"

auth:
  local_principals:
    - name: "atom-audit-publisher"
      certificate_uri_san: "spiffe://absmach/atom/audit-publisher"
      current_secret_file: "/run/secrets/atom_audit_secret_current"
      previous_secret_file: "/run/secrets/atom_audit_secret_previous"
      permissions:
        publish:
          - exchange: ""
            routing_key: "atom-audit"
        subscribe: []
```

The internal listener requires a CA-verified certificate URI SAN, SASL
username, and local secret to match one configured principal. Permissions are
exact-match allowlists. Port `5683` must remain on a private network and must
not be published to the host or Internet.

The listener is single-node only. Its publications are durable on the receiving
node and are never forwarded to other nodes, so configuring it together with
`cluster.enabled` is a startup error rather than a deployment whose records
some consumers cannot reach. `cluster.enabled` defaults to true, so it must be
set to false explicitly.

Publish permissions support only the default exchange (`exchange: ""`) and an
exact, non-empty routing key. Other exchanges and wildcard routing keys are
rejected when the configuration is loaded. At publish time the ACL is applied
to the resolved exchange, so a client may address the default exchange as `""`
or as `amq.default`.

A publish target must be a pre-provisioned protected stream on a queue store
that provides real crash durability; the in-memory queue store cannot back one.
Publisher confirms are sent only after the append and its durability barrier
complete. The wait for that barrier is bounded: an fsync cannot be cancelled
once started, so FluxMQ stops waiting after the internal publish timeout and
NACKs rather than leaving the connection stalled. An abandoned append may still
complete, so a NACK does not prove the record was not written. The number of
abandoned appends waiting on one stream is capped; publications beyond that cap
are refused before any storage work starts, and both outcomes close the channel
so a stalled stream cannot accumulate retries.

Local principals are publish-only. Subscription ACLs are not implemented, so
`permissions.subscribe` must remain `[]`; every consume or `Basic.Get`
operation is denied.

Local-principal counters are exposed under
`by_protocol.amqp.local_principals` in `GET /api/v1/stats`. They cover active
connections, authentication success/failure, publish and operation denials,
reload success/failure, and forced disconnects. The counters are bounded and
do not use principal names, certificate values, or routing keys as dimensions.

See [Internal AMQP Local Principals](/deployment/internal-amqp-local-principals)
for the complete production configuration, rotation process, tests, and
rollout contract.

## Reserved Message Properties

Message property names beginning with `_flux.` are reserved for broker-internal
state passed between first-party services. They are not part of the client
API, and no client may set or read one.

The boundary is the listener's trust policy, not the protocol. Only the AMQP
0.9.1 `internal` and `service` listeners are trusted, because they admit solely
mTLS peers whose verified certificate URI SAN matches a principal declared in
FluxMQ's own configuration. Every other connection — MQTT, HTTP, CoAP, AMQP
1.0, and AMQP 0.9.1 on the remote listener — is treated as a tenant or device:

- **Ingress**: reserved properties supplied by an untrusted publisher are
  dropped before routing, so a reserved value can never be forged by a client.
- **Egress**: reserved properties are omitted from deliveries to untrusted
  consumers, so a client cannot observe state another service set.

Speaking AMQP does not by itself confer trust, and an externally authenticated
AMQP client is never treated as a service.

### Service Listener

`server.amqp091.internal` admits publish-only principals: it exists for audit
records, so a principal there may publish to the targets its ACL names and
nothing else, and may never relay an origin identity.

`server.amqp091.service` admits the same kind of mTLS local principal but also
permits the consumer lifecycle, so a first-party service can subscribe and read
the reserved properties another service set. It is what makes the namespace
useful in both directions rather than write-only.

A service listener grants no blanket access. Every operation is still authorized
against the principal's own ACL:

```yaml
auth:
  local_principals:
    - name: rules-engine
      certificate_uri_san: spiffe://absmach/magistrala/rules-engine
      current_secret_file: /run/secrets/re-current
      permissions:
        publish:
          - routing_key_prefix: "m."
        subscribe:
          - m
```

`subscribe` names exact queues; wildcards, duplicates, blank entries, and
surrounding whitespace are rejected at load. A principal declaring no
`subscribe` entry is refused a consumer even on the service listener, and one
declaring no `publish` entry cannot publish. Unlike publish targets, subscribe
targets need no matching `queues` entry: the durability contract that publish
targets carry exists because local publishes are acknowledged as crash-durable,
which does not apply to reading.

#### Publish targets: exact keys and prefixes

A publish permission sets exactly one of `routing_key` or `routing_key_prefix`.

`routing_key` names one exact target. It is what a durable-stream publisher
needs, because the routing key is the queue it appends to, so it must also
appear under `queues`. The audit publisher uses this form.

`routing_key_prefix` grants every routing key beneath it and is checked against
no queue, so it authorizes topic publishing rather than a durable append. It
exists because a service publishes to topics built from its own runtime data —
a tenant identifier, a channel identifier, a subtopic — which cannot be
enumerated in broker configuration. The Rules Engine republishing a rule output
to `m.<domain>.c.<channel>.<subtopic>` is the motivating case.

A prefix is a wildcard by construction and must never be written as one:
`m.#` is rejected, because accepting it would silently grant the literal `#`
as well. Prefix matching is a plain string prefix over the routing key and
applies only to the default exchange, the same restriction every publish
permission carries.

Keep the prefix as narrow as the service's topic namespace allows. It is what
separates one service's reach from another's, so `m.` is a meaningful boundary
while an empty or single-character prefix is not. The same string as an exact
key and as a prefix are different grants and produce different permissions
fingerprints, so narrowing one into the other revokes existing sessions.

Publish and subscribe ACLs share one permissions fingerprint, so narrowing
either revokes the sessions that authenticated under the wider one, exactly as a
credential rotation does.

A service relays messages it did not originate, so unlike a publish-only
principal it may state a message's true `external_id` and `protocol` rather than
having its own identity stamped on them.

The admin API (`server.api`) is exempt because it is an operator plane, not a
client plane: it is unauthenticated and already exposes session inspection and
configuration reload, so a caller that can reach it holds broker-administrator
capability. Its queue append and read operations therefore pass properties
through unchanged. Keep that listener on a private network.

## Origin Identity

Every published message carries `client_id`, `external_id`, and `protocol`
properties describing who published it and over which transport. These are
stamped from the authenticated connection, so a publisher cannot attribute its
message to another principal or claim it arrived over another protocol.

A trusted AMQP 0.9.1 service relaying a message on behalf of an original
publisher may override `external_id` and `protocol` to preserve the true
origin, by setting them as message headers. The same trust rule as reserved
properties applies, with one addition: a **local principal may not** relay an
origin either. Its identity is fixed by configuration and its publications are
audit records, so relaying would make the record disagree with the peer that
actually authenticated. Untrusted connections on every protocol — including
AMQP 0.9.1 remote and AMQP 1.0 — have any supplied value discarded and
replaced with their own authenticated identity.

Ordinary user properties are unaffected and continue to flow in both
directions on every protocol that supports them.

## Blocking Hooks

Blocking hooks are optional synchronous callouts for operations that need an
external allow/deny or normalization decision before FluxMQ continues. Hooks
run after authentication and before final authorization of the effective topic
or filter.

```yaml
hooks:
  url: "https://hooks.internal:7017"
  transport: "grpc"
  timeout: "500ms"
  fail_mode: "deny"
  protocols:
    mqtt: true
    amqp: true
    amqp091: true
    http: true
    coap: false
  events:
    auth_on_register: true
    auth_on_publish: true
    auth_on_subscribe: true
    auth_on_unsubscribe: true
```

See [Blocking Hooks](/architecture/hooks) for request and response details.

## TLS and mTLS

Listeners share TLS fields across `tls` and `mtls` blocks.

```yaml
server:
  tcp:
    tls:
      addr: ":8883"
      cert_file: "/path/server.crt"
      key_file: "/path/server.key"
    mtls:
      addr: ":8884"
      cert_file: "/path/server.crt"
      key_file: "/path/server.key"
      ca_file: "/path/clients-ca.crt"
      client_auth: "require"
```

## Inter-Broker TLS

```yaml
cluster:
  transport:
    tls_enabled: true
    tls_cert_file: "/path/transport.crt"
    tls_key_file: "/path/transport.key"
    tls_ca_file: "/path/transport-ca.crt"
```

## Rate Limiting

```yaml
ratelimit:
  enabled: true
  connection:
    enabled: true
    rate: 50
    burst: 200
  message:
    enabled: true
    rate: 500
    burst: 2000
```

## Learn More

- [Configuration reference](/reference/configuration-reference)
