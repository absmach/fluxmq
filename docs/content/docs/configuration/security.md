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
`server.amqp091.local`; they do not disable authentication on a remote
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
identity here, and each entry grants exactly the targets it names. Remote
clients, devices, and tenants authenticate through `auth.external`.

```yaml
server:
  amqp091:
    local:
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

An **exact** publish target is single-node only. It is appended and synced on
the receiving node and never forwarded, so granting one together with
`cluster.enabled` is a startup error rather than a deployment whose records some
consumers cannot reach. `cluster.enabled` defaults to true, so a principal
holding an exact target needs it set to false explicitly.

A **prefix** publish target names no queue and is an ordinary topic publish,
which the cluster forwards like any other message, so a principal holding only
prefix permissions runs clustered without restriction. The permission decides
this, exactly as it decides how the publication is routed.

The same rule is applied to reloads against the running node rather than the
new file. `auth.local_principals` reloads at runtime while `cluster.enabled` and
local listener changes require a restart. A single edit that disables
clustering or removes the local listener while adding an exact target would
otherwise apply the target immediately while the node stayed clustered and its
listener stayed active. Such a reload is refused; make the changes in separate
steps, restarting in between.

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

What a principal may do here is decided by its [role](#principal-roles), not by
this listener. A `publisher` is refused every consume, `basic.get`, and
`queue.declare`; a `service` is admitted subject to its own subscribe ACL.

Local-principal counters are exposed under
`by_protocol.amqp.local_principals` in `GET /api/v1/stats`. They cover active
connections, authentication success/failure, publish, subscribe and operation
denials, reload success/failure, and forced disconnects. The counters are
bounded and do not use principal names, certificate values, or routing keys as
dimensions.

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

### Principal Roles

A local principal carries a **role**, and the role is what decides its
capability. Roles live on the principal rather than on a listener because
nothing binds a principal to a listener: any principal with a valid certificate
and secret can connect to any local listener, so a capability granted by a port
would be granted to every principal able to reach that port.

| Role | May publish | May consume | May relay an origin identity |
| --- | --- | --- | --- |
| `publisher` (default) | yes, per its ACL | no | no |
| `service` | yes, per its ACL | yes, per its ACL | yes |

`publisher` is the default when `role` is omitted, so an unspecified principal
gets the least privilege. It is the audit-publisher shape: its publications are
its own records, so it may never attribute one to another origin.

`service` additionally runs consumers and may relay the true origin of messages
it did not author. It is what makes the reserved-property namespace useful in
both directions rather than write-only, because a service can read what another
service set.

A role grants no blanket access. Every operation is still authorized against the
principal's own ACL:

```yaml
auth:
  local_principals:
    - name: rules-engine
      certificate_uri_san: spiffe://absmach/magistrala/rules-engine
      role: service
      current_secret_file: /run/secrets/re-current
      permissions:
        publish:
          - routing_key_prefix: "m."
        subscribe:
          - m
```

Declaring `permissions.subscribe` on a `publisher` is rejected at load rather
than silently ignored, so a principal's configuration cannot suggest a
capability it does not have.

The role joins the permissions fingerprint, so demoting a `service` to a
`publisher` revokes its live sessions exactly as narrowing an ACL or rotating a
credential does. A demoted principal does not keep consuming until it happens to
reconnect.

### Listeners

`server.amqp091.local` admits mTLS peers whose verified certificate URI SAN
matches a configured principal. It confers no capability of its own. Configure
more than one local listener only when you want, say, the audit path and the
service path on separate network segments; one is enough otherwise. Each
requires a configured principal. Whether a deployment may run clustered is a
question about the permissions its principals hold, not about the listener: see
the exact-versus-prefix distinction above.

`server.amqp091.internal` and `server.amqp091.service` are deprecated aliases
for `local`. They behave identically to it and to each other, and FluxMQ logs a
warning naming any that are still in use. They exist because capability used to
be a property of the listener; it is now a property of the principal, so the
distinction they drew no longer means anything.

`subscribe` names exact queues; wildcards, duplicates, blank entries, and
surrounding whitespace are rejected at load. A principal declaring no
`subscribe` entry is refused a consumer even with the `service` role, and one
declaring no `publish` entry cannot publish. Unlike exact publish targets,
subscribe targets need no matching `queues` entry: the durability contract those
carry exists because local publishes are acknowledged as crash-durable, which
does not apply to reading.

A subscribe permission grants reads and nothing else. `basic.consume` is
allowed for the queues it names, and `queue.declare` is allowed only in its
passive form, to assert that a queue exists. A non-passive declare creates a
queue and rewrites the type, retention, TTL and durability of one that already
exists, which is a configuration write: services consume queues that were
provisioned for them, so they never need it. `basic.get`, `queue.bind`,
`queue.unbind`, `queue.purge`, and `queue.delete` are refused.

Consumption also uses a non-mutating queue-manager operation: it fails if the
queue does not exist and a stream cursor is refused for a queue not already
configured as a stream. A read grant therefore cannot create a queue or change
its type indirectly through `basic.consume`.

`subscribe` names queues, but a client addresses one through the queue prefix,
so `subscribe: [m]` authorizes `basic.consume` on `$queue/m`. The wire value is
resolved before it is matched, and an address that resolves to no queue —
a bare `m`, or a pub/sub filter — is refused, because no entry grants one.
`queue.declare` follows AMQP queue-name semantics instead: its passive assertion
uses the bare name `m`, which is matched directly against the same ACL entry.

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

The permission that matched decides how the publication is delivered. An exact
target is appended to its protected stream and synced before the publisher
confirm; a prefix match is always routed as an ordinary topic publish and
carries no durability barrier. A prefix grant can never reach a queue, whatever
routing key it covers — not through a `$queue/`-shaped prefix, and not through a
routing key that happens to name a configured stream — because it was authorized
against no `queues` entry. This is a property of the permission, not of the port
the principal connected to, so one `permissions.publish` entry means the same
thing everywhere.

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

The role and both ACLs share one permissions fingerprint, so narrowing any of
them revokes the sessions that authenticated under the wider one, exactly as a
credential rotation does.

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

A local principal with the `service` role may relay a message it did not
author, overriding `external_id` and `protocol` with the true origin by setting
them as message headers. A `publisher` may not: its publications are its own
records, and a relayed origin would make one disagree with the principal that
authenticated. Because the rule follows the role rather than the listener, a
publisher stays a publisher on whichever local listener it connects to.

Untrusted connections on every protocol — including AMQP 0.9.1 remote and AMQP
1.0 — have any supplied value discarded and replaced with their own
authenticated identity.

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
