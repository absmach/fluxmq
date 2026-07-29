---
title: Internal AMQP Local Principals
description: Implemented production design for a dedicated mTLS AMQP 0.9.1 listener
---

# Internal AMQP Local Principals

**Status:** implemented
**Last Updated:** 2026-07-29

This document describes the implementation and acceptance contract for publishing
Atom audit events to FluxMQ without sending those publications through Atom's
own external authorization callout.

## What this feature is for

Local principals are a service-to-service ingress for a small, fixed set of
first-party producers that are part of the deployment itself — audit and event
streams, internal telemetry, and similar broker-adjacent pipelines. Every
identity, its certificate URI SAN, and its exact publish target are declared in
FluxMQ's own configuration and mounted secrets, so the set of local principals
is known before the process starts and changes only through a deliberate
configuration change plus `SIGHUP`.

It is deliberately not a general client authentication mechanism:

- there is no registration, discovery, or self-service enrollment;
- there is no per-tenant, per-user, or dynamic identity — adding a principal is
  a configuration and secret-provisioning change, not an API call;
- the ACL grants exactly one `(default exchange, routing key)` pair per entry,
  with no wildcards, patterns, or hierarchy;
- the principal is publish-only into a pre-provisioned protected stream, so it
  cannot consume, discover topology, or create anything;
- it scales to a handful of principals, not to a client population. Remote
  clients, devices, and tenants continue to authenticate through
  `auth.external`.

Use it when a service that FluxMQ's own authorization path depends on has to
publish into FluxMQ — the case where routing that publication through the
external callout would add remote-auth latency to every event or create a
feedback loop. For everything else, use `auth.external`.

## Goal

Run two independent AMQP 0.9.1 authentication paths in one FluxMQ process:

```text
remote clients -> remote AMQP listener -> auth.external -> shared queues/storage
Atom publisher -> internal AMQP :5683 -> auth.local_principals -> shared queues/storage
```

The remote listener never reads the local-principal store. The internal
listener never calls external authentication, authorization, or blocking
hooks. Unknown local identities fail closed; there is no fallback between the
two paths.

Both listeners share the queue manager and storage. Separate listeners isolate
authentication latency and the trust boundary, but do not isolate CPU, memory,
or disk capacity. Production sizing must account for the combined workload.

The dedicated listener protects only AMQP ingress. FluxMQ's native admin API is
a separate privileged control plane: it is unauthenticated and exposes queue
operations over the same manager and storage. Keep it disabled or reachable
only from a trusted container or management network. Any access outside that
boundary must go through an authenticated mTLS reverse proxy or equivalent
control. The local-principal Compose overlay leaves `fluxmq:8082` available to
the dashboard on the default container network but does not publish port
`8082` to the host.

## Breaking configuration

The old flat auth form is removed. FluxMQ does not provide a compatibility
period or silently translate it.

```yaml
# Removed
auth:
  url: "https://auth.internal:9090"
  transport: "grpc"
```

All external-callout settings move below `auth.external`. Local identities are
declared separately in `auth.local_principals`:

```yaml
server:
  amqp091:
    # Existing public/remote listener. It uses auth.external and blocking hooks.
    tls:
      addr: ":5681"
      max_connections: 10000
      cert_file: "/run/secrets/fluxmq_server_cert"
      key_file: "/run/secrets/fluxmq_server_key"
      min_version: "TLS1.2"

    # Private listener. Never expose it through a public load balancer.
    internal:
      addr: ":5683"
      max_connections: 32
      cert_file: "/run/secrets/fluxmq_server_cert"
      key_file: "/run/secrets/fluxmq_server_key"
      ca_file: "/run/secrets/atom_client_ca"
      client_auth: "require"
      min_version: "TLS1.2"

auth:
  external:
    url: "https://auth.internal:9090"
    transport: "grpc"
    timeout: "2s"
    protocols:
      mqtt: true
      amqp: true
      amqp091: true
      http: true
      coap: true
    identity_cache_size: 50000
    identity_cache_ttl: "1h"

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

Local-principal secrets are mounted files, never inline YAML. Each file must
contain a high-entropy printable value of at least 32 bytes/characters, for
example 32 random bytes encoded as hex or base64. Do not use raw binary: the
value must not contain embedded CR/LF or NUL characters. FluxMQ strips one
terminal newline, stores only a digest in memory, and uses a constant-time
comparison. Principal names and certificate URI SANs must be unique. Publish
ACLs support only the default exchange (`exchange: ""`) and exact, non-empty
routing keys; other exchanges and wildcards are invalid.

The remote listener requires the separately deployed external auth service;
FluxMQ does not bundle `fluxmq-auth`. Because authentication calls carry client
credentials, use a trusted HTTPS endpoint or a service mesh that provides
mTLS. The built-in HTTPS client uses the system trust store and does not expose
client-certificate settings; terminate mTLS in a sidecar when it is required.
An external-auth outage fails the remote path closed but does not invoke or
interrupt the internal local-principal path.

Authentication on the internal listener requires all of the following to
match the same principal:

- the SASL username;
- the SASL secret;
- the URI SAN in a client certificate verified by the configured CA.

## Atom audit stream policy

Provision the stream before starting the publisher:

```yaml
queues:
  - name: "atom-audit"
    topics:
      - "$queue/atom-audit/#"
    reserved: true
    type: "stream"
    retention:
      max_age: "720h"
      max_length_bytes: 10737418240
      max_length_messages: 0
    limits:
      max_message_size: 1048576
      message_ttl: "720h"
```

`retention.max_age` controls stream retention, while
`limits.message_ttl` controls when each message expires. Keep the TTL at least
as long as the intended replay window. This stream sets both to `720h` so the
default seven-day message TTL cannot shorten the advertised 30-day replay
availability.

At startup and before activating a local-principal snapshot on `SIGHUP`,
FluxMQ validates every local publish ACL target against both the configured and
persisted queue definition. Startup aborts, or reload is rejected while the
previous snapshot remains active, if a target is missing, the queue store
lacks durable-sync support, or the persisted target is not the configured
reserved, durable, non-replicated stream with matching retention and message
limits. FluxMQ does not silently overwrite stale persisted queue metadata;
stop FluxMQ, reconcile it with the YAML queue definition through a controlled
storage migration or restore, and then restart before retrying the reload.

While FluxMQ is running, the queue manager protects only the streams named by
local-principal publish ACLs. It rejects create/update/delete operations that
would change a protected stream's topics, type, durability, reserved flag,
replication mode, retention window, retention byte/message limits, maximum
message size, or message TTL. AMQP `QueueDeclare` attempts that would change
the contract close the channel with `406 Precondition Failed`; the admin API
returns `failed_precondition`. Other queues, including unrelated reserved
queues, keep their existing mutation behavior.

The exact durable publish path re-reads and compares the persisted contract
immediately before every append, so out-of-band storage drift fails closed and
cannot receive a publisher ACK. `MaxDepth` is intentionally not part of this
contract because stream depth is not currently enforced; the effective
retention byte/message limits are protected instead.

Local durable confirms currently support single-node, non-replicated streams
only. Enabling replication on a local publish target fails startup. Supporting
a clustered audit stream requires a future end-to-end quorum durability
barrier before FluxMQ can ACK the publication.

The internal listener therefore cannot run on a clustered node, and FluxMQ
refuses that configuration at startup: `server.amqp091.internal.addr` together
with `cluster.enabled` is a validation error. A local publication is appended
and synced on the receiving node only and is never forwarded to other nodes,
because forwarding would acknowledge a publisher on a barrier no single node
established. In a cluster those records would be unreachable from consumers
attached to any other node, with nothing to signal it, so the combination fails
closed instead. Note that `cluster.enabled` defaults to true, so a deployment
enabling the internal listener must set it to false explicitly, as the shipped
`config-local-principal.yaml` does.

The `atom-audit-publisher` may open connections and channels, enable publisher
confirms, and publish only to the default exchange with the exact routing key
`atom-audit`. It cannot consume, get, declare or modify queues or exchanges,
bind topology, purge, delete, or use transactions. A denied channel operation
returns AMQP `403 Access Refused`.

The ACL is evaluated against the exchange the router resolves, so a client may
name the default exchange either as `""` or as its `amq.default` alias. The
configuration itself accepts only `exchange: ""`.

Local principals are publish-only. `permissions.subscribe` is unsupported and
must remain empty (`[]`).

FluxMQ resolves the preconfigured stream through the shared queue manager; the
publisher never needs `QueueDeclare` permission. A publisher confirm is an ACK
only after the exact configured stream append succeeds and that queue's
durable storage sync completes. The append and durability barrier are one
storage operation: segment rotation is serialized until the segment containing
that record is synced, and a newly created segment's directory entry is synced
when the segment is created, so the record cannot be acknowledged while its
file is still only in the page cache. FluxMQ sends a NACK when the append or
sync fails.

Durability covers the whole queue, not only the record. A queue's log directory
and its ancestors are synced as they are created, and queue metadata is written
to a synced temporary file, renamed, and followed by a directory sync. Without
that, a crash could leave an acknowledged record inside a directory or behind a
metadata file that never reached disk. If a crash still lands between the log
directory and its metadata, recreating the queue from the same configuration
repairs the metadata rather than reporting the queue as already existing, so
acknowledged records stay reachable.

Only a queue store that provides a real crash-durability barrier may back a
protected stream. FluxMQ checks this capability, not just the presence of an
atomic append: startup aborts, and a reload is rejected, when the configured
store cannot make a single append survive a machine crash. An in-memory queue
store therefore cannot serve a local publish target.

The confirm a publisher waits for is bounded, but the barrier underneath it is
not interruptible. An fsync that has already started cannot be cancelled, so
FluxMQ enforces the deadline by giving up on the wait: when the append and sync
do not complete within the internal publish timeout, or the process is shutting
down, the publication is NACKed and the connection stays responsive instead of
a stalled disk holding an internal-listener slot open. The abandoned append can
still complete afterwards and become visible to consumers. A NACK therefore
never proves the record was not written, and the at-least-once retry and
deduplication rules below apply to it.

An abandoned append keeps running and keeps its message, so FluxMQ also caps
how many may be waiting on one stream at a time. Publications beyond that cap
are refused before any storage work starts, which stops a publisher retrying
against unresponsive storage from accumulating barriers and payloads. Both
outcomes close the channel after the NACK: a publisher must reconnect rather
than retry into a stream whose storage has not recovered. The
`publish_timeouts` and `publish_rejections` counters report how often each
happened; sustained growth means storage, not the publisher, is the fault.

Delivery is at least once. Atom may retry after a NACK or an ambiguous
disconnect, so the same event can be appended more than once. Atom must keep a
stable event ID across retries, and every consumer must deduplicate by that
event ID.

## Rotation and revocation

Local-principal definitions and secret-file contents reload on `SIGHUP`. A
reload validates a complete replacement snapshot before atomically activating
it. On any read or validation error, the old snapshot remains active.

Use the optional previous secret for a no-downtime rotation:

1. Set the new secret as `current_secret_file` and the old one as
   `previous_secret_file`, then send `SIGHUP`.
2. Reconnect Atom with the new secret and verify a confirmed publication.
3. Remove `previous_secret_file` and send `SIGHUP` again.
4. FluxMQ disconnects sessions authenticated with the removed secret.

Removing a principal or certificate URI also disconnects its sessions. ACL
reductions apply immediately. Listener addresses and TLS settings require a
process restart.

## Implemented behavior

- Bind a dedicated `server.amqp091.internal` mTLS listener on port `5683`.
- Pass listener-scoped authentication, authorization, and hook policies into
  AMQP connections instead of selecting them from broker-global state.
- Keep remote AMQP on `auth.external`; do not add a local lookup to that path.
- Add a reloadable, immutable local-principal snapshot with active-connection
  revocation.
- Enforce an internal-listener operation allowlist, including AMQP topology
  methods that are outside publish/subscribe authorization today.
- Resolve statically configured streams through the shared queue manager.
- Enforce listener connection limits, TLS handshake timeouts, and message-size
  limits before allocating a message body.
- Add bounded admin statistics and structured logs for local authentication, denials,
  reloads, and active connections. Never log secrets, hashes, or certificates.
- Include successful internal-listener initialization and binding in
  readiness.

The bounded counters are returned by `GET /api/v1/stats` under
`by_protocol.amqp.local_principals`; they are not per-principal metric labels.

## Test and rollout contract

Unit and component tests plus the real-process smoke test prove:

- remote AMQP calls only `auth.external` and never checks local identities;
- internal AMQP never calls external auth or blocking hooks, including when
  those services are unavailable;
- valid mTLS, URI SAN, username, and current/previous secret combinations work;
- invalid CA, SAN, username, or secret combinations fail closed;
- only the exact `atom-audit` publication succeeds;
- consumption and every topology-changing operation are denied;
- secret reload is atomic and removed credentials disconnect active sessions;
- the stream accepts publication without a queue declaration;
- publisher confirms ACK only after stream append and per-queue durable sync,
  and NACK append/sync failures;
- protected stream redeclare/update/delete attempts fail explicitly, and
  publish-time checks reject out-of-band topic/retention/TTL/message-limit drift;
- an audit event is replayable after a graceful FluxMQ restart.

The implemented acceptance command is:

```bash
make smoke-local-auth
```

It starts a real FluxMQ process, creates test PKI, exercises both listener
paths and negative credentials/ACLs, reloads credentials, restarts the broker,
and verifies stream replay. This smoke test passed on 2026-07-29.

### Manual rabtap deployment smoke

Run this smoke after deploying the Compose overlay, and before enabling the
Atom audit publisher. It complements `make smoke-local-auth` by proving that an
independent AMQP 0.9.1 client can publish, receive a durable confirmation, and
replay the event through the externally authorized listener.

The examples below were verified with `rabtap` v1.44.1. Install that exact
version when it is not already available:

```bash
go install github.com/jandelgado/rabtap@v1.44.1
```

Before running the commands, verify all of the following:

- FluxMQ is built from the checkout under test and started with
  `compose.local-principal.yaml` and `config-local-principal.yaml`.
- The FluxMQ server certificate is valid for the hostname used in the AMQP
  URI, normally `fluxmq` inside the Compose network.
- The Atom client certificate is signed by `ATOM_CLIENT_CA_FILE`, has the
  client-auth extended key usage, and contains the URI SAN
  `spiffe://absmach/atom/audit-publisher`.
- The local secret is printable, at least 32 characters, and URI-safe. Hex is
  recommended. Use a short-lived smoke credential because AMQP URI credentials
  can be visible to local process inspection and may appear in client errors.
- A remote reader accepted by `auth.external` may subscribe to `atom-audit`.
  The internal principal is deliberately unable to perform the replay.

The CA passed to `rabtap --tls-ca-file` verifies the FluxMQ **server**
certificate. It is the opposite trust direction from `ATOM_CLIENT_CA_FILE`,
which FluxMQ uses to verify the Atom client certificate. They may be the same
CA in a test deployment, but production does not require that.

Port `5683` is not published to the host. Run the publish commands from Atom or
a short-lived diagnostics container attached only to the private
`atom-internal` network. A temporary `127.0.0.1` port mapping is acceptable for
a developer smoke, but must never be carried into the production overlay.

Export a unique event ID and read the mounted secret without its terminal
newline:

```bash
export AUDIT_EVENT_ID="rabtap-smoke-1"
export AUDIT_SECRET="$(tr -d '\r\n' </secure/atom/audit-secret-current)"
export INTERNAL_AMQP_URI="amqps://atom-audit-publisher:${AUDIT_SECRET}@fluxmq:5683/"
```

Publish through the default exchange to the one permitted routing key:

```bash
printf '{"id":"%s","action":"entity.create"}\n' "${AUDIT_EVENT_ID}" |
  rabtap pub \
    --uri="${INTERNAL_AMQP_URI}" \
    --exchange='' \
    --routingkey='atom-audit' \
    --confirms \
    --mandatory \
    --property='ContentType=application/json' \
    --property='DeliveryMode=persistent' \
    --property="MessageId=${AUDIT_EVENT_ID}" \
    --tls-ca-file=/secure/fluxmq/server-ca.crt \
    --tls-cert-file=/secure/atom/audit-publisher.crt \
    --tls-key-file=/secure/atom/audit-publisher.key
```

The command must exit with status zero. `--confirms` waits for FluxMQ's broker
ACK, which this listener sends only after the exact stream append and durable
sync succeed.

`--mandatory` is kept for parity with the remote listener but does not add a
check here: the internal listener routes only to its protected stream, so an
unroutable or rejected publication is reported as a NACK rather than a
`Basic.Return`. Treat the confirm result, not a returned message, as the
authoritative outcome.

Prove that the local ACL remains exact and publish-only. Both commands must
exit non-zero with AMQP `403 Access Refused`:

```bash
printf 'denied\n' |
  rabtap pub \
    --uri="${INTERNAL_AMQP_URI}" \
    --exchange='' \
    --routingkey='not-atom-audit' \
    --confirms \
    --tls-ca-file=/secure/fluxmq/server-ca.crt \
    --tls-cert-file=/secure/atom/audit-publisher.crt \
    --tls-key-file=/secure/atom/audit-publisher.key

rabtap sub atom-audit \
  --uri="${INTERNAL_AMQP_URI}" \
  --limit=1 \
  --idle-timeout=3s \
  --tls-ca-file=/secure/fluxmq/server-ca.crt \
  --tls-cert-file=/secure/atom/audit-publisher.crt \
  --tls-key-file=/secure/atom/audit-publisher.key
```

An optional topology guard check must also fail with AMQP `403`:

```bash
rabtap queue create forbidden \
  --uri="${INTERNAL_AMQP_URI}" \
  --tls-ca-file=/secure/fluxmq/server-ca.crt \
  --tls-cert-file=/secure/atom/audit-publisher.crt \
  --tls-key-file=/secure/atom/audit-publisher.key
```

Restart FluxMQ gracefully, wait for readiness, then replay through remote AMQP
port `5682`. Use a unique consumer group so a previous cursor cannot hide the
test record:

```bash
docker compose \
  -f deployments/docker/compose.yaml \
  -f deployments/docker/compose.local-principal.yaml \
  restart fluxmq

curl --fail http://127.0.0.1:8081/health

rabtap sub atom-audit \
  --uri='amqps://remote-reader:REMOTE_SECRET@fluxmq:5682/' \
  --offset=first \
  --args='x-consumer-group=rabtap-smoke-unique' \
  --limit=1 \
  --idle-timeout=15s \
  --format=raw \
  --tls-ca-file=/secure/fluxmq/server-ca.crt
```

The output must contain the exact `AUDIT_EVENT_ID` published before the
restart. The external auth service must record authentication and subscribe
authorization for the remote reader. It must not record any request for the
internal publication.

Finally, make `auth.external` unavailable and repeat the confirmed internal
publication. Internal port `5683` must still succeed, while remote port `5682`
must fail closed. Restore the external service after the check. This proves
that listener isolation is behavioral and not merely a configuration split.

For a review bot or release operator, the minimum evidence to retain is:

- the successful `make smoke-local-auth` output;
- the zero exit status of the confirmed exact-target `rabtap pub`;
- non-zero exit statuses for the wrong target, internal subscription, and
  topology mutation;
- the replayed event body after restart;
- external-auth logs showing remote authentication/authorization and no
  internal-listener callout;
- `GET /api/v1/stats` counters showing one accepted message plus the expected
  `publish_denied` and `operation_denied` increments.

Roll out in this order:

1. Deploy the breaking nested auth configuration with the internal listener
   disabled and verify the remote path.
2. Mount the CA, server certificate, current local secret, and pre-provision
   `atom-audit`.
3. Enable port `5683` only on a private network shared with Atom.
4. Run `make smoke-local-auth` against the release source.
5. As deployment-specific rollout gates, validate the rendered Compose or
   orchestrator configuration, start the deployed container, and verify its
   health/readiness and both listener paths in that environment.
6. As a deployment-specific performance gate, load test both paths and set an
   acceptable remote-auth latency budget for that environment. FluxMQ must not
   add a local-principal lookup or callout to the remote path.
7. Enable the Atom audit publisher in a separate deployment change.

The feature is implemented and covered by the real-process acceptance smoke.
A particular production deployment is ready only after its container startup,
network-policy, certificate, external-auth transport, and performance rollout
gates pass.
