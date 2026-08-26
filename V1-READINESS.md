# FluxMQ V1.0 Readiness Assessment

**Date:** 2026-08-25
**Assessed version:** `v0.51.0` (`main` @ `b29fbeefdc`), plus the current-tree
public API/error-contract freeze, protocol-independent queue state machine, and
strict v1 message envelope
**Scope:** the original repo-wide audit, plus targeted current-tree revalidation
of the cluster wire, session ownership, DLQ, cluster TLS, experimental
replication paths, the public protobuf/Go/YAML queue contract, and shared queue
command behavior across protocol adapters and storage backends, including
message ownership, persisted envelope decoding, reserved metadata projection,
and protocol metadata/expiry preservation

**Validation:** the final assessed tree passes the split repository-wide short
race suite, `golangci-lint --config .golangci.yaml run` with zero findings, Buf
lint, and the frozen protobuf breaking-change check. The race suite was split
because the combined build exceeded the environment's `/tmp` quota; the
package groups themselves pass.

---

## Verdict

**Not ready to tag 1.0, but the known broker-core correctness blockers are now
materially narrower.** The completed short-term plan closes P0-2, P0-3, P0-7,
P0-10, and P0-11: the cluster wire no longer freezes abandoned RPCs, inflight
metadata survives takeover, session ownership is fenced by CAS and lease loss,
DLQ movement is loss-safe, cluster transports are secure by default, and
experimental replication fails closed.

The tag is still blocked by work that the short-term plan explicitly deferred:

- **P0-1:** authorization callouts are cancellable but still synchronous and
  uncached on every publish. The breaking Go interface change is complete; the
  remaining cache can be additive.
- **P0-8:** CRL/OCSP behavior remains unverified on a trust-critical path.
- **P0-9:** the admin and Connect queue surfaces remain unauthenticated. This is
  outside the active broker-core workstream, but deferral does not make it safe
  to freeze or expose as a supported 1.0 API.
- **Roadmap 1.8:** the protocol/parser and concurrency surfaces listed below
  have not received the required second audit pass.

The public queue API/error model, protocol-independent queue state machine, and
strict v1 message envelope are now complete.

**Corrected 2026-08-26.** This paragraph previously claimed there was "no legacy
envelope decoder or dual payload/message representation". That was not true when
it was written. A JSON envelope encoder existed for Raft operations, and
`queue/types.PublishRequest` restated the envelope's publisher namespace field
for field, so the durable publish path copied every payload twice and the
cluster boundary flattened typed broker metadata into a `map[string]string` that
could not carry most of it. Each hop silently dropped the queue state, its
timestamps and retry count, most of the transfer metadata, and every publisher
field except properties.

It is true now, and of the current tree rather than of the assessed tag:

- One representation. The JSON encoder is gone, `PublishRequest` is deleted, and
  the publish and append commands name `*message.Envelope` directly. They borrow
  it — the queue clones what it stores, and a successful storage append owns
  that clone, never the caller's envelope.
- One wire. Peer RPCs carry a complete binary envelope rather than a payload
  beside a flattened property map, so a delivery crossing a node keeps its
  redelivery count and its TTL.
- One schema of record. `proto/message/v1/envelope.proto` states the stored
  format, and the format is pinned three ways: a conformance pair holding the
  hand-written codec and the schema against each other field for field, a golden
  encoding that fails on any byte change, and its own breaking-change baseline
  beside the public and cluster ones. Until that existed the persisted format
  was defined only by two Go switch statements kept aligned by hand.

The next broker-core work is a recoverable transition boundary, followed by a
capability interface that keeps experimental Raft outside the stable API. See
[`ROADMAP.md`](./ROADMAP.md#next).

This assessment distinguishes **stable-core readiness** from **release
readiness**. Completing the former is necessary and valuable; it is not a claim
that admin security, broad verification, release governance, or operational
qualification are complete.

---

## Coverage and confidence

This audit combined a completed repo-wide inventory pass with direct
verification of every finding quoted here — each file:line was read in place,
not taken on report.

**Audited in depth:** repo inventory, planning/doc state, dependency and
vulnerability posture, `broker/` auth + authorization + callout, config load
and defaults, `cluster/` session ownership and watch handling, cluster
transport security, `logstorage/` sync discipline, DLQ state transitions,
replication admission/failure behavior, MQTT delivery backpressure and QoS 2
queue retry behavior, Connect queue mutations, queue pending/cursor transitions,
and publish-path hooks.

**Audited shallowly or not at all — treat as unknown, not as clean:**

- AMQP 0.9.1 protocol correctness (exchange routing, confirms, tx, prefetch)
- AMQP 1.0 link credit / flow control / partial-delivery reassembly
- MQTT codec DoS surface (attacker-controlled length prefixes)
- MQTT v5 topic-alias and property round-trip correctness
- Queue consumer-group rebalance and retention/compaction races beyond the
  focused ownership, settlement, and DLQ transition tests. Those focused tests
  are narrower than they read: the MQTT settlement tests built their envelopes
  by hand and wrote the broker-owned namespace directly, so none of them crossed
  the ingress boundary that strips reserved properties from client input. Every
  explicit MQTT v5 ack, nack and reject therefore failed on the wire — the
  documented client recipe could not work — and no test noticed, because only
  the implicit PUBACK path was exercised end to end. Fixed 2026-08-26; a
  settlement now travels in the inbound command namespace. Read the rest of this
  list as "untested", not "tested at the seam".
- WebSocket / HTTP / CoAP transport DoS surface, CoAP UDP amplification
- `pkg/tls` CRL/OCSP fail-open behavior (the code is untested, see P0-8)
- `ratelimit/` per-IP map growth and `X-Forwarded-For` handling
- `reload/` atomicity under concurrent load

A second pass over that list is required before the tag.

**Measured since the first pass:** strict-decode breakage across all 10 shipped
config files (P0-5), which surfaced P0-5a. The 2026-08-21 sweep read the admin
API, queue append/DLQ, etcd/Raft transport, AMQP 1.0 accept, readiness, and CI
paths. On 2026-08-24 the short-term-plan paths were revalidated against current
`main`, including their focused tests and protocol fields. Both sweeps were
finding-directed rather than systematic; they do not shorten the remaining
unaudited list above.

The follow-on contract pass inventoried all three public protobuf modules and
the stable Go/YAML surfaces. A checked-in Buf image now enforces additive
protobuf evolution; exact compile-time interface guards cover authentication
and queue-manager implementers. Focused adapter tests pin queue error projection
for Connect, MQTT 5, AMQP 0.9.1, and AMQP 1.0, while append/storage tests pin
exact offsets, atomic buffered batches, and binary key/header preservation.

---

## P0 — Blockers

### P0-1. Authorization callout runs synchronously on every PUBLISH, uncached — partially addressed

`mqtt/broker/v3_handler.go:291`, `mqtt/broker/v5_handler.go:362` call
`CanPublish` per inbound PUBLISH packet. With the HTTP or gRPC callout
authorizer configured, that reaches
`broker/authcallout/http.go:121 authorize()` — a synchronous HTTP POST with
retry and backoff. There is **no authorization decision cache anywhere in
`broker/authcallout/`**.

Per-publish worst case, at defaults (`broker/authcallout/options.go:19-24`):

```
attempt 1   up to 2s   (defaultTimeout)
backoff        100ms   (defaultRetryBackoff)
attempt 2   up to 2s   (defaultRetries = 1)
-------------------------------
            up to 4.1s blocking, on the connection read goroutine
```

Two compounding defects on the same path:

- `broker/authcallout/http.go:130` and `grpc.go:101` pass
  `context.Background()` into `retryWithBackoff`. The cancellation branch at
  `options.go:155` is therefore **dead code** — a client disconnect or broker
  shutdown cannot cancel an in-flight authorize.
- `http.go:146` logs `slog.Info` on **every** authorize call. One log line per
  published message.

**Failure scenario:** 10k devices at 10 msg/s with callout authz configured =
100k HTTP round-trips/s against the auth service. Broker throughput collapses
to the auth service's capacity. When the auth service degrades to 2s
latency, every publisher stalls for 4.1s per message and the broker's
connection goroutines pile up.

**Fix:** bounded TTL+LRU decision cache keyed on `(identity, topic, action)` —
the pattern already exists at `broker/identitycache.go`. Plumb the caller's
`context.Context` through `Authorizer` (this is a breaking interface change,
so it must land **before** 1.0). Drop the per-call `Info` log to `Debug`.

*Note:* the authorize path correctly **fails closed** — `http.go:143` returns
`false` on error. That part is right.

> **Half of this is fixed, 2026-08-21**, #577. Both
> authorization interfaces now take a `context.Context`, sourced from the
> connection that triggered the decision, so a client disconnect or a shutdown
> cancels an in-flight authorize and the cancellation branch at `options.go:155`
> is live code. The breaking interface change is therefore done and off the
> critical path to the tag.
>
> **The scalability defect is untouched.** Authorization is still one
> synchronous callout per PUBLISH with no decision cache: the failure scenario
> above stands exactly as written, and the 4.1s stall is now cancellable rather
> than absent. Roadmap 1.3 is what closes it.

### There are two Raft tracks, and only one is part of the current code

The distinction remains important after resolving P0-2 and P0-3:

**Track A — `queue/raft/`, queue log replication. Real and fully wired.**
hashicorp/raft v1.7.3, 2,771 non-test LOC against 1,506 test LOC. Started from
`cmd/main.go:974` via `StartQueueCoordinator`, configured through
`config.RaftConfig` (`config/config.go:889-911`) with `replication_factor`,
`sync_mode`, `min_in_sync_replicas`, `ack_timeout`, `write_policy`, and
`distribution_mode`. Its transport is now secure and its failure behavior is
fail-closed, but its operational and recovery model is not qualified for the
stable contract. It remains experimental and disabled by default.

**Track B — replacing etcd as the coordination layer. Deleted scaffolding.**
The former stub RPCs in P0-3 were leftovers from a deleted 20-week design document
(`docs/custom-raft-implementation-plan.md`, removed in `f6a31c8c1`) that
proposed replacing etcd with a hashicorp/raft-backed, MQTT-aware coordination
layer. Note that "custom Raft" in that document never meant implementing
consensus by hand — its own summary reads "Use existing libraries
(hashicorp/raft + BadgerDB) to avoid implementing consensus from scratch."
Track B remains a post-1.0 project. Its abandoned RPCs are now gone, so it no
longer consumes compatibility budget in the 1.0 protobuf contract. If revisited,
it must enter through an additive capability boundary rather than redefine
public queue behavior.

### P0-2. Raft replication uses a cleartext, unauthenticated transport — ✅ RESOLVED 2026-08-24

Queue Raft now uses `raft.NewNetworkTransport` over a TLS stream layer and
reuses the broker cluster identity. Peer verification requires the configured
CA and certificate identity; Raft diagnostics are bridged into structured
logging. Generated-certificate tests cover authenticated success and rejection
of an untrusted peer.

This secures an experimental feature; it does not promote queue Raft into the
1.0 compatibility or production-support contract.

### P0-3. Three Raft RPCs in the cluster protobuf contract are hardcoded failures — ✅ RESOLVED 2026-08-24

`AppendEntries`, `RequestVote`, and unary `InstallSnapshot` were removed from
`proto/cluster/v1` before the contract freeze. No production caller used them,
and the live Hashicorp Raft transport does not depend on the cluster Connect
service. Reintroducing a future transport API remains additive.

The same wire revision added `InflightMessage.properties`, preserving user and
broker-owned queue metadata through session export, takeover, restoration,
retry, and settlement.

### P0-4. A missing config file silently starts a default broker

> **FIXED 2026-08-20**, #576. `Load` now returns `ErrConfigNotFound`; the old
> behaviour moved to `LoadOptional`, reachable via a new `--config-optional`
> flag. Reload uses `Load`, so a config file that goes missing under a running
> broker now fails the reload and retains the live configuration instead of
> silently resetting it to defaults.

`config/config.go:1283-1287`:

```go
data, err := os.ReadFile(filename)
if err != nil {
	if os.IsNotExist(err) {
		return Default(), nil
	}
```

`fluxmq --config /etc/fluxmq/typo.yaml` starts a fully-default broker rather
than failing. Combined with `broker/auth.go:109` (`if e.authz == nil { return
true }`) and `auth.go:88` (`if e.auth == nil { return true, "", nil }`), the
default broker is **open**: no authentication, no authorization.

A typo in a systemd unit or Helm chart produces a silently unauthenticated
broker that reports healthy. `docs/content/docs/reference/cli-reference.md:18`
documents this as intentional; it should not survive 1.0. An explicit
`--config-optional` flag is the compatible escape hatch.

### P0-5. Config decoding is not strict

> **FIXED 2026-08-20**, #576. Decoding goes through `yaml.Decoder` with
> `KnownFields(true)`. `config/schema_test.go` pins the top-level and listener
> key sets and asserts every shipped config file decodes strictly.

`config/config.go:1295` uses plain `yaml.Unmarshal`. `grep -rn 'KnownFields'`
returns nothing repo-wide. Only `rejectLegacyAuthKeys` (line 1291) guards
anything, and it covers legacy auth keys only.

A misspelled key under `auth:`, `cluster.transport.tls_*`, or `storage:` is
silently ignored and the broker starts clean with that protection absent.

This is the single highest-leverage fix in the document: `KnownFields(true)`
plus a schema test is a few hours and closes an entire class of
silent-misconfiguration incident.

**Confirmed by measurement, 2026-08-20.** A throwaway spike decoded all 10
shipped config files with `KnownFields(true)`:

```
files broken by strict decode: 4 / 10
distinct unknown keys:         1
  plain                        x8
```

| File | Result |
| --- | --- |
| `examples/config.yaml` | FAIL — `tcp.plain` (line 7), `websocket.plain` (line 17) |
| `examples/no-cluster.yaml` | FAIL — `tcp.plain` (line 7), `websocket.plain` (line 16) |
| `examples/single-node-cluster.yaml` | FAIL — `tcp.plain` (line 7), `websocket.plain` (line 16) |
| `examples/tls-server.yaml` | FAIL — `tcp.plain` (line 6), `websocket.plain` (line 44) |
| `examples/production.yaml` | pass |
| `deployments/cluster/config/node{1,2,3}.yaml` | pass |
| `deployments/docker/config.yaml`, `config-local-principal.yaml` | pass |

Every failure is the same key. `MQTTTCPConfig` and `MQTTWebSocketConfig` have slots `v3`, `v5`, `tls`, `mtls` — the
schema was changed at some point and four example files still say `plain`.

### P0-5a. Five shipped examples silently open listeners they never declared

> **FIXED 2026-08-20**, #576. All five files corrected. `Validate` additionally
> rejects two listeners bound to the same address, which is what caught the
> `production.yaml` collision below. In the same pass the MQTT listener
> sections moved under a `server.mqtt` parent — see the note at the end of this
> finding.

The `plain` key above is not documentation drift — it is a live
misconfiguration, and it is the concrete instance of what P0-5 describes.

`examples/no-cluster.yaml` declares exactly one plaintext TCP listener:

```yaml
server:
  tcp:
    plain:
      addr: ":1883"
      max_connections: 10000
```

The whole block is discarded. What the broker actually opens comes from
`Default()` (`config/config.go:1025-1080`, `:38-39`):

| Listener | Declared | Actually opened |
| --- | --- | --- |
| TCP v3 | `:1883` | `:1883` — coincides, so the bug is invisible |
| TCP v5 | *not declared* | **`:1884`, plaintext** |
| WebSocket v3 | `:8083` | `:8083` — coincides |
| WebSocket v5 | *not declared* | **`:8084`, plaintext** |

So each of the four examples runs **two** plaintext TCP listeners and **two**
plaintext WebSocket listeners where it declared one of each. The coincidence on
`:1883` is why nobody has noticed: the README quickstart appears to work.

`examples/tls-server.yaml` is the worst case — an operator reading a file whose
stated purpose is "demonstrates how to enable TLS" gets two undeclared
plaintext ports alongside the TLS ones. Equally, every `max_connections`,
`read_timeout`, and `write_timeout` inside those `plain` blocks is silently
discarded; they currently match the defaults, so an operator who edits one sees
no effect and no error.

#### The production profile was the worst case

Fixing the four `plain` files unblocked loading `examples/production.yaml` far
enough to validate it — and it failed on a *second* collision, on the same
`:8084` pair. That file is the template an operator copies for a real
deployment, and it states three times that it does not serve plaintext:

- header: "TLS on every public listener; plaintext listeners are commented out"
- `server.tcp`: "Plaintext MQTT is intentionally disabled."
- `server.websocket`: "Plaintext WS disabled by default; expose only WSS publicly."

All three were false. Because the file omitted the `v3`/`v5` slots rather than
emptying them, `Default()` supplied them, and the "hardened" profile opened
**four plaintext ports** — `:1883`, `:1884`, `:8083`, `:8084` — with the last
racing the declared WSS listener for the same socket. Its commented-out
`# plain:` block was doubly dead: uncommenting it would have been an unknown
key.

**Fixed:** `plain` renamed to `v3` in the four examples, with `v5` declared
explicitly so each file states what it runs; `tls-server.yaml` and
`production.yaml` disable the plaintext slots with an explicit `addr: ""`.
Strict decoding plus the new duplicate-bind check make both shapes of this bug
impossible to reintroduce silently.

#### Related schema change, same pass

`server.tcp` and `server.websocket` said nothing about MQTT while sitting
directly beside `server.amqp` and `server.amqp091` — the same ambiguity, one
level up. Both moved under `server.mqtt` (`server.mqtt.tcp.v3`,
`server.mqtt.websocket.v3`), with the Go types renamed to match
(`MQTTTCPConfig`, `MQTTWebSocketConfig`, and a new `MQTTConfig` grouping the two
transports). Breaking for every deployed configuration, and deliberately taken
before the tag freezes the key names. All 10 shipped configs, 7 documentation
pages, and the validation error paths were updated with it.

### P0-6. Queue durability is 1-second async fsync, and not reachable from config — ✅ RESOLVED 2026-08-21

`logstorage/types.go:168` — `DefaultSyncInterval = time.Second`.
`cmd/main.go:834-837` constructs the queue store with
`logStorage.DefaultAdapterConfig()` and never overrides `SyncInterval`.
`grep` for `sync_interval` across `config/`, `examples/`, and `deployments/`
returns **nothing** — the knob does not exist in the config schema.

`logstorage/options.go:112` defines `WithSyncForEveryWrite()` (interval 0).
Nothing wires it.

**Failure scenario:** publisher receives PUBACK for a QoS 1 message. Host loses
power 400ms later. The message was in the page cache, never fsynced. It is
gone, and the publisher was told it was durable. Up to one second of
acknowledged messages per node.

The README markets "Durable Queues", "at-least-once or exactly-once delivery
(QoS 1/2)", and "Persistent message queues". MQTT QoS 1/2 semantics require
the server to take ownership before acknowledging. One-second async fsync is
ownership against process crash only, not host crash.

Additionally: all three cluster reference deployments
(`deployments/cluster/config/node{1,2,3}.yaml:66`) set `sync_writes: false`
for Badger — the shipped cluster example is non-durable.

**Fix:** expose `storage.queue.sync_interval` in config, default it
conservatively, and document the durability guarantee explicitly against each
setting. An operator must be able to choose fsync-per-append.

> **Resolved 2026-08-21**, #578 and #579. `storage.queue_ack_durability` selects the policy,
> `queues[].ack_durability` overrides it per queue, and
> `storage.queue_sync_interval` exposes the window that was hardcoded at one
> second. Startup refuses `fsync` on a store that cannot sync a single append.
>
> **The default stayed `buffered`, deliberately.** Measurement, not preference:
> the barrier costs the device's fsync latency, and at the time it was one fsync
> per message — ~203 msg/s on consumer NVMe against ~130,000 buffered, flat at
> any concurrency, because the fsync was held under the segment lock. Group
> commit (roadmap 1.5b) fixed the scaling — ~175 msg/s at one publisher, ~3,300
> at sixty-four — but ~40x still separates the two, so `fsync` is what a queue
> asks for rather than what every deployment inherits. The failure scenario
> above therefore still describes the default; what changed is that an operator
> can now choose otherwise, per queue, and knows the price.
>
> Two older durability defects surfaced while building the barrier, both fixed
> with it and neither previously known:
>
> - **A rotated segment was never fsynced by the broker at all.**
>   `SegmentManager.Sync` — what the background sync loop calls — only touches
>   the *active* segment, and rotation retired the previous one without syncing
>   it. Its tail waited on OS writeback under every acknowledgement policy.
> - **`Segment.Sync` skips readonly segments**, so a barrier owed to a publisher
>   whose append landed just before a rotation would have been dropped silently.
>
> Still open from this finding's last paragraph: `badger_sync_writes: false` in
> the three cluster reference deployments.

### P0-7. Session ownership can split-brain — ✅ RESOLVED 2026-08-24

Fresh acquisition is a create-if-absent transaction. Reacquisition by the same
node is idempotent; another live owner produces typed `ErrSessionOwned`
information rather than being overwritten. Takeover is serialized per client
and completes with a compare-and-swap from the owner that was actually
observed.

Lease-expiry deletes are no longer resurrected. Lease loss fences and
disconnects every local session associated with that lease before new claims
are accepted. Focused tests exercise simultaneous acquisition, concurrent
takeover, stale observations, lease loss, and reacquisition. etcd remains the
authoritative ownership state; caches cannot grant a session.

### P0-8. The TLS revocation stack has zero tests

`pkg/tls/verifier/crl` (281 LOC), `pkg/tls/verifier/ocsp` (242 LOC), and
`pkg/tls/verifier` (24 LOC) contain **no `*_test.go` files at all**, while
being wired onto the peer-verification path at `pkg/tls/tls.go:253,287` for
every mTLS listener.

547 lines of untested certificate-revocation logic deciding whether to trust a
client certificate. The fail-open/fail-closed behavior when an OCSP responder
is unreachable was not verified in this pass and must be — a fail-open there
is a security finding in its own right.

### P0-9. The admin API and the Connect queue service have no authentication

*Imported from `v1.md` during the 2026-08-21 plan reconciliation; every line
below was re-read against the tree before being recorded here.*

`server/api/server.go:45-72` constructs its `http.ServeMux` and mounts every
route with **no authentication middleware**:

- REST: `/api/v1/reload`, `/api/v1/sessions`, `/api/v1/sessions/`,
  `/api/v1/subscriptions`, `/api/v1/stats`, `/api/v1/cluster`,
  `/api/v1/overview`.
- The full Connect `QueueService` handler (`server/queue/handler.go`):
  `CreateQueue:95`, `DeleteQueue:181`, `UpdateQueue:195`, `Append:235`,
  `AppendBatch:261`, `AppendQueue:300`, `Read:348`, `Tail:385`,
  `SeekToOffset:420`, `CreateConsumerGroup:498`, `DeleteConsumerGroup:557`,
  `JoinGroup:567`, `Ack:712`, `Nack:751`, `Claim:780`.

`deployments/docker/compose.yaml:12` publishes port `8082` to the host, and the
`fluxmq-dashboard` service at `:45` consumes that same unauthenticated API.

Anyone who can open a TCP connection to the admin port can delete every queue
and reload the broker's configuration. This is the single largest gap between
what the broker claims and what it enforces, and — unlike P0-1 — it needs no
particular configuration to be reachable.

### P0-10. DLQ movement and replication both fail open — ✅ RESOLVED 2026-08-24

DLQ transitions now propagate create, append, and sync failures; use the normal
queue durability path; and settle the source only after the destination append
succeeds. Explicit reject uses the same transition. A stable broker-owned
transfer ID derived from source queue, group, and offset makes the remaining
crash-window duplicate detectable. When the DLQ is disabled or unavailable,
the source delivery remains pending.

Experimental replication now validates configuration at startup and on queue
create/update. A replicated write is rejected when its gate, Raft manager,
group, leader, replication factor, or minimum-in-sync requirement is
unavailable. Only explicit `reject` and `forward` policies are allowed; local
fallback is gone. Replicated `fsync` remains rejected because its stronger
acknowledgement contract has not been implemented.

The remaining architectural gap is atomic recovery across source settlement
and destination append. The stable transfer ID makes the current behavior
loss-safe; the transition journal proposed in `ROADMAP.md` would make replay a
first-class model rather than a DLQ-specific recovery convention.

### P0-11. etcd peer and client traffic is plaintext — ✅ RESOLVED 2026-08-24

Embedded-etcd peer and client traffic now uses the same mutual-TLS identity as
the broker cluster transport. The client listener is restricted to loopback.
Cluster configuration fails validation without TLS unless the operator selects
the explicit development-only `cluster.allow_insecure` opt-in. Certificate and
plaintext-policy tests cover both secure and intentionally insecure modes.

---

## P1 — Should fix before 1.0

### P1-1. Event hooks block the MQTT publish hot path

`mqtt/broker/publish.go:117-118`, inline before `b.distribute(ctx, msg)`:

```go
if b.eventHook != nil {
	if err := b.eventHook.OnPublish(ctx, msg.ClientID, msg.Topic, msg.QoS, msg.GetPayload()); err != nil {
```

A slow hook stalls every publish broker-wide. The error is logged rather than
fatal, which is right; the synchronous call is not. `broker/asynchook.go`
exists — route `OnPublish` through it, or document the hook contract as
"must return in microseconds".

### P1-2. AMQP shutdown paths take no context

`amqp/broker/broker.go:426` `func (b *Broker) Close() error` and
`amqp1/broker/broker.go:329` `func (b *Broker) Close()` are both contextless.
Cleanup can block indefinitely and there is no deadline to bound it, so
`shutdown_timeout` cannot be honored. Confirms the long-standing known issue.

Related: `amqp/broker/channel.go:735` calls `qm.Publish(context.Background(), …)`
— an unbounded queue publish from the AMQP 0.9.1 path with no deadline and no
cancellation.

### P1-3. `make bench` runs a stub e2e suite

`benchmarks/e2e_bench_test.go:400-440`:

```go
func startTestBroker(tb testing.TB) *TestServer {
	server := &TestServer{}   // "returning a mock that needs implementation"
	return server
}
func (s *TestServer) Addr() string { return "localhost:1883" }
func (s *TestServer) Stop() {}
```

Plus `benchmarks/e2e_bench_test.go:395`:
`time.Sleep(time.Duration(b.N) * time.Second)`. No build tag guards the
package and `Makefile:139` runs `./...`.

**Any published e2e throughput number sourced from this file is fiction.**
Either implement broker startup or delete the file — do not ship a 1.0 whose
benchmark suite fabricates results.

### P1-4. ~20 sentinel error comparisons use `==` / `!=` instead of `errors.Is`

Against the project's own stated rule. Concentrated in `logstorage/adapter.go`
(lines 224, 331, 334, 351, 367, 370, 500, 554, 594) and
`queue/consumer/manager.go` (116, 129, 174, 302). Also
`cluster/retained.go:254`, `cluster/will.go:217,255`,
`mqtt/broker/session.go:433`, `logstorage/segment.go:460`.

These are latent, not live: they break silently the first time any layer wraps
an error.

**Narrowed 2026-08-24.** ~~The `UpdateCommitted` comparison around
`CommitOffset` was the most likely path to lost offset commits.~~ Persistent
committed offsets no longer call that legacy ack alias at all, which also fixes
the underlying bug where committing a cursor could delete the next pending
record. The touched `queue/consumer` group-lookup comparisons now use
`errors.Is`; the rest of the repository-wide sentinel sweep remains open.

### P1-5. `queue/consumer` is the least-tested and most concurrency-sensitive package

1,789 non-test LOC against 260 test LOC — a **0.15** ratio, in the package that
owns consumer-group membership, heartbeats, work-stealing, and the PEL. For
comparison `mqtt/` sits at 0.94 and `broker/` at 0.96.

**Narrowed 2026-08-24.** The shared state-machine conformance suite now covers
pending ownership, cursor/committed progression, ack, immediate and delayed
nack, loss-safe reject, deterministic pending-only claim, and seek against both
memory and persistent log storage. It found and fixed a persistent cursor update
that could delete the next pending record. The package-local ratio remains low,
and rebalance, work-stealing under partition, and concurrency simulation still
need the table-driven suite before consumer groups leave beta.

### P1-6. Suppressed error handling is concentrated in the riskiest packages

570 `//nolint` directives repo-wide, 496 of them `errcheck`. Non-test
concentration: `mqtt/broker` 70, `client/mqtt` 15, `logstorage` 14,
`amqp1/broker` 11. Separately, 99 `_ =` ignored errors in non-test code, with
`amqp/broker` at 23 — protocol error paths silently dropped.

A targeted sweep of the `mqtt/broker` 70 and the `amqp/broker` 23 is
proportionate; the rest can wait.

**Narrowed 2026-08-24.** Connect append, consume, ack, nack, claim, and seek no
longer suppress storage, PEL, cursor, ownership, or delayed-nack failures; they
delegate to the typed command processor. `queue/consumer` also propagates group
update and explicit claim errors on those paths. The broader MQTT/AMQP ignored-
error audit above remains open.

### P1-7. `CLAUDE.md` documents a configuration system that is not on `main`

> **CORRECTED 2026-08-21.** This finding originally read "a configuration system
> that does not exist". That was wrong. `config/v1.go`, `config/schema_test.go`,
> `version: 1`, the `listeners` key, and `fluxmq config validate` all exist — on
> the unmerged local branch `config` (15 commits, 83 files, forked from main at
> `dbfc4d326` on 2026-08-05). `CLAUDE.md` describes that branch. The trap is
> real and unchanged in effect — a contributor reading it against `main` is sent
> to files that are not there — but it is drift between a branch and `main`, not
> invented documentation. The branch's listener model was subsequently rejected
> in favour of `server.mqtt.tcp`, so the drift now resolves by correcting
> `CLAUDE.md`, not by merging the branch.

The Configuration section describes `version: 1`, `config/v1.go`,
`config/schema_test.go`, a `fluxmq config validate` subcommand, strict
decoding, and top-level `listeners` / `admin` / `health` / `telemetry` /
`experimental` keys. **None of these exist.** Actual top-level keys are
`server, broker, session, log, storage, cluster, webhook, ratelimit,
queue_manager, queues, auth, hooks`.

The Hot Reload section *is* accurate (verified against `config/diff.go:55-110`).

The "Known Issues" list is 60% stale — `RefCountedBuffer` double-free
(`mqtt/refbuffer.go:87-94`), the unbounded identity cache
(`broker/identitycache.go`, now bounded TTL+LRU), and the breaker half-open
state (now `sony/gobreaker`) are all **already fixed**. Still open: AMQP
context timeouts, synchronous event hooks.

This misleads every contributor and every AI-assisted session. It is also the
repo's only known-issue record.

### P1-8. The AMQP 1.0 handshake is unbounded — ✅ RESOLVED 2026-08-21

`server/amqp1/server.go:120-123` runs `tlsConn.Handshake()` with no deadline
set on the connection, and nothing bounds SASL or AMQP `Open` after it. A
client that connects and then stalls holds a connection slot for as long as it
likes.

AMQP 0.9.1 already solved this: `server/amqp/server.go:24-26` documents a
`HandshakeTimeout` covering the transport and AMQP handshake through
`Connection.Open`, and `:155` sets the deadline, clearing it on success. The
fix is to copy that model, not to invent one.

> **Resolved 2026-08-21**, #578. Per-listener
> `handshake_timeout` on both AMQP families, default 10s, covering transport,
> TLS, SASL and OPEN, cleared once the connection is established. AMQP 0.9.1 had
> the same 10s hardcoded in `cmd/main.go` and now reads the key too.
>
> **Worse than reported, and fixed with it:** the TLS handshake ran *inline in
> the accept loop*, so one unresponsive peer stalled every connection waiting to
> be accepted — not just its own slot. It now runs on the connection's own
> goroutine, under the deadline, through `HandshakeContext`.

### P1-9. Images are published before they are scanned

`.github/workflows/build.yml:60` pushes the image, `:71` signs it, and `:81`
runs the Trivy image scan — so a Critical finding is discovered after the
artifact is public. The workflow also triggers on tags independently, with no
dependency on `ci.yml`, so a tag whose tests fail still publishes. CI itself
runs only `make test` (`ci.yml:57`) — the short race suite — with no
integration, stress, interoperability, or cluster-failure coverage. Actions are
pinned by mutable tags (`aquasecurity/trivy-action@v0.36.0`,
`sigstore/cosign-installer@v3`) rather than digests.

### P1-10. `AsyncEventHook.Close` races enqueue; remote response bodies are unbounded

`broker/asynchook.go` checks `h.closed.Load()` at `:127` and `:139` and then
sends on `h.queue`, while `Close` executes `close(h.queue)` at `:191`. A send
that passes the check just before the close panics on a closed channel — a
shutdown-under-load crash on any broker with hooks configured. The
`atomic.Bool` CAS at `:187` makes `Close` idempotent; it does not close this
window.

Separately, neither `broker/authcallout/http.go` nor the webhook client bounds
the response body it reads — no `io.LimitReader` or `http.MaxBytesReader`
appears on either path.

---

## P2 — Process and documentation

### P2-1. Release planning exists; architecture decisions and release policy remain informal

`ROADMAP.md` and this readiness assessment now provide a tracked 1.0 scope and
completion record, so the original "no committed plan" finding is resolved.
The focused `short-term-plan.md` is a working document rather than the durable
release plan; its completed outcomes have been reconciled into these files.

The remaining gap is narrower: there is no durable ADR series for architectural
decisions and no `SECURITY.md`, `CHANGELOG.md`, or `CODEOWNERS`. Those are not
part of the active broker-core track, but they remain release-readiness work.

### P2-2. README contradicts itself on Raft and DLQ

`README.md:29` — `- ✅ Optional Raft layer for queue appends (WIP)` — marks the
same item done and unfinished on one line instead of stating the actual support
boundary: experimental, secure, fail-closed, disabled by default, and outside
the 1.0 contract. `README.md:96` still says "DLQ handler present (delivery path
wiring pending)", which is now false after the loss-safe delivery and reject
paths landed.

### P2-3. Six files exceed 2,000 lines

`queue/manager.go` 2,467 · `cluster/etcd.go` 2,379 ·
`amqp/broker/channel.go` 2,337 · `config/config.go` 2,176 ·
`amqp/codec/methods.go` 2,081 · `cmd/main.go` 1,508.

Against a stated 40–60 line function target. Not a 1.0 blocker, but these are
where review attention goes to die, and three of them
(`queue/manager.go`, `cluster/etcd.go`, `amqp/broker/channel.go`) hold the
logic most likely to contain the bugs this audit could not reach.

---

## What is genuinely solid

Stated plainly, because the P0 list above is not a verdict on the whole
codebase:

- **`go vet ./...` clean. `govulncheck ./...` clean** — zero called
  vulnerabilities. One unreachable transitive advisory (`GO-2026-5932`,
  `x/crypto/openpgp`).
- **Dependency posture is healthy.** 30 direct deps, only 3 trivially behind.
  No abandoned direct dependencies. Dependabot active.
- **MQTT delivery backpressure is correctly designed.** `mqtt/broker/delivery.go`
  implements a receive-maximum send window, a bounded pending queue, and an
  explicit drop on overflow (`delivery.go:134`). This is the failure mode that
  OOMs most brokers, and it is handled.
- **Authorization fails closed** on callout error (`authcallout/http.go:143`).
- **Public contract drift is mechanically guarded.** CI compares the current
  protobufs to `api/compat/proto-v1.binpb`; exact Go-interface and YAML-schema
  tests fail on unreviewed shape changes. The exact `queue.CommandProcessor`
  method set is guarded too, while its concrete state machine remains private.
  Queue clients receive typed failures instead of needing to parse
  implementation error strings.
- **The message model now has one strict ownership boundary.** Version 1 is the
  only accepted persisted envelope schema. User metadata cannot occupy
  broker-owned source, delivery, queue/stream, transfer, or trace namespaces;
  protocol adapters use explicit public or trusted projections. Clones share
  one immutable reference-counted payload while deep-copying mutable metadata,
  and storage/session/queue interfaces state who owns each reference.
- **Session ownership now has an explicit fencing model.** etcd transactions
  arbitrate acquisition and takeover; lease loss disconnects local sessions;
  caches are observational rather than authoritative.
- **`logstorage/` has real durability machinery** — segment fsync, directory
  fsync with correct `ErrUnsupported` handling (`segment.go:183-193`),
  recovery, time index, and PEL. A queue can choose `buffered` or `fsync`, and
  concurrent fsync acknowledgements share a durability barrier.
- **Test ratios are strong where it counts**: `broker/` 0.96, `mqtt/` 0.94,
  `server/` 0.92, `storage/` 1.01, `reload/` 1.63.
- **Three previously-known issues are already fixed** (refbuffer double-free,
  unbounded identity cache, breaker half-open).

---

## Recommended sequencing

The active sequence is architecture-first and API-stability-first:

1. ~~**Freeze the queue protobuf, exported Go, YAML, error, and
   protocol-semantic contracts; enforce an additive compatibility baseline.**~~
   **Done.**
2. ~~**Move append/consume/settlement operations behind one typed queue state
   machine and a shared cross-protocol conformance suite.**~~ **Done
   2026-08-24.**
3. ~~**Introduce a versioned message envelope with separate user and
   broker-owned metadata namespaces.**~~ **Done 2026-08-25.** The implementation
   is strict v1 only, with no backward decoder, aliases, or parallel message
   representation.
4. **Next:** add a recoverable transition journal/outbox for source settlement plus
   destination append.
5. Put experimental replication behind a capability contract, then optimize
   only measured stable paths.

In parallel, the core-relevant existing blockers are authorization decision
caching (P0-1), TLS revocation verification (P0-8), and the second protocol
audit. Admin authentication (P0-9) is deliberately deferred from this
workstream but still blocks the 1.0 tag. Detailed acceptance criteria and the
performance guardrail are in [`ROADMAP.md`](./ROADMAP.md#next).
