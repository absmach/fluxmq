# FluxMQ V1.0 Readiness Assessment

**Date:** 2026-08-20
**Audited version:** `v0.51.0` (`main` @ `84e9bf8a5`)
**Scope:** 162,792 Go LOC, 539 files, 81 packages (~85,700 non-test, non-generated)

---

## Verdict

**Not ready to tag 1.0.** The engineering quality is high — `go vet` clean,
`govulncheck` clean, healthy dependency posture, mature backpressure and etcd
watch handling. The blockers are not sloppiness; they are **unfinished seams
that a 1.0 tag would freeze**: an authorization path that cannot carry
production load, a replication transport with no transport security, stub RPCs
inside a protobuf contract about to be declared stable, and a durability
default that is not reachable from configuration.

Eleven P0 items below — four now resolved, see the next paragraph — plus P0-5a — a live misconfiguration in four shipped
example files, found by a sizing spike run against P0-5 on 2026-08-20. Six of
them are days of work. Two (auth caching, the audit backlog) are the real
schedule risk.

**Resolved so far**, all merged 2026-08-21: P0-4, P0-5 and P0-5a (#576, strict
config decode, missing file is an error), P0-6 (#578 and #579, queue
acknowledgement durability, configurable per queue, with barriers that share an
fsync), P1-8 (#578, AMQP 1.0 handshake bound). P0-1 is half done
(#577): the interface carries a context, the decision cache does not exist.

**P0-9 — an admin API with no authentication at all — is untouched, and is the
most serious finding in this document.**

**Found by shipping, not by this audit.** Three defects surfaced while building
the fixes above, none of them visible to the pass that produced this document:
MQTT 3.1.1 could subscribe to a classic queue and never settle a message, so its
work redelivered five times and went to the dead-letter queue (#580); a rotated
queue segment was never fsynced by anything, because the periodic sync only
visits the active segment (#578); and the AMQP 1.0 TLS handshake ran inline in
the accept loop, where one unresponsive peer stalled every pending connection
(#578). That ratio is the argument for starting 1.8 early rather than treating
it as a final gate.

P0-9 through P0-11 and P1-8 through P1-10 arrived on 2026-08-21, when the
second v1 plan (`v1.md`) was reconciled into `ROADMAP.md`; each was re-verified
against the tree before being recorded.

Two defects were found by *building the fixes* rather than by review, and both
are recorded under P0-6: a rotated queue segment was never fsynced by the broker
at all, and the AMQP 1.0 TLS handshake ran inline in the accept loop where one
unresponsive peer stalled every pending connection (P1-8). Neither was visible
from the audit pass that produced this document — worth remembering when
weighing 1.8, the second audit pass, against shipping the fixes it would gate.

---

## Coverage and confidence

This audit combined a completed repo-wide inventory pass with direct
verification of every finding quoted here — each file:line was read in place,
not taken on report.

**Audited in depth:** repo inventory, planning/doc state, dependency and
vulnerability posture, `broker/` auth + authorization + callout, config load
and defaults, `cluster/` session ownership and watch handling, `logstorage/`
sync discipline, MQTT delivery backpressure, publish-path hooks.

**Audited shallowly or not at all — treat as unknown, not as clean:**

- AMQP 0.9.1 protocol correctness (exchange routing, confirms, tx, prefetch)
- AMQP 1.0 link credit / flow control / partial-delivery reassembly
- MQTT codec DoS surface (attacker-controlled length prefixes)
- MQTT v5 topic-alias and property round-trip correctness
- Queue consumer-group rebalance, retention/compaction races, DLQ wiring
- WebSocket / HTTP / CoAP transport DoS surface, CoAP UDP amplification
- `pkg/tls` CRL/OCSP fail-open behavior (the code is untested, see P0-8)
- `ratelimit/` per-IP map growth and `X-Forwarded-For` handling
- `reload/` atomicity under concurrent load

A second pass over that list is required before the tag.

**Measured since the first pass:** strict-decode breakage across all 10 shipped
config files (P0-5), which surfaced P0-5a. On 2026-08-21 a targeted
verification sweep read the admin API surface, the queue append and DLQ paths,
the etcd and Raft transports, the AMQP 1.0 accept loop, the readiness handler,
and both CI workflows — the source of P0-9 through P0-11 and P1-8 through
P1-10. That sweep was finding-directed, not a systematic pass; it does not
shorten the unaudited list above.

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

### There are two Raft tracks, and only one of them is real

Worth separating before P0-2 and P0-3, because they are not the same project:

**Track A — `queue/raft/`, queue log replication. Real and fully wired.**
hashicorp/raft v1.7.3, 2,771 non-test LOC against 1,506 test LOC. Started from
`cmd/main.go:974` via `StartQueueCoordinator`, configured through
`config.RaftConfig` (`config/config.go:889-911`) with `replication_factor`,
`sync_mode`, `min_in_sync_replicas`, `ack_timeout`, `write_policy`, and
`distribution_mode`. This is considerably more finished than the README's
"(WIP)" suggests. Its defect is the transport — P0-2.

**Track B — replacing etcd as the coordination layer. Abandoned scaffolding.**
The stub RPCs in P0-3 are leftovers from a deleted 20-week design document
(`docs/custom-raft-implementation-plan.md`, removed in `f6a31c8c1`) that
proposed replacing etcd with a hashicorp/raft-backed, MQTT-aware coordination
layer. Note that "custom Raft" in that document never meant implementing
consensus by hand — its own summary reads "Use existing libraries
(hashicorp/raft + BadgerDB) to avoid implementing consensus from scratch."
Track B is a post-1.0 project regardless.

The connection between them: Track B's gRPC transport was also intended to
carry Track A's traffic. That is why P0-2 and P0-3 have a shared fix — and why
the cheap fix for P0-2 does not require touching the proto.

### P0-2. Raft replication uses a cleartext, unauthenticated transport

`queue/raft/manager.go:166`:

```go
transport, err := raft.NewTCPTransport(m.bindAddr, addr, 3, 10*time.Second, os.Stderr)
```

`config.RaftConfig` (`config/config.go:889-911`) exposes `BindAddr`,
`DataDir`, `Peers` — and **no TLS fields at all**. Every other cluster channel
(etcd peer, broker gRPC) is mTLS. Enabling Raft opens a plaintext,
unauthenticated socket carrying queue message payloads and accepting
unauthenticated `AppendEntries` from anything that can reach the port.

Also logs to `os.Stderr`, bypassing `log/slog` in violation of the project's
own logging rule.

**Fix:** `raft.NewNetworkTransport` with a TLS `StreamLayer` — the pattern
Consul uses. It reuses the cluster's existing certificate material and does not
touch `proto/cluster/v1`, so it stays off the critical path to the tag.
Roughly a day. Route the `os.Stderr` writer into `slog` at the same time.

### P0-3. Three Raft RPCs in the cluster protobuf contract are hardcoded failures

`cluster/transport.go:678,687,696`:

```go
func (t *Transport) AppendEntries(ctx context.Context, req *AppendEntriesReq) (*AppendEntriesResp, error) {
	//nolint:godox // TODO: Implement Raft consensus
	return connect.NewResponse(&clusterv1.AppendEntriesResponse{
		Term:    req.Msg.Term,
		Success: false,
	}), nil
}
```

`RequestVote` returns `VoteGranted: false`; `InstallSnapshot` returns an empty
response. A 1.0 tag freezes `proto/cluster/v1`. Shipping stub RPCs inside a
frozen contract is not reversible without a breaking proto change.

**The shipped proto could not carry a real `raft.Transport` even if the stubs
were implemented.** Comparing the deleted plan's sketch
(`docs/custom-raft-implementation-plan.md:150-159`) against what actually
landed in `proto/cluster/v1/broker.proto:37-44`:

| Planned | Shipped |
| --- | --- |
| `rpc InstallSnapshot(stream InstallSnapshotRequest)` | **unary** — the entire snapshot in one message, bounded by the gRPC max message size |
| `rpc TimeoutNow(TimeoutNowRequest)` | **absent** — no leadership transfer |
| `ApplyCommand`, `GetSessionOwner` | absent (Track B only; correctly out of scope) |

A unary `InstallSnapshot` is a defect that the tag would freeze.

**Decision required before tag:** remove these three RPCs from
`proto/cluster/v1`. Nothing calls them, Track A does not need them under the
P0-2 fix, and re-adding RPCs to a proto later is additive and non-breaking —
removing them after 1.0 is not.

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

### P0-7. Session ownership can split-brain

Two defects on the same path.

**(a) `AcquireSession` is an unconditional leased Put, not a CAS.**
`cluster/etcd.go:933-945`:

```go
// This is called after takeover has completed (if needed), so it's safe
// to unconditionally overwrite — the caller already handled ownership transfer.
func (c *EtcdCluster) AcquireSession(ctx context.Context, clientID, nodeID string) error {
	key := sessionsPrefix + clientID + "/owner"
	if err := c.putWithSessionLease(ctx, key, nodeID); err != nil {
```

`ReleaseSession` immediately below (`etcd.go:956-957`) *does* use a CAS
(`If(Compare(Value(key), "=", c.nodeID))`), so the pattern is understood — the
acquire path just doesn't use it. Two nodes racing a CONNECT for the same
client ID both run takeover, both Put, last write wins, and both believe they
own the session.

**(b) The watcher resurrects an ownership key deleted by lease expiry.**
`cluster/etcd.go:2176-2210`: when a session-owner key is deleted and this node
still tracks it as leased, the node re-Puts its own claim:

```go
if event.Type == clientv3.EventTypeDelete {
	if value, tracked := c.getLeasedKey(key); tracked {
		restoreKeys[key] = value
		continue
	}
```

Lease expiry is precisely etcd's signal that this node is **no longer trusted**
to own the session. Restoring the claim inverts that signal.

**Failure scenario:** node A is partitioned, its etcd lease expires, client
reconnects to node B which legitimately takes over. Partition heals; A's
watcher drains the backlog, sees the delete for its tracked key, and re-Puts
`owner=A`. The restore Put (line 2201) is itself unconditional. etcd now names
A as owner while B holds the live TCP connection. Publishes route to A; the
client on B receives nothing.

The `value != c.nodeID` untrack at line 2194 mitigates this only when A
processes B's PUT before the DELETE — which event ordering does not guarantee
across a watch restart.

**Fix:** CAS on acquire (`Compare(CreateRevision(key), "=", 0)`, or compare
against the observed ModRevision after takeover). Delete the resurrection path
entirely; on lease loss, a node must drop the session and let the client
reconnect.

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
  `AppendBatch:261`, `AppendStream:300`, `Read:348`, `Tail:385`,
  `SeekToOffset:420`, `CreateConsumerGroup:498`, `DeleteConsumerGroup:557`,
  `JoinGroup:567`, `Ack:712`, `Nack:751`, `Claim:780`.

`deployments/docker/compose.yaml:12` publishes port `8082` to the host, and the
`fluxmq-dashboard` service at `:45` consumes that same unauthenticated API.

Anyone who can open a TCP connection to the admin port can delete every queue
and reload the broker's configuration. This is the single largest gap between
what the broker claims and what it enforces, and — unlike P0-1 — it needs no
particular configuration to be reachable.

### P0-10. DLQ movement and replication both fail open

**DLQ.** `queue/consumer/manager.go:441-446`: when `DeliveryCount >=
MaxDeliveryCount`, `OnDLQ` is called only if the source read succeeds, its
outcome is not checked, and the pending entry is then removed with
`_ = m.groupStore.RemovePendingEntry(...)` unconditionally. The message is gone
whether or not it reached the DLQ. Explicit `Reject` never enters this path at
all.

**Replication.** `queue/manager.go:204` defaults to `WritePolicyLocal`; the
switch at `:1025-1049` appends locally when `m.raftCoordinator` is nil or
disabled, and `:339` downgrades a replicated distribution mode to `forward`
with a warning. A queue configured for replication can accept writes at a
replication factor of one and answer success.

### P0-11. etcd peer and client traffic is plaintext

`cluster/etcd.go:39` hardcodes `urlPrefix = "http://"`, and the peer URL built
from it is installed at `:174`. Session ownership, subscription routing, and
queue consumer state therefore cross the network unauthenticated and
unencrypted — including on clusters that have configured broker mTLS and
believe themselves secured. Clustering is a supported 1.0 feature; queue Raft
(P0-2) is not, which makes this the more serious of the two cleartext
transports.

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
an error. `logstorage/adapter.go:594`
(`err != ErrGroupNotFound` guarding a `CommitOffset`) is the one most likely to
turn into lost offset commits.

### P1-5. `queue/consumer` is the least-tested and most concurrency-sensitive package

1,789 non-test LOC against 260 test LOC — a **0.15** ratio, in the package that
owns consumer-group membership, heartbeats, work-stealing, and the PEL. For
comparison `mqtt/` sits at 0.94 and `broker/` at 0.96.

This is where a partition-induced duplicate-delivery or lost-message bug will
live. It needs a table-driven rebalance suite and a partition simulation before
1.0.

### P1-6. Suppressed error handling is concentrated in the riskiest packages

570 `//nolint` directives repo-wide, 496 of them `errcheck`. Non-test
concentration: `mqtt/broker` 70, `client/mqtt` 15, `logstorage` 14,
`amqp1/broker` 11. Separately, 99 `_ =` ignored errors in non-test code, with
`amqp/broker` at 23 — protocol error paths silently dropped.

A targeted sweep of the `mqtt/broker` 70 and the `amqp/broker` 23 is
proportionate; the rest can wait.

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

### P2-1. There is no committed plan, roadmap, ADR, CHANGELOG, or SECURITY.md

`plan.md` was deleted in `5101c90f2` (2025-12-24, "Merge roadmap and plan").
`docs/queues-implementation-plan.md`, `docs/performance-optimization-plan.md`,
and `docs/custom-raft-implementation-plan.md` were deleted over the following
three weeks. What replaced them is `docs/content/docs/roadmap.md` — 26 lines,
a ten-bullet emoji list ending in "track open issues and PRs".

All substantive planning now lives in **untracked** local files (`CLAUDE.md`,
`chat/`, `.claude/`). For a 1.0 cut, release scope existing only in one
developer's working tree is the largest process risk in this document.

Missing release-governance files: `SECURITY.md`, `CHANGELOG.md`, `CODEOWNERS`.
Present: `LICENSE`, `CONTRIBUTING.md`.

### P2-2. README contradicts itself on Raft and DLQ

`README.md:29` — `- ✅ Optional Raft layer for queue appends (WIP)` — marks the
same item done and unfinished on one line. Given P0-2 and P0-3, neither half is
accurate. `README.md:96` — "DLQ handler present (delivery path wiring
pending)" — while the Features list above advertises DLQ without qualification.

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
- **The etcd session-owner watcher is mature code** — handles compaction
  restart, watch-channel close, cache reload, and ownership-move untracking
  (`cluster/etcd.go:2142-2210`). The resurrection path is the one flaw in an
  otherwise careful implementation.
- **`logstorage/` has real durability machinery** — segment fsync, directory
  fsync with correct `ErrUnsupported` handling (`segment.go:183-193`),
  recovery, time index, PEL. The gap is the *default* and its
  *unconfigurability*, not the mechanism.
- **Test ratios are strong where it counts**: `broker/` 0.96, `mqtt/` 0.94,
  `server/` 0.92, `storage/` 1.01, `reload/` 1.63.
- **Three previously-known issues are already fixed** (refbuffer double-free,
  unbounded identity cache, breaker half-open).

---

## Recommended sequencing

See `ROADMAP.md`.
