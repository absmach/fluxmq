# FluxMQ Short-Term Broker-Core Plan

## Scope

Close the highest-value message-broker correctness gaps without expanding into
admin access control, release automation, GitHub governance, operational
rollout, broad audits, or soak testing.

Queue Raft remains experimental and disabled by default. The work is ordered
to freeze wire behavior before changing ownership and delivery semantics:

1. Cluster wire contract and takeover state.
2. Session ownership correctness.
3. Dead-letter queue correctness.
4. Secure cluster transports.
5. Replication failure behavior.

## 1. Freeze the cluster wire contract

- Remove the unused `AppendEntries`, `RequestVote`, and unary
  `InstallSnapshot` RPCs from `proto/cluster/v1`.
- Add properties to `InflightMessage` and preserve them through session export,
  transport, restoration, retry, and settlement.
- Regenerate protobuf and Connect code and verify downstream wire consumers.
- Do not implement the abandoned gRPC Raft transport.

## 2. Make session ownership atomic

- Acquire fresh ownership with an etcd compare-and-swap; reacquisition by the
  same node is idempotent and another owner produces a typed conflict.
- Transfer takeover ownership with a compare-and-swap from the observed old
  owner to the new owner.
- Never resurrect session-owner keys after lease expiry.
- Disconnect sessions belonging to a lost ownership lease before accepting new
  claims.
- Test simultaneous connection, concurrent takeover, partition, lease expiry,
  recovery, and stale-cache behavior. Exactly one live owner must remain.

## 3. Make DLQ transitions loss-safe

- Return DLQ creation, append, and sync errors to the caller.
- Use the normal queue durability path for DLQ appends.
- Remove a pending entry or advance a stream cursor only after the durable DLQ
  append succeeds.
- Route explicit reject through the same transition.
- Add a stable broker-owned transfer ID derived from source queue, group, and
  offset so crash-window duplicates are detectable.
- If DLQ is disabled or unavailable, leave the source delivery pending.

## 4. Secure existing cluster transports

- Replace the queue-Raft plaintext TCP transport with a TLS stream layer.
- Reuse the cluster transport certificate, key, and CA for broker, embedded-etcd,
  and queue-Raft traffic.
- Add explicit `cluster.allow_insecure` development opt-in; secure clustering is
  the default.
- Keep embedded-etcd client traffic loopback-only and route Raft diagnostics
  through structured logging.
- Cover transport behavior with in-process generated-certificate tests.

## 5. Make experimental replication fail closed

- Validate replication at startup and on queue creation/update.
- Reject replication when Raft is disabled, unhealthy, missing its experimental
  enablement, or lacks a usable group or leader.
- For replicated queues, allow only `reject` or `forward` write policies and
  remove unavailable/unknown-policy local fallbacks.
- Validate replication factor and minimum in-sync replicas before accepting
  writes.
- Continue rejecting `ack_durability: fsync` with replication in this phase.

## Performance and acceptance

- Use allocation-reporting benchmarks for any optimization in the touched
  takeover, ownership, DLQ, or replicated-write paths. Take optimizations only
  when they stay inside those subsystems and preserve correctness; do not add
  speculative fast paths without a measured bottleneck.
- Reject unexplained regressions above 10% in affected benchmarks.
- Run focused failure-injection and protocol tests, then `make test`, project
  lint with the repository Go toolchain, and `go vet ./...`.

## Deferred

Admin/API authentication, release and image workflows, GitHub milestones,
public-roadmap work, broad parser fuzzing, interoperability matrices, and soak
testing remain outside this short-term plan.
