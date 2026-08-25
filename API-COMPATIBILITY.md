# FluxMQ 1.0 API Compatibility

This document is the compatibility baseline for the broker interfaces that are
expensive to change after 1.0. It freezes wire shape and the queue failure
vocabulary, and the protocol-independent queue command model used by every
supported adapter.

## Stable surfaces

- Protobuf, public: `proto/queue/v1` and `proto/auth/v1`. These are what
  external clients compile against. `api/compat/proto-public-v1.binpb` is the
  reviewed descriptor baseline.
- Protobuf, internal: `proto/cluster/v1`. This is the inter-node wire, an
  implementation detail shared between broker nodes rather than a client-facing
  contract. `api/compat/proto-cluster-v1.binpb` is its separate baseline, so a
  cluster-wire change is reviewed on its own terms instead of being weighed
  against a published promise.

  CI runs `make proto-breaking`, which checks both. Both are hard failures.
- Go: `broker.Authenticator`, `broker.Authorizer`, `broker.QueueManager`, and
  `broker.StreamQueueManager`, plus `queue.CommandProcessor` and its typed
  command/outcome values. Compile-time guards pin the interface method sets and
  signatures in both directions. The state-machine implementation remains
  private so internal helpers and storage dependencies do not become API.
- Configuration: the accepted YAML keys and strict decoding behavior pinned by
  `config/schema_test.go`. Adding a key is compatible; removing, renaming, or
  changing absent-versus-zero behavior is not.
- Protocol queue projections described below for MQTT 3.1.1, MQTT 5.0,
  AMQP 0.9.1, AMQP 1.0, and the Connect QueueService.

The HTTP-MQTT and CoAP bridges and queue Raft replication remain experimental.
They are not part of this compatibility contract. Replication-related failures
are nevertheless represented without leaking Raft into the public error model.

## Evolution rules

For protobuf APIs:

- only additive messages, fields, enum values, and methods are allowed;
- field numbers and enum numbers are never reused;
- an existing field's meaning, default interpretation, or required behavior is
  not changed;
- removals reserve the old name and number when a future major version permits
  the removal;
- `make proto-baseline` is run only after intentional review of an additive
  contract change. A baseline refresh is committed with its schema change.

For Go interfaces, adding a method is breaking for external implementations as
well as removing or changing one. Add a new optional capability interface
instead. Concrete implementation types are not frozen interfaces.

For YAML, new optional keys with backward-compatible defaults are additive.
Existing keys and absent-versus-zero semantics remain stable.

## Queue failure contract

`queue.Failure` is the protocol-independent failure. Its stable fields are:

- `code`: `canceled`, `invalid_argument`, `not_found`, `already_exists`,
  `conflict`, `failed_precondition`, `resource_exhausted`, `out_of_range`,
  `unavailable`, `deadline_exceeded`, or `internal`;
- `retryable`;
- `ownership`: `unspecified`, `caller`, `other`, or `lost`;
- `leader`: `unspecified`, `required`, `unavailable`, or `not_local`;
- `durability`: `unspecified`, `not_attempted`, `unconfirmed`, or
  `unsupported`.

Clients make decisions from these values or their documented native projection,
never from error text. Unknown implementation errors map to `internal`.

| Domain code | Connect | MQTT 5 publish acknowledgement | AMQP 0.9.1 | AMQP 1.0 condition | AMQP 1.0 management |
| --- | --- | --- | --- | --- | --- |
| `invalid_argument` | `invalid_argument` | implementation-specific error | precondition-failed | invalid-field | 400 |
| `not_found` | `not_found` | implementation-specific error | not-found | not-found | 404 |
| `already_exists` | `already_exists` | implementation-specific error | precondition-failed | precondition-failed | 409 |
| `conflict` | `aborted` | implementation-specific error | resource-locked | resource-locked | 409 |
| `failed_precondition` | `failed_precondition` | implementation-specific error | precondition-failed | precondition-failed | 412 |
| `resource_exhausted` | `resource_exhausted` | quota-exceeded | resource-error | resource-limit-exceeded | 429 |
| `out_of_range` | `out_of_range` | implementation-specific error | precondition-failed | invalid-field | 400 |
| `unavailable` | `unavailable` | unspecified error | internal-error | internal-error | 503 |
| `deadline_exceeded` | `deadline_exceeded` | unspecified error | internal-error | internal-error | 503 |
| `canceled` | `canceled` | unspecified error | internal-error | internal-error | 500 |
| `internal` | `internal` | unspecified error | internal-error | internal-error | 500 |

Every Connect QueueService error carries `QueueErrorDetail`. AMQP 1.0 rejected
deliveries and management errors carry the five fields in `fluxmq:*` info or
application-properties. MQTT and AMQP 0.9.1 have smaller native error spaces;
their stable mapping is the table above. Error descriptions are diagnostic and
are not a compatibility surface.

## Append semantics

- `Append` targets exactly the named queue and returns the offset assigned by
  that append. It does not route through topic matching or auto-create another
  queue.
- `AppendBatch` is atomic and returns one contiguous offset range for a
  single-node buffered queue. Replicated or fsync batch modes are rejected with
  `failed_precondition` until they can provide the same contract.
- `AppendQueue` commits a successful prefix and stops at the first failed
  append. Its response never counts a failed append.
- Message keys and headers are opaque bytes and survive the queue storage
  round-trip. Broker-owned storage metadata is kept outside the user-header
  namespace.

## Queue state-machine semantics

`queue.CommandProcessor` is the canonical append, consume, settlement, claim,
and seek boundary. MQTT, AMQP, Connect, and the delivery engine adapt their
wire behavior to these commands; they do not own separate queue transitions.
The existing broker interfaces are unchanged.

- Queue-mode `Consume` claims records into the pending-entry list before they
  are returned. A PEL or cursor update failure is returned rather than reported
  as an empty or successful consume.
- Stream-mode `Consume` only peeks. An adapter calls `CommitConsume` after it
  has delivered the selected prefix, so a send failure does not advance the
  stream cursor past an undelivered record.
- `Ack`, `Nack`, and `Reject` identify a record by its queue offset. A protocol
  that exposes a textual message identifier to its clients derives it at its own
  boundary; it is never parsed back into an offset. Adapters that receive a
  delivery as a property map rather than an envelope resolve the offset once, at
  delivery, from the projected `offset` property.
- `Ack`, `Nack`, and `Reject` enforce pending ownership when a consumer ID is
  present. Compatibility adapters that cannot carry a consumer ID resolve the
  owner from the group PEL. A multi-offset command stops at its first failure,
  and both its in-process outcome and the `QueueProgressDetail` on the returned
  error identify the successfully settled prefix and the group cursor it left.
- `Nack` releases an entry for redelivery after at least the requested delay;
  normal visibility and claim-idle rules may extend that wait. A zero delay
  makes the entry immediately eligible.
- `Reject` appends durably to the DLQ before removing the source pending entry.
  It remains loss-safe and duplicate-detectable, but source settlement and DLQ
  append are not yet one crash-atomic transition.
- `Claim` considers pending records only, orders them oldest first, applies the
  minimum-idle threshold, and transfers ownership to the named consumer.
- `Seek` resolves a bounded offset without changing consumer-group state.
  Offset seeks clamp to the queue's current head/tail; timestamp seeks return
  the first record at or after the requested time, or the tail.

For MQTT 3.1.1, a queue write failure has no negative PUBACK/PUBREC reason
space, so the broker closes the connection. MQTT 5 maps a QoS 1 write failure to
the reason table above. An exact MQTT QoS 2 queue write happens after PUBREL,
where PUBCOMP cannot carry a queue failure. The broker therefore executes that
append synchronously, withholds PUBCOMP on failure, and leaves the inbound
transaction pending for protocol retry. If the append succeeds and the process
crashes before inbound settlement is recorded, a retry can still duplicate the
append; the recoverable transition journal in the roadmap is the fix for that
remaining crash window.
AMQP 0.9.1 publisher confirms use ack/nack; a non-confirmed publish closes the
channel on queue failure. AMQP 1.0 uses `accepted` or typed `rejected` outcomes;
a pre-settled sender cannot receive an outcome.

## Verification

Run:

```sh
make proto-lint
make proto-breaking
go test ./broker ./queue ./server/queue ./mqtt/broker ./amqp/broker ./amqp1/broker ./logstorage
```

These checks pin the interface shape, protobuf compatibility, error projection,
exact append offsets and targeting, batch behavior, and binary key/header
preservation. `TestStateMachineStorageContract` runs append, consume, ack, nack,
reject, claim, and seek against memory and persistent log storage;
`TestMQTTAndAMQPManagerAdapterContract` and
`TestConnectAdapterUsesSharedQueueStateMachine` apply the same behavior through
the supported adapter boundaries.
