# FluxMQ 1.0 API Compatibility

This document is the compatibility baseline for the broker interfaces that are
expensive to change after 1.0. It freezes wire shape and the queue failure
vocabulary. Delivery and settlement are implemented today, but their shared
protocol-independent state machine is the next architecture item and is not
expanded into new guarantees here.

## Stable surfaces

- Protobuf: `proto/queue/v1`, `proto/auth/v1`, and `proto/cluster/v1`.
  `api/compat/proto-v1.binpb` is the reviewed descriptor baseline and CI runs
  `make proto-breaking` against it.
- Go: `broker.Authenticator`, `broker.Authorizer`, `broker.QueueManager`, and
  `broker.StreamQueueManager`. `broker/api_compat_test.go` pins their exact
  method sets and signatures in both directions.
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

For MQTT 3.1.1, a queue write failure has no negative PUBACK/PUBREC reason
space, so the broker closes the connection. MQTT 5 maps a QoS 1 write failure to
the reason table above. An MQTT QoS 2 write happens after PUBREL, where PUBCOMP
cannot describe a queue failure; the canonical settlement state machine must
resolve that limitation before 1.0 claims stronger QoS 2 failure reporting.
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
preservation. Cross-operation delivery and settlement conformance belongs to
the shared queue state-machine work rather than being duplicated here.
