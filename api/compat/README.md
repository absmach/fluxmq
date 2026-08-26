# Protobuf compatibility baselines

Three source-controlled Buf images, because the schemas they cover carry
different promises.

| Image | Covers | Promise |
| --- | --- | --- |
| `proto-public-v1.binpb` | `proto/queue/v1`, `proto/auth/v1` | The client-facing contract. External clients compile against it, so a change here is a change to a published API. |
| `proto-cluster-v1.binpb` | `proto/cluster/v1` | The inter-node wire. It is an implementation detail shared between broker nodes, not exposed to clients. |
| `proto-message-v1.binpb` | `proto/message/v1` | The stored message format. Nothing else reads what is already on disk, so a break here is the only one that cannot be fixed by upgrading both ends. |

CI runs `make proto-breaking`, which checks all three. **Every gate is a hard
failure.** The split exists so that a cluster-wire change is reviewed on its own
terms rather than being weighed against a client-facing promise — not so that it
can be waved through.

`proto/message/v1` additionally has an implementation to stay honest with. It is
the schema of record for the format the hand-written codec in `message/codec.go`
writes, and `message/conformance_test.go` holds the two against each other: the
codec's bytes must parse into the schema's messages field for field, and
`message/testdata/envelope-v1.bin` pins the encoding itself. Changing the schema
without the codec, or the codec without the schema, fails there.

Each image defines its own scope: `buf breaking` reports a change only for
files present in both the image and the workspace, so the public gate ignores
the cluster schemas and the internal gate ignores the client-facing ones.

## Refreshing a baseline

```sh
make proto-baseline            # all three
make proto-baseline-public     # queue/v1 + auth/v1
make proto-baseline-internal   # cluster/v1
make proto-baseline-stored     # message/v1
```

The stored format has a second baseline of its own, the golden encoding. Refresh
it only for an intended format change, and put both diffs in the same review:

```
go test ./message -run TestGoldenEnvelopeEncoding -update-envelope-golden
```

Refresh only after the schema change has been reviewed, and commit the
regenerated image together with the `.proto` change it corresponds to. A
baseline refreshed in its own commit, ahead of the change it permits, defeats
the check.

## Go API baselines

`go-queue-v1.txt` renders `queue.CommandProcessor`, the optional capabilities
beside it, and the typed command/outcome values. `go-message-v1.txt` renders the
canonical envelope, its immutable metadata values, and their constructors and
methods. They are checked by their package tests.

It exists because the compile-time guards cannot see what they promise to
guard. `queue/state_machine_api_compat_test.go` duplicates the interface and
assigns it in both directions, which pins the method set — but it names the
live command types, so adding a field to a command, or changing an enum's
underlying type, satisfies it unchanged. A `SeekKind` string-to-int change
passed that guard.

Record an intended change with:

```
make go-baseline
```

and put the baseline diff in the review, exactly as a `proto-baseline` refresh
is reviewed.

Protocol property names are pinned separately, by
`TestProtocolPropertyNamesAreFrozen` in `message` and
`TestCommandPropertyNamesAreFrozen` in `queue/types`. They are literal tables
rather than a rendering: a client reads and writes those strings, so the value
is the contract, and comparing a constant to itself would pass through a rename.
