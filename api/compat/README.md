# Protobuf compatibility baselines

Two source-controlled Buf images, because the schemas they cover carry
different promises.

| Image | Covers | Promise |
| --- | --- | --- |
| `proto-public-v1.binpb` | `proto/queue/v1`, `proto/auth/v1` | The client-facing contract. External clients compile against it, so a change here is a change to a published API. |
| `proto-cluster-v1.binpb` | `proto/cluster/v1` | The inter-node wire. It is an implementation detail shared between broker nodes, not exposed to clients. |

CI runs `make proto-breaking`, which checks both. **Both gates are hard
failures.** The split exists so that a cluster-wire change is reviewed on its
own terms rather than being weighed against a client-facing promise — not so
that it can be waved through.

Each image defines its own scope: `buf breaking` reports a change only for
files present in both the image and the workspace, so the public gate ignores
the cluster schemas and the internal gate ignores the client-facing ones.

## Refreshing a baseline

```sh
make proto-baseline            # both
make proto-baseline-public     # queue/v1 + auth/v1
make proto-baseline-internal   # cluster/v1
```

Refresh only after the schema change has been reviewed, and commit the
regenerated image together with the `.proto` change it corresponds to. A
baseline refreshed in its own commit, ahead of the change it permits, defeats
the check.
