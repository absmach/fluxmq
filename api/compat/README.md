# Protobuf compatibility baseline

`proto-v1.binpb` is the source-controlled Buf image for the public
`proto/queue/v1`, `proto/auth/v1`, and `proto/cluster/v1` contracts.

CI runs `make proto-breaking` against this image. Refresh it with
`make proto-baseline` only when an additive protobuf change has been reviewed;
commit the regenerated image with the schema change.
