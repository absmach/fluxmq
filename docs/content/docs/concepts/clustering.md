---
title: Clustering
description: High availability and cross-node routing basics
---

# Clustering

**Last Updated:** 5th February 2026

Clustering enables high availability and cross-node routing. FluxMQ uses embedded etcd for metadata coordination and a gRPC transport for inter-node message delivery.

## What You Get

- Session ownership and takeover
- Cross-node subscription routing
- [Shared subscription](/concepts/shared-subscriptions) groups that span nodes — members on different nodes are one group, and each message reaches one of them
- Queue consumer registry across nodes
- Optional Raft replication for queues

## Learn More

- [Running a cluster](/deployment/running-cluster)
- [Clustering internals](/architecture/clustering)
