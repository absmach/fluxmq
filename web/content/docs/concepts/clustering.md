---
title: Clustering
description: High availability and cross-node routing basics
---

# Clustering

**Last Updated:** 2026-02-05

Clustering enables high availability and cross-node routing. FluxMQ uses embedded etcd for metadata coordination and a gRPC transport for inter-node message delivery.

## What You Get

- Session ownership and takeover
- Cross-node subscription routing
- Queue consumer registry across nodes
- Optional Raft replication for queues

## Learn More

- [/docs/deployment/running-cluster](/docs/deployment/running-cluster)
- [/docs/architecture/clustering-internals](/docs/architecture/clustering-internals)
