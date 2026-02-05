---
title: Comparison
description: Comprehensive comparison of FluxMQ against industry solutions including EMQX, HiveMQ, Artemis, Mosquitto, NATS, RabbitMQ, and Kafka
---

# Comparison Guide

**Last Updated:** 2026-02-05

This document provides a basic, evergreen comparison guide without hard claims about third‑party products. Vendor feature sets change frequently; verify details with official docs before making a decision.

## FluxMQ Snapshot (from current codebase)

- **Protocols**: MQTT 3.1.1/5.0 (TCP, WebSocket), HTTP publish bridge, CoAP publish bridge, AMQP 1.0 and AMQP 0.9.1
- **Queues**: Durable queues, consumer groups, ack/nack/reject; stream queues supported; retention policies (time/size/message count)
- **DLQ**: Handler exists, automatic DLQ routing not wired yet
- **Clustering**: Embedded etcd coordination, gRPC routing, session takeover, hybrid retained storage, optional Raft for queue appends
- **Storage**: BadgerDB or in‑memory; pluggable interfaces
- **Security**: TLS/mTLS, WebSocket origin validation, rate limiting
- **Observability**: OpenTelemetry metrics/tracing (OTLP); no native Prometheus endpoint
- **Clients**: Go MQTT client, Go AMQP 0.9.1 client (AMQP 1.0 client not provided)

## How to Compare Systems (by category)

### MQTT‑Native Brokers
Best fit when:
- MQTT 5.0 features and device connectivity are the primary focus
- Edge/IoT workloads need lightweight clients and topic‑based routing

### Queue‑Centric Brokers
Best fit when:
- Work‑queue semantics and explicit acknowledgments are primary
- Fine‑grained retry/DLQ handling is critical

### Log‑Centric Brokers
Best fit when:
- Long‑term event retention and replay are primary
- Stream processing and analytics require strong log semantics

### Multi‑Protocol Brokers
Best fit when:
- You need multiple client protocols in a single deployment
- Cross‑protocol routing and shared durability are important

## Decision Checklist

- Protocols you must support (MQTT, AMQP, HTTP, CoAP)
- Delivery guarantees required (QoS 0/1/2, at‑least‑once, exactly‑once)
- Durability and retention needs (time/size retention, replay)
- Operational complexity you can tolerate (single binary vs external dependencies)
- Cluster behavior (session takeover, routing, replication strategy)
- Observability requirements (OTLP, Prometheus, tracing)
- Client library availability and ecosystem fit

## Notes

- FluxMQ is still evolving. For current status of features and limitations, see `/docs/guides/durable-queues`, `/docs/reference/configuration-reference`, and `/docs/roadmap`.
- Benchmarking should be done on your target hardware using `benchmarks/`.
