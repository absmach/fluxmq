---
title: Broker & Message Routing
description: Internal MQTT broker architecture, session management, message routing mechanisms, topic matching, QoS handling, and cluster integration
---

# MQTT Broker Internals

**Last Updated:** 2026-02-05

This document describes the MQTT broker implementation and how it integrates with queues and clustering. The broker code lives in `mqtt/broker/` and uses shared components in `broker/` (router, webhook events, interfaces).

## Responsibilities

- Manage MQTT sessions (clean start, expiry, inflight tracking, offline queue)
- Route messages to local subscribers via the topic router
- Persist retained messages and wills
- Enforce QoS rules and MaxQoS downgrade
- Integrate with the queue manager for `$queue/` topics
- Integrate with clustering for cross-node routing and session takeover
- Emit webhook events (optional)

## Session Lifecycle (High Level)

- CONNECT arrives over a transport (TCP or WebSocket)
- Broker creates or resumes session based on `clean_start` and expiry
- Session state is restored from storage if needed
- In clustered mode, session ownership is acquired and takeover is handled
- On disconnect, offline queue is persisted (if session not expired)

## Message Routing

- Topic matching uses a trie-based router in `broker/router/`
- Shared subscriptions (MQTT 5.0) are handled by the shared subscription manager
- Retained messages are stored in the retained store and delivered on subscribe
- Queue topics (`$queue/...`) are routed to the queue manager
- Queue acks (`$queue/.../$ack|$nack|$reject`) are handled separately and do not enter normal pub/sub routing

## QoS Handling

- QoS 0, 1, and 2 are supported
- Inflight tracking is persisted for QoS 1/2 sessions
- MaxQoS is enforced by downgrading requested QoS when above server limits

## Cluster Integration

- Session ownership is coordinated via the cluster layer
- Publishes are routed to remote nodes with matching subscriptions
- Retained and will messages are backed by cluster-aware stores
- Session takeover is supported when a client reconnects to another node

## Optional Subsystems

- **Auth/Authz**: pluggable interfaces in `broker/auth.go`
- **Rate limiting**: per-IP and per-client limits in `ratelimit/`
- **Webhooks**: event delivery via `broker/webhook/`
- **OTel metrics/tracing**: optional, configured via `server` settings

## Configuration Pointers

- `broker.*` for broker limits (max message size, max QoS, retry policy)
- `session.*` for session storage and offline queue limits
- `ratelimit.*` for rate limiting
- `webhook.*` for webhook delivery

See `docs/configuration.md` for full details.
