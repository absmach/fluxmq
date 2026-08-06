---
title: Atom Certificate Authentication
description: Resolver-tier MQTT mTLS identity, tenant binding, revocation, and trust refresh
---

# Atom Certificate Authentication

FluxMQ's Atom integration is a **resolver verification tier** for MQTT over the
TCP and WebSocket mTLS listeners. It is intended for standard- and long-lived
device certificates, where authoritative revocation and owner lifecycle state
matter more than removing Atom from the connection path. Plain MQTT, TLS
without client certificates, HTTP, CoAP, and AMQP authentication are unchanged.

At each MQTT CONNECT, FluxMQ copies the verified peer leaf and issuer DER from
the TLS termination path and derives the leaf SHA-256 fingerprint and serial.
It calls Atom's `atom.v1.CertificateService/ResolveCertificateV2` with those
selectors. FluxMQ never reads identity or tenant scope from the certificate
subject. Atom's response is the only certificate-to-entity mapping.

For every publish and subscribe, FluxMQ:

1. extracts the canonical tenant UUID from a Magistrala `m/<tenant>/...` or
   `hc/<tenant>` topic;
2. compares it with Atom's resolved tenant, rejecting cross-tenant and global
   to tenant escalation;
3. revalidates the certificate lifecycle fact from the bounded cache; and
4. invokes the existing MQTT external authorizer with Atom's entity UUID and
   the original operation.

The tenant check always precedes normal authorization. Certificate
authentication does not change topic design or existing authorization policy.

## Failure and cache policy

The default resolver cache TTL is 30 seconds and configuration rejects any TTL
over five minutes. Entries are also capped by the certificate's own expiry and
by `cache_size` (default 10,000, LRU eviction). A valid cache hit may be used
during an Atom outage until that bound expires. A cache miss or expired entry
fails closed. Concurrent connections presenting the same leaf share one
resolver request.

The operational recovery objective is therefore to restore Atom before the
configured cache TTL elapses. After that point, new connections and the next
operation on an existing certificate session are denied until Atom recovers.
Set alerts on `resolver_failures`, `resolver_timeouts`, and a sustained fall in
cache hits. Do not increase the TTL to mask availability problems: it is also
the maximum fallback revocation lag if event consumption is interrupted.

Atom lifecycle events normally make revocation faster than the TTL. Atom
publishes its outbox to a protected `atom.events` stream. FluxMQ authenticates
the exact local publisher principal, validates the Atom v1 envelope, and
idempotently evicts entries by credential, entity, tenant, or issuer. Revocation,
bulk entity revocation, inactive/deleted entities, and frozen/inactive/deleted
tenants also remove matching certificate bindings and disconnect their live MQTT
sessions. A reconnect must resolve authoritatively again. Certificate rotation
replaces the client ID's old binding only for the same entity; the new
certificate must resolve independently.

This reference event topology uses an exact, crash-durable local-principal
publish target and is consequently a single-node deployment contract. FluxMQ
already rejects exact local-principal targets while clustering is enabled. A
future clustered deployment must provide an equivalent fan-out stream in which
every broker node receives every Atom invalidation event; it must not put all
nodes in one competing-consumer group.

## Trust bundle behavior

FluxMQ fetches `/certs/trust-bundle.pem` from Atom before exposing an MQTT mTLS
listener. Startup fails if the first fetch or PEM validation fails. The pool is
refreshed with ETag revalidation on the configured interval and immediately
after a `pki.authority.*` event. A failed refresh retains the last known-good
pool. Each new handshake obtains the current pool atomically, so a newly
provisioned tenant CA becomes usable without restarting FluxMQ.

No legacy certificate service or serial-only resolver is called. Migrated
deployments need only Atom's V2 resolver, published trust endpoint, normal
external authorizer, and outbox event stream.

## Configuration

The Atom event publisher connects to `server.amqp091.local` with the declared
URI SAN, username, and secret. Its exact default-exchange routing key must match
the reserved stream below. `service_token_file` contains the bearer token used
only on the resolver gRPC call and is reread for rotation.

```yaml
cluster:
  enabled: false
  node_id: broker-1

server:
  tcp:
    mtls:
      addr: ":8883"
      protocol: auto
      max_connections: 10000
      read_timeout: 10s
      write_timeout: 10s
      cert_file: /run/secrets/fluxmq-server.pem
      key_file: /run/secrets/fluxmq-server-key.pem
      client_auth: require
      min_version: TLS1.2
  websocket:
    mtls:
      addr: ":8085"
      path: /mqtt
      protocol: auto
      max_connections: 10000
      read_timeout: 10s
      write_timeout: 10s
      cert_file: /run/secrets/fluxmq-server.pem
      key_file: /run/secrets/fluxmq-server-key.pem
      client_auth: require
      min_version: TLS1.2
  amqp091:
    local:
      addr: ":5683"
      max_connections: 8
      cert_file: /run/secrets/fluxmq-server.pem
      key_file: /run/secrets/fluxmq-server-key.pem
      ca_file: /run/secrets/service-client-ca.pem
      client_auth: require
      min_version: TLS1.2

auth:
  external:
    url: https://magistrala-auth.internal:8181
    transport: grpc
    timeout: 2s
    protocols:
      mqtt: true

  certificate:
    enabled: true
    resolver_address: atom.internal:8081
    resolver_insecure: false
    service_token_file: /run/secrets/fluxmq-atom-token
    trust_bundle_url: https://atom.internal/certs/trust-bundle.pem
    resolver_timeout: 3s
    cache_ttl: 30s
    cache_size: 10000
    trust_refresh_interval: 1m
    event_queue: atom.events
    event_consumer_group_prefix: fluxmq-pki
    event_source_principal: atom-events

  local_principals:
    - name: atom-events
      certificate_uri_san: spiffe://example.org/atom/events
      role: publisher
      current_secret_file: /run/secrets/atom-events-secret
      permissions:
        publish:
          - exchange: ""
            routing_key: atom.events
        subscribe: []

queues:
  - name: atom.events
    topics: ["$queue/atom.events/#"]
    reserved: true
    type: stream
    primary_group: fluxmq-pki
    retention:
      max_age: 24h
      max_length_bytes: 67108864
      max_length_messages: 100000
    limits:
      max_message_size: 1048576
      max_depth: 100000
```

`resolver_insecure: true` permits both plaintext resolver gRPC and an HTTP
trust URL and is for a loopback or already protected service-mesh hop only.
Without it, the resolver uses system-trust TLS and `trust_bundle_url` must be
HTTPS. The MQTT mTLS listeners do not need a static `ca_file` when this
integration is enabled; Atom's published bundle is their client trust source.

## Metrics

`GET /api/v1/stats` includes a `certificates` object with active session and
cache counts plus resolver requests/failures/timeouts, cache hits/misses/
evictions, accepted/rejected events, invalidations, lifecycle-disconnected
sessions, tenant denials, and trust refresh successes/failures. Counters have no
entity, tenant, credential, fingerprint, or issuer labels.
