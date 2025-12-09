# Architectural Alignment & Improvement Plan

This document analyzes the current broker implementation against **Eclipse Paho** (Standard Go MQTT implementation) and **SuperMQ** (Production-grade IoT Platform), identifying gaps and proposing improvements.

## 1. Feature & Reliability (vs. Eclipse Paho)

Eclipse Paho is the gold standard for MQTT correctness in Go.

| Feature | Paho (Goal) | Current Implementation | Gap / Improvement |
| :--- | :--- | :--- | :--- |
| **Packet Handling** | `ControlPacket` interface, robust serialization. | `packets/` package with v3/v5 support. Interface-based. | **Good Alignment.** Our `packets` package is solid. Recommendation: Move to `pkg/packets` to expose as a reusable library. |
| **QoS Reliability** | Strict QoS 1/2 state machines (PubRec -> PubRel -> PubComp). Retry loops. | QoS 1/2 implemented in `handlers`. In-flight tracking in `session/inflight.go`. | **Partial.** We need to ensure our `Inflight` persistence survives restarts (currently Memory store). Paho assumes `Store` persistence for reliability. |
| **KeepAlive** | Proactive ping management for connection health. | Basic detection. | **Gap.** We need robust `KeepAlive` enforcement in the read loop to disconnect idle clients proactively. |
| **Ordering** | `SetOrderMatters(false)` optimization. | Strict ordering implied. | **Neutral.** Strict ordering is safer for a broker. Async handling could optionally be added later. |

## 2. Coding Style & Architecture (vs. SuperMQ)

SuperMQ (folks at Absolutum/Mainflux) represents strict, scalable, Cloud-Native Go.

| Aspect | SuperMQ Style | Current Implementation | Improvement Action |
| :--- | :--- | :--- | :--- |
| **Project Layout** | Standard Go Layout (`cmd`, `pkg`, `internal`, `api`). Strict visibility control. | Flat root layout (`broker`, `packets`, etc. in root). | **High Priority.** Move core logic (`broker`, `session`, `store`) to `internal/` to prevent misuse. Move `packets` to `pkg/`. |
| **Configuration** | Struct-based, env var support (Viper/Cleanenv). | `flag` parsing in `main.go`. | **High Priority.** Create `internal/config` package. Defines strictly typed config structs. Support Env vars for Docker usage. |
| **Observability** | Prometheus metrics, OpenTelemetry tracing. | Structured logging (`slog`). | **Medium Priority.** Add `internal/telemetry`. Instrument `packets` (counts) and `broker` (latency). |
| **Interfaces** | Domain-Driven Design (DDD). Hexagonal (Services vs Adapters). | Adapter pattern used (`adapter/http`). | **Good Alignment.** Our new `ConnectionHandler` plugin system aligns well. We should rename `transport` to fit this (e.g., `adapter/tcp`). |
| **Error Handling** | Wrapped errors, clear domain errors. | Basic Go errors. | **Improvement.** Define domain errors in `internal/errors` (e.g., `ErrSessionNotFound`, `ErrQuotaExceeded`). |

## 3. Improvement Roadmap

Based on the comparison, here is the prioritized plan to reach "Production Readiness":

### Phase 1: Structure & Configuration (Immediate)
1.  **Refactor Directory Layout**: Adopt `internal/` and `pkg/` structure.
    *   `pkg/packets`: The packet library.
    *   `internal/broker`: The core.
    *   `internal/session`, `internal/store`, etc.
2.  **Configuration**: Replace `flag` with a Config struct. Allows loading from environment variables (essential for K8s/Docker).

### Phase 2: Reliability & Observability
1.  **Metrics**: Expose `/metrics` endpoint (Prometheus). Track partial packet reads, active connections, messages/sec.
2.  **Persistence**: Implement `FileStore` or `RedisStore` for `store` package. Memory store is insufficient for QoS 1/2 reliability across restarts.
3.  **KeepAlive**: Enforce `KeepAlive` timeout in `transport/tcp.go`.

### Phase 3: Security (SuperMQ Style)
1.  **mTLS**: Add TLS configuration to `TCPFrontend`.
2.  **Auth Interfaces**: Enhance `Authenticator` to support JWT (SuperMQ uses this heavily).

## Recommendation

**Start with Phase 1**. Refactoring the folder structure will communicate "Professional/Production Grade" immediately and enforce better modularity.
