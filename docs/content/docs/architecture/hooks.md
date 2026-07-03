---
title: Blocking Hooks
description: Synchronous broker hooks for allow, deny, and safe request mutation before FluxMQ processes an operation
---

# Blocking Hooks

**Last Updated:** 2026-07-03

FluxMQ blocking hooks call an external service before selected broker
operations continue. A hook service can allow an operation, deny it, or return
a limited mutation such as a normalized topic, rewritten payload, or updated
metadata.

Blocking hooks are different from [webhooks](/architecture/webhooks):

- `hooks` are synchronous and sit on the publish/subscribe path.
- `webhook` is asynchronous event fanout after broker events happen.
- A slow or unavailable hook service directly affects client latency unless
  `hooks.fail_mode: "allow"` is configured.

## Execution Flow

```text
Client operation
  |
  v
FluxMQ validates protocol frame/request
  |
  v
Blocking hook callout
  |
  +-- deny  -> reject the operation
  |
  +-- error -> apply hooks.fail_mode
  |
  v
FluxMQ applies protocol-specific checks and authorizes the effective topic/filter
  |
  v
Broker routing, storage, delivery
```

## Supported Hook Points

| Hook                  | MQTT | HTTP | CoAP | AMQP 1.0 | AMQP 0.9.1 |
| --------------------- | ---- | ---- | ---- | -------- | ---------- |
| `auth_on_register`    | yes  | no   | no   | no       | no         |
| `auth_on_publish`     | yes  | yes  | yes  | yes      | yes        |
| `auth_on_subscribe`   | yes  | no   | no   | yes      | yes        |
| `auth_on_unsubscribe` | yes  | no   | no   | no       | no         |

Protocol names used in configuration are `mqtt`, `http`, `coap`, `amqp`, and
`amqp091`.

## Configuration

Set `hooks.url` to enable blocking hooks. The URL is the base address for the
hook service.

```yaml
hooks:
  url: "https://hooks.internal:7017"
  transport: "grpc"       # "grpc" (default) or "http"
  timeout: "500ms"        # per-call timeout; 0 uses the client default
  fail_mode: "deny"       # "deny" (default) or "allow"

  protocols:
    mqtt: true
    http: true
    coap: false
    amqp: true
    amqp091: true

  events:
    auth_on_register: true
    auth_on_publish: true
    auth_on_subscribe: true
    auth_on_unsubscribe: true
```

When `hooks.protocols` is omitted or empty, every supported protocol runs hooks
when `hooks.url` is set. When `hooks.events` is omitted or empty, every
supported hook point runs. In production, prefer explicit maps so enabling the
URL does not accidentally put every protocol and hook event on the synchronous
callout path.

## Transports

### gRPC / Connect

With `transport: "grpc"`, FluxMQ calls `HookService.Handle` from
`proto/auth/v1/auth.proto` using Connect gRPC.

The service receives `HookReq` and returns `HookRes`. The response result must
be `HookResultOk` or `HookResultDeny`; `HookResultUnspecified` is treated as a
hook error and follows `hooks.fail_mode`.

### HTTP JSON

With `transport: "http"`, FluxMQ sends `POST {hooks.url}/hooks` with a JSON
body.

Example request:

```json
{
  "hook": "auth_on_publish",
  "client_id": "device-1",
  "external_id": "tenant-a/device-1",
  "protocol": "mqtt",
  "topic": "raw/device-1/temperature",
  "payload": "MjIuNQo=",
  "qos": 1,
  "retain": false,
  "properties": {
    "protocol": "mqtt",
    "external_id": "tenant-a/device-1"
  }
}
```

`payload` is JSON-encoded bytes, so it appears as base64 in JSON.
`username` and `password` are populated only for `auth_on_register`.

Example allow response with a normalized topic:

```json
{
  "result": "ok",
  "topic": "tenant-a/devices/device-1/temperature",
  "properties": {
    "policy": "normalized"
  }
}
```

Example deny response:

```json
{
  "result": "deny",
  "reason_code": 403,
  "reason": "topic is outside tenant scope"
}
```

The HTTP `result` field must be exactly `ok` or `deny` ignoring case and
surrounding whitespace. Any other value is a hook error.

## Response Fields

| Field         | Behavior                                                                                                                                         |
| ------------- | ------------------------------------------------------------------------------------------------------------------------------------------------ |
| `result`      | Required decision: allow (`ok`) or deny (`deny`).                                                                                                |
| `topic`       | Non-empty value replaces the requested topic, filter, queue, or address.                                                                         |
| `payload`     | Publish-only mutation. Use `payload_set: true` when replacing the payload, including with empty bytes.                                           |
| `qos`         | Supported for MQTT subscribe QoS. Do not mutate publish QoS; MQTT rejects publish QoS changes.                                                   |
| `retain`      | Publish-only mutation for protocols with retained-message semantics.                                                                             |
| `properties`  | Merged into the current message properties map. Existing keys can be overwritten; broker-reserved origin keys may be reset by protocol handlers. |
| `external_id` | `auth_on_register` may set or override the session external identity.                                                                            |
| `reason_code` | Optional machine-readable reason for denials or errors.                                                                                          |
| `reason`      | Optional human-readable reason for denials or logs.                                                                                              |

The response schema is shared across protocols, but every protocol applies only
the fields that are safe for its state machine. For example, MQTT publish topic
mutations are revalidated before publish, and MQTT publish QoS mutations are
rejected because they would desynchronize packet acknowledgments.

## Auth Interaction

When both `auth` and `hooks` are configured, FluxMQ authenticates first, passes
the resulting external identity into the hook request, then re-runs
authorization against the effective topic or filter after the hook returns.

`auth_on_register` can replace the authenticated external identity for MQTT
sessions by returning `external_id`. If that hook denies the registration,
FluxMQ drops any cached identity for the client.

## Failure Mode

`hooks.fail_mode` controls hook callout errors such as timeouts, non-2xx HTTP
responses, invalid responses, or gRPC failures.

| Value   | Behavior                                                         |
| ------- | ---------------------------------------------------------------- |
| `deny`  | Default. Reject the operation when the hook service errors.      |
| `allow` | Continue with the original request when the hook service errors. |

Denials returned by the hook service always reject the operation regardless of
`fail_mode`.

## Operational Guidance

- Keep hook services close to the broker network path and set a short timeout.
- Use TLS or a private network; register hooks can receive credentials.
- Start with explicit `protocols` and `events` maps.
- Treat publish hooks as hot-path code. A hook timeout adds directly to client
  publish or subscribe latency.
- Use `webhook` for asynchronous observability and `hooks` only when the
  external service must make the allow/deny or normalization decision before
  FluxMQ continues.
