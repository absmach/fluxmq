---
title: MQTT
description: Connect using MQTT 3.1.1 or 5.0 over TCP
---

# MQTT

**Last Updated:** 2026-02-06

FluxMQ supports MQTT 3.1.1 and MQTT 5.0 over TCP.

## Quick Start

```bash
mosquitto_sub -h localhost -p 1883 -t "sensors/#" -v
```

In another terminal:

```bash
mosquitto_pub -h localhost -p 1883 -t "sensors/temp" -m "22.5" -q 1
```

## MQTT v5 Consumer Groups (Queue Topics)

Use MQTT v5 `SUBSCRIBE` user property `consumer-group` for queue consumers:

```bash
mosquitto_sub -V mqttv5 -h localhost -p 1883 -u usr -P pwd \
  -i worker-1 -q 1 -t '$queue/orders/#' -v \
  -D subscribe user-property consumer-group workers
```

Start another worker in the same group:

```bash
mosquitto_sub -V mqttv5 -h localhost -p 1883 -u usr -P pwd \
  -i worker-2 -q 1 -t '$queue/orders/#' -v \
  -D subscribe user-property consumer-group workers
```

Publish messages to the queue:

```bash
mosquitto_pub -V mqttv5 -h localhost -p 1883 -u usr -P pwd \
  -t '$queue/orders' -m '{"id":"order-1"}' -q 1
```

## Queue ACK/NACK/REJECT with `mosquitto_pub`

`mosquitto_sub` does not automatically send queue acknowledgments.  
To ack (or nack/reject), publish to queue ack topics with MQTT v5 user properties:

```bash
# Ack
mosquitto_pub -V mqttv5 -h localhost -p 1883 -u usr -P pwd \
  -t '$queue/orders/$ack' -m '' \
  -D publish user-property message-id 'orders:42' \
  -D publish user-property group-id 'workers'

# Nack (retry)
mosquitto_pub -V mqttv5 -h localhost -p 1883 -u usr -P pwd \
  -t '$queue/orders/$nack' -m '' \
  -D publish user-property message-id 'orders:42' \
  -D publish user-property group-id 'workers'

# Reject (no retry)
mosquitto_pub -V mqttv5 -h localhost -p 1883 -u usr -P pwd \
  -t '$queue/orders/$reject' -m '' \
  -D publish user-property message-id 'orders:42' \
  -D publish user-property group-id 'workers' \
  -D publish user-property reason 'invalid payload'
```

`message-id` format is typically `<queue>:<offset>` (for example `orders:42`).

## Special Topics

| Topic | Purpose |
|-------|---------|
| `$queue/<queue>` | Publish to a durable queue root |
| `$queue/<queue>/<routing-key>` | Publish or consume queue messages with routing keys |
| `$queue/<queue>/#` | Subscribe to all messages in a queue |
| `$queue/<queue>/$ack` | Acknowledge a queue message |
| `$queue/<queue>/$nack` | Negative-acknowledge (retry) |
| `$queue/<queue>/$reject` | Reject (no retry) |
| `$share/<group>/<filter>` | MQTT shared subscription filter |

## Notes

- Use single quotes around `$queue/...` topics in shell commands to avoid `$` expansion.
- Queue consumer groups require MQTT v5 (`-V mqttv5`) if you want explicit `consumer-group` assignment.
- Reusing the same consumer group with different queue filters creates distinct internal groups per filter (`group@pattern` in logs).
- TLS and mTLS listeners are configured via `server.tcp.tls` and `server.tcp.mtls`.
- Shared subscriptions are supported.

## CLI Tip

To inspect SUBSCRIBE/PUBLISH packets and reason codes during troubleshooting, add `-d`:

```bash
mosquitto_sub -d -V mqttv5 -h localhost -p 1883 -t '$queue/orders/#' \
  -D subscribe user-property consumer-group workers
```

## Learn More

- [/docs/configuration/server](/docs/configuration/server)
- [/docs/messaging/publishing-messages](/docs/messaging/publishing-messages)
- [/docs/messaging/consuming-messages](/docs/messaging/consuming-messages)
- [/docs/messaging/consumer-groups](/docs/messaging/consumer-groups)
