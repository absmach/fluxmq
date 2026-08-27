---
title: MQTT
description: Connect using MQTT 3.1.1 or 5.0 over TCP
---

# MQTT

**Last Updated:** 25th February 2026

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
mosquitto_sub -V mqttv5 -h localhost -p 1884 -u usr -P pwd \
  -i worker-1 -q 1 -t '$queue/orders/#' -v \
  -D subscribe user-property consumer-group workers
```

Start another worker in the same group:

```bash
mosquitto_sub -V mqttv5 -h localhost -p 1884 -u usr -P pwd \
  -i worker-2 -q 1 -t '$queue/orders/#' -v \
  -D subscribe user-property consumer-group workers
```

Publish messages to the queue:

```bash
mosquitto_pub -V mqttv5 -h localhost -p 1884 -u usr -P pwd \
  -t '$queue/orders' -m '{"id":"order-1"}' -q 1
```

## Queue ACK/NACK/REJECT with `mosquitto_pub`

`mosquitto_sub` does not automatically send queue acknowledgments.  
To ack (or nack/reject), publish to queue ack topics with MQTT v5 user
properties naming the consumer group and the offset being settled:

```bash
# Ack
mosquitto_pub -V mqttv5 -h localhost -p 1884 -u usr -P pwd \
  -t '$queue/orders/$ack' -m '' \
  -D publish user-property x-group-id 'workers' \
  -D publish user-property x-offset '42'

# Nack (retry)
mosquitto_pub -V mqttv5 -h localhost -p 1884 -u usr -P pwd \
  -t '$queue/orders/$nack' -m '' \
  -D publish user-property x-group-id 'workers' \
  -D publish user-property x-offset '42'

# Reject (no retry)
mosquitto_pub -V mqttv5 -h localhost -p 1884 -u usr -P pwd \
  -t '$queue/orders/$reject' -m '' \
  -D publish user-property x-group-id 'workers' \
  -D publish user-property x-offset '42' \
  -D publish user-property reason 'invalid payload'
```

The offset arrives on the delivery as the `offset` user property, and also as
the tail of the `message-id` handle, whose format is `<queue>:<offset>` (for
example `orders:42`). Offset `0` is the first record in a queue, so it must be
sent explicitly; a settlement with no `x-offset` is refused rather than settling
the head of the queue.

<Callout type="warn">
Earlier releases documented sending `message-id` and `group-id` back on the ack
topic. Those are broker-owned delivery property names: every protocol boundary
strips them from client input so a publisher cannot forge them, which meant a
settlement sent that way never reached the broker. Use `x-group-id` and
`x-offset`, the same inbound command names AMQP 0.9.1 already uses.
</Callout>

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
- TLS and mTLS listeners are configured via `server.mqtt.tcp.tls` and `server.mqtt.tcp.mtls`.
- MQTT mTLS requires two bound factors on every connection: a CA-verified
  client certificate plus CONNECT username/password accepted by the external
  MQTT authenticator. The returned external identity must match the configured
  certificate CN or URI SAN template.
- Username and password are sent only in CONNECT. FluxMQ stores the resolved
  external identity on the session and uses it for later PUBLISH and SUBSCRIBE
  authorization calls.
- Listener protocol mode can be pinned per TCP/WebSocket listener via `protocol: auto|v3|v5`.
- Shared subscriptions are supported.

## CLI Tip

To inspect SUBSCRIBE/PUBLISH packets and reason codes during troubleshooting, add `-d`:

```bash
mosquitto_sub -d -V mqttv5 -h localhost -p 1884 -t '$queue/orders/#' \
  -D subscribe user-property consumer-group workers
```

## Learn More

- [Server configuration](/configuration/server)
- [Publishing messages](/messaging/publishing-messages)
- [Consuming messages](/messaging/consuming-messages)
- [Consumer groups](/messaging/consumer-groups)
