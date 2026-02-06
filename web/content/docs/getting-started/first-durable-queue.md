---
title: First Durable Queue
description: Send a message to a durable queue and consume it with a consumer group
---

# First Durable Queue

**Last Updated:** 2026-02-05

Durable queues use the `$queue/` topic prefix. The broker stores the message in the queue log and delivers it to consumers in a group.

## 1. Publish to a Queue

```bash
mosquitto_pub -p 1883 -t "$queue/orders" -m "order-1" -q 1
```

## 2. Consume with a Consumer Group

Choose a client that can set a consumer group:

- **MQTT v5**: set the `consumer-group` user property on SUBSCRIBE.
- **AMQP 1.0**: set `consumer-group` in attach properties.
- **AMQP 0.9.1**: set `x-consumer-group` on `basic.consume`.

## 3. Acknowledge

Acks are required for durable queues. For MQTT v5, publish to:

- `$queue/<queue>/$ack`
- `$queue/<queue>/$nack`
- `$queue/<queue>/$reject`

Include `message-id` and `group-id` as user properties.

## Next Steps

- `/docs/messaging/durable-queues`
- `/docs/concepts/consumer-groups`
