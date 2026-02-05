---
title: Consuming Messages
description: Subscribe with MQTT and receive messages from topics or queues
---

# Consuming Messages

**Last Updated:** 2026-02-05

## MQTT Subscribe

```bash
mosquitto_sub -p 1883 -t "sensors/#" -v
```

Use QoS 1 or 2 when you need delivery guarantees:

```bash
mosquitto_sub -p 1883 -t "sensors/#" -q 1 -v
```

## Queue Consumption

Queue consumers subscribe to `$queue/<queue>/...` and set a consumer group (protocol-specific).

### Basic Queue Subscription

```bash
# Subscribe to all messages in the "orders" queue
mosquitto_sub -p 1883 -t '$queue/orders/#' -q 1 -v
```

### Filtered Queue Subscription

Use wildcards to filter messages within a queue:

```bash
# Only receive messages with routing key starting with "images/"
mosquitto_sub -p 1883 -t '$queue/orders/images/#' -q 1 -v

# Only receive messages matching +/images/# (e.g., eu/images/resize, us/images/png)
mosquitto_sub -p 1883 -t '$queue/orders/+/images/#' -q 1 -v
```

### With Consumer Group (MQTT v5)

```bash
# Join consumer group "workers" for load balancing
mosquitto_sub -p 1883 -t '$queue/orders/#' -q 1 -v \
  -D subscribe user-property consumer-group workers
```

See:

- `/docs/guides/consumer-groups`
- `/docs/guides/durable-queues`
