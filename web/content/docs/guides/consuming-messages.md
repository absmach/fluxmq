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

Queue consumers subscribe to `$queue/<queue>/#` and set a consumer group (protocol-specific). See:

- `/docs/guides/consumer-groups`
- `/docs/guides/durable-queues`
