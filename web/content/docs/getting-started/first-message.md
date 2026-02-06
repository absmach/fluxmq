---
title: First Message
description: Publish and subscribe to your first MQTT topic
---

# First Message

**Last Updated:** 2026-02-05

This example uses `mosquitto_pub`/`mosquitto_sub` against the default MQTT TCP listener.

## 1. Subscribe

```bash
mosquitto_sub -p 1883 -t "test/#" -v
```

## 2. Publish

```bash
mosquitto_pub -p 1883 -t "test/hello" -m "Hello FluxMQ"
```

You should see the message appear in the subscriber terminal.

## Next Steps

- [/docs/messaging/publishing-messages](/docs/messaging/publishing-messages)
- [/docs/messaging/consuming-messages](/docs/messaging/consuming-messages)
