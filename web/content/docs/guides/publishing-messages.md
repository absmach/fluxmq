---
title: Publishing Messages
description: Publish via MQTT or HTTP, including queue and retained messages
---

# Publishing Messages

**Last Updated:** 2026-02-05

## MQTT Publish

```bash
mosquitto_pub -p 1883 -t "sensors/temp" -m "22.5" -q 1
```

Retained message:

```bash
mosquitto_pub -p 1883 -t "sensors/last" -m "22.5" -r
```

## HTTP Publish (Bridge)

Enable the HTTP bridge by setting `server.http.plain.addr` in your config. The `/publish` endpoint accepts JSON with `topic`, `payload`, `qos`, and `retain`.

Note: `payload` is a base64-encoded string in JSON.

```bash
curl -sS -X POST http://localhost:8080/publish \
  -H 'Content-Type: application/json' \
  -d '{"topic":"sensors/temp","payload":"MjIuNQ==","qos":1,"retain":false}'
```

## Publishing to Queues

Use the `$queue/` prefix to publish to durable queues:

```bash
mosquitto_pub -p 1883 -t '$queue/orders' -m '{"id": "order-1"}' -q 1
```

### With Routing Keys

Add routing keys after the queue name to enable filtered consumption:

```bash
# Routing key: images/png
mosquitto_pub -p 1883 -t '$queue/orders/images/png' -m '{"file": "photo.png"}' -q 1

# Routing key: eu/images/resize
mosquitto_pub -p 1883 -t '$queue/orders/eu/images/resize' -m '{"file": "photo.png"}' -q 1
```

Consumers subscribed to `$queue/orders/images/#` will receive the first message.
Consumers subscribed to `$queue/orders/+/images/#` will receive both messages.

See `/docs/guides/durable-queues` for wildcard patterns, acknowledgments, and consumer groups.
