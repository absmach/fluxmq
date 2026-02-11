---
title: WebSocket
description: Connect with MQTT over WebSocket
---

# WebSocket

**Last Updated:** 2026-02-05

FluxMQ supports MQTT over WebSocket. Configure the listener under `server.websocket.*`.

## Default Path

The default WebSocket path is `/mqtt`.

Example URL:

```
ws://localhost:8083/mqtt
```

Use any MQTT-over-WebSocket client library and point it to the WebSocket URL.

## Learn More

- [Server configuration](/docs/configuration/server)
