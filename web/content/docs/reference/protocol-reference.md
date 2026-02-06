---
title: Protocol Reference
description: Supported protocols and transport options
---

# Protocol Reference

**Last Updated:** 2026-02-05

FluxMQ supports multiple protocols and transports:

- **MQTT 3.1.1 and MQTT 5.0** over TCP
- **MQTT over WebSocket**
- **AMQP 1.0**
- **AMQP 0.9.1**
- **HTTP publish bridge** (`POST /publish`)
- **CoAP publish bridge**

Protocol listeners are configured under `server.*` in the YAML config.

## Learn More

- [Server configuration](/docs/configuration/server)
- [MQTT client](/docs/clients/mqtt)
- [WebSocket client](/docs/clients/websocket)
- [HTTP client](/docs/clients/http)
