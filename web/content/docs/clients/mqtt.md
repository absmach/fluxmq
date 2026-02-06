---
title: MQTT
description: Connect using MQTT 3.1.1 or 5.0 over TCP
---

# MQTT

**Last Updated:** 2026-02-05

FluxMQ supports MQTT 3.1.1 and MQTT 5.0 over TCP.

## Connect

```bash
mosquitto_sub -p 1883 -t "sensors/#" -v
```

## Notes

- TLS and mTLS listeners are configured via `server.tcp.tls` and `server.tcp.mtls`.
- Shared subscriptions (MQTT 5.0) are supported.

## Learn More

- `/docs/configuration/server`
- `/docs/messaging/publishing-messages`
- `/docs/messaging/consuming-messages`
