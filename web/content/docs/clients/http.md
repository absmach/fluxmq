---
title: HTTP
description: Publish messages over the HTTP bridge
---

# HTTP

**Last Updated:** 2026-02-05

FluxMQ includes an HTTP publish bridge.

## Enable

Set `server.http.plain.addr` (or TLS/MTLS) in your config.

## Publish

`POST /publish` with JSON body:

```json
{"topic":"sensors/temp","payload":"MjIuNQ==","qos":1,"retain":false}
```

`payload` is base64-encoded in JSON.

Example:

```bash
curl -sS -X POST http://localhost:8080/publish \
  -H 'Content-Type: application/json' \
  -d '{"topic":"sensors/temp","payload":"MjIuNQ==","qos":1,"retain":false}'
```

## Health

`GET /health` returns a simple status payload.

## Learn More

- `/docs/guides/publishing-messages`
- `/docs/configuration/server`
