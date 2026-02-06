---
title: Security
description: TLS/mTLS listeners, inter-broker TLS, and rate limiting
---

# Security Configuration

**Last Updated:** 2026-02-05

## TLS and mTLS

Listeners share TLS fields across `tls` and `mtls` blocks.

```yaml
server:
  tcp:
    tls:
      addr: ":8883"
      cert_file: "/path/server.crt"
      key_file: "/path/server.key"
    mtls:
      addr: ":8884"
      cert_file: "/path/server.crt"
      key_file: "/path/server.key"
      ca_file: "/path/clients-ca.crt"
      client_auth: "require"
```

## Inter-Broker TLS

```yaml
cluster:
  transport:
    tls_enabled: true
    tls_cert_file: "/path/transport.crt"
    tls_key_file: "/path/transport.key"
    tls_ca_file: "/path/transport-ca.crt"
```

## Rate Limiting

```yaml
ratelimit:
  enabled: true
  connection:
    enabled: true
    rate: 50
    burst: 200
  message:
    enabled: true
    rate: 500
    burst: 2000
```

## Learn More

- `/docs/reference/configuration-reference`
