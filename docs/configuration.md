# MQTT Broker Configuration

## Overview

The MQTT broker can be configured via YAML configuration file. If no configuration file is specified, sensible defaults are used.

## Configuration File

Create a YAML file (e.g., `config.yaml`):

```yaml
server:
  tcp_addr: ":1883"
  tcp_max_connections: 10000
  tcp_read_timeout: 60s
  tcp_write_timeout: 60s
  
  # TLS configuration
  tls_enabled: false
  tls_cert_file: "/path/to/cert.pem"
  tls_key_file: "/path/to/key.pem"
  
  # HTTP adapter (future)
  http_addr: ":8080"
  http_enabled: false
  
  # WebSocket adapter (future)
  ws_addr: ":8083"
  ws_enabled: false
  
  shutdown_timeout: 30s

broker:
  max_message_size: 1048576  # 1MB
  max_retained_messages: 10000
  retry_interval: 20s
  max_retries: 0  # infinite

session:
  max_sessions: 10000
  default_expiry_interval: 300  # 5 minutes
  max_offline_queue_size: 1000
  max_inflight_messages: 100

log:
  level: info  # debug, info, warn, error
  format: text  # text, json

storage:
  type: memory  # memory (etcd support coming later)
```

## Configuration Options

### Server

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `tcp_addr` | string | `:1883` | TCP listen address |
| `tcp_max_connections` | int | `10000` | Maximum concurrent connections |
| `tcp_read_timeout` | duration | `60s` | Read timeout for connections |
| `tcp_write_timeout` | duration | `60s` | Write timeout for connections |
| `tls_enabled` | bool | `false` | Enable TLS |
| `tls_cert_file` | string | `""` | Path to TLS certificate |
| `tls_key_file` | string | `""` | Path to TLS private key |
| `shutdown_timeout` | duration | `30s` | Graceful shutdown timeout |

### Broker

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `max_message_size` | int | `1048576` | Maximum MQTT message size (1MB) |
| `max_retained_messages` | int | `10000` | Maximum retained messages |
| `retry_interval` | duration | `20s` | QoS 1/2 retry interval |
| `max_retries` | int | `0` | Max retries (0 = infinite) |

### Session

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `max_sessions` | int | `10000` | Maximum sessions allowed |
| `default_expiry_interval` | int | `300` | Default session expiry (seconds) |
| `max_offline_queue_size` | int | `1000` | Max queued messages per offline client |
| `max_inflight_messages` | int | `100` | Max inflight messages per session |

### Log

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `level` | string | `info` | Log level: `debug`, `info`, `warn`, `error` |
| `format` | string | `text` | Log format: `text`, `json` |

### Storage

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `type` | string | `memory` | Storage backend: `memory` |

## Usage

### With configuration file:

```bash
./mqtt-broker -config config.yaml
```

### With defaults:

```bash
./mqtt-broker
```

## Environment Variables (Future)

Environment variables can override configuration:

- `MQTT_TCP_ADDR` - TCP listen address
- `MQTT_LOG_LEVEL` - Log level
- `MQTT_LOG_FORMAT` - Log format
- `MQTT_TLS_ENABLED` - Enable TLS (true/false)

## Example Configurations

### Production setup:

```yaml
server:
  tcp_addr: ":1883"
  tcp_max_connections: 50000
  tls_enabled: true
  tls_cert_file: "/etc/mqtt/cert.pem"
  tls_key_file: "/etc/mqtt/key.pem"

broker:
  max_message_size: 2097152  # 2MB
  max_retained_messages: 50000

session:
  max_sessions: 50000
  max_offline_queue_size: 5000

log:
  level: info
  format: json
```

### Development setup:

```yaml
server:
  tcp_addr: ":1883"

log:
  level: debug
  format: text
```
