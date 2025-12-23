# Webhook System Documentation

## Overview

The MQTT broker provides a comprehensive webhook system for asynchronous event notifications. This enables integrations with analytics services, audit systems, monitoring platforms, and custom applications.

## Quick Start

### 1. Configuration

Create or update your `config.yaml`:

```yaml
webhook:
  enabled: true
  queue_size: 10000
  drop_policy: "oldest"
  workers: 5
  include_payload: false
  shutdown_timeout: 30s

  defaults:
    timeout: 5s
    retry:
      max_attempts: 3
      initial_interval: 1s
      max_interval: 30s
      multiplier: 2.0
    circuit_breaker:
      failure_threshold: 5
      reset_timeout: 60s

  endpoints:
    - name: "analytics-service"
      type: "http"
      url: "https://analytics.example.com/mqtt/events"
      events:
        - "message.published"
        - "client.connected"
      topic_filters:
        - "sensors/#"
      headers:
        Authorization: "Bearer your-token"
```

### 2. Start Broker

```bash
./mqtt-broker --config config.yaml
```

### 3. Receive Webhooks

Your webhook endpoint will receive HTTP POST requests with JSON payloads:

```json
{
  "event_type": "message.published",
  "event_id": "550e8400-e29b-41d4-a716-446655440000",
  "timestamp": "2025-12-23T10:30:00.123Z",
  "broker_id": "broker-1",
  "data": {
    "client_id": "publisher-1",
    "topic": "sensors/temperature",
    "qos": 1,
    "retained": false,
    "payload_size": 256
  }
}
```

## Event Types

| Event Type                    | Description                                  | Key Fields                                      |
|-------------------------------|----------------------------------------------|-------------------------------------------------|
| `client.connected`            | Client successfully connected                | clientID, protocol, cleanStart, keepAlive       |
| `client.disconnected`         | Client disconnected                          | clientID, reason, remoteAddr                    |
| `client.session_takeover`     | Session migrated between cluster nodes       | clientID, fromNode, toNode                      |
| `message.published`           | Message published to broker                  | clientID, topic, qos, retained, payloadSize     |
| `message.delivered`           | Message delivered to subscriber              | clientID, topic, qos, payloadSize               |
| `message.retained`            | Retained message set or cleared              | topic, payloadSize, cleared                     |
| `subscription.created`        | Client subscribed to topic                   | clientID, topicFilter, qos                      |
| `subscription.removed`        | Client unsubscribed from topic               | clientID, topicFilter                           |
| `auth.success`                | Authentication succeeded                     | clientID, remoteAddr                            |
| `auth.failure`                | Authentication failed                        | clientID, reason, remoteAddr                    |
| `authz.publish_denied`        | Publish authorization denied                 | clientID, topic, reason                         |
| `authz.subscribe_denied`      | Subscribe authorization denied               | clientID, topicFilter, reason                   |

## Filtering

### Event Type Filtering

Only send specific event types to an endpoint:

```yaml
endpoints:
  - name: "audit-system"
    url: "https://audit.example.com/events"
    events:
      - "auth.failure"
      - "authz.publish_denied"
      - "authz.subscribe_denied"
```

Empty `events` array = all event types.

### Topic Pattern Filtering

For message-related events, filter by MQTT topic patterns:

```yaml
endpoints:
  - name: "temperature-analytics"
    url: "https://temp-analytics.example.com/data"
    events:
      - "message.published"
    topic_filters:
      - "sensors/+/temperature"
      - "devices/#"
```

**MQTT Wildcards:**
- `+` - Single-level wildcard (e.g., `sensors/+/temp` matches `sensors/device1/temp`)
- `#` - Multi-level wildcard (e.g., `sensors/#` matches `sensors/temp` and `sensors/room1/temp`)

Empty `topic_filters` array = all topics.

## Configuration Reference

### Global Settings

| Field                | Type     | Default   | Description                                           |
|----------------------|----------|-----------|-------------------------------------------------------|
| `enabled`            | bool     | false     | Enable/disable webhook system                         |
| `queue_size`         | int      | 10000     | Max events in memory queue                            |
| `drop_policy`        | string   | "oldest"  | "oldest" or "newest" when queue is full               |
| `workers`            | int      | 5         | Number of concurrent worker goroutines                |
| `include_payload`    | bool     | false     | Include message payloads in events (base64 encoded)   |
| `shutdown_timeout`   | duration | 30s       | Graceful shutdown timeout                             |

### Default Settings

| Field                                  | Type     | Default | Description                              |
|----------------------------------------|----------|---------|------------------------------------------|
| `defaults.timeout`                     | duration | 5s      | HTTP request timeout                     |
| `defaults.retry.max_attempts`          | int      | 3       | Maximum retry attempts                   |
| `defaults.retry.initial_interval`      | duration | 1s      | Initial retry delay                      |
| `defaults.retry.max_interval`          | duration | 30s     | Maximum retry delay                      |
| `defaults.retry.multiplier`            | float64  | 2.0     | Backoff multiplier (exponential)         |
| `defaults.circuit_breaker.failure_threshold` | int | 5    | Consecutive failures before opening      |
| `defaults.circuit_breaker.reset_timeout`     | duration | 60s | Time in open state before half-open  |

### Endpoint Settings

| Field            | Type              | Required | Description                                   |
|------------------|-------------------|----------|-----------------------------------------------|
| `name`           | string            | Yes      | Unique endpoint identifier                    |
| `type`           | string            | Yes      | "http" (gRPC support planned)                 |
| `url`            | string            | Yes      | Webhook URL                                   |
| `events`         | []string          | No       | Event type filter (empty = all)               |
| `topic_filters`  | []string          | No       | Topic pattern filter (empty = all)            |
| `headers`        | map[string]string | No       | Custom HTTP headers                           |
| `timeout`        | duration          | No       | Override default timeout                      |
| `retry`          | RetryConfig       | No       | Override default retry config                 |

## Resilience

### Circuit Breaker

The circuit breaker prevents cascading failures by failing fast when an endpoint is unhealthy.

**States:**
- **Closed**: Normal operation, all requests sent
- **Open**: Endpoint failing, requests fail immediately (no network calls)
- **Half-Open**: Testing if endpoint recovered

**Thresholds:**
- 5 consecutive failures → Open circuit
- 60 seconds in Open → try Half-Open
- Success in Half-Open → Close circuit

### Retry Strategy

Exponential backoff with configurable parameters:

```
Attempt 1: immediate
Attempt 2: 1s delay
Attempt 3: 2s delay
Attempt 4: 4s delay
Attempt 5: 8s delay
Max cap:   30s
```

After max retries exhausted, the event is logged and dropped.

### Queue Overflow

When the event queue is full:

**Drop Policy: "oldest"** (default)
- Removes oldest event from queue
- Adds new event
- Best for: Real-time monitoring (latest state matters)

**Drop Policy: "newest"**
- Keeps existing queue
- Drops incoming event
- Best for: Audit logs (chronological order matters)

## Performance

### Overhead

| Scenario                           | Overhead  | Notes                         |
|------------------------------------|-----------|-------------------------------|
| Webhooks disabled                  | 0%        | No-op, zero cost              |
| Webhooks enabled, queue not full   | < 0.1%    | Single channel send           |
| Webhooks enabled, queue full       | < 0.5%    | Drop oldest + enqueue         |
| 1M messages/sec with webhooks      | ~0.2%     | Workers process async         |

**Key Insight**: Non-blocking design ensures broker performance is unaffected.

### Scalability

- **Queue size**: Adjust based on expected event rate
  - Low traffic (< 1k events/sec): 1,000 - 5,000
  - Medium traffic (1k - 10k events/sec): 10,000 - 50,000
  - High traffic (> 10k events/sec): 50,000 - 100,000

- **Worker count**: More workers = higher throughput
  - 1-2 endpoints: 2-5 workers
  - 3-5 endpoints: 5-10 workers
  - 5+ endpoints: 10-20 workers

## Monitoring

### Logs

Webhook system emits structured logs:

```json
{
  "level": "info",
  "msg": "webhook notifier started",
  "workers": 5,
  "queue_size": 10000,
  "endpoints": 2
}

{
  "level": "debug",
  "msg": "webhook delivered successfully",
  "endpoint": "analytics-service",
  "event_type": "message.published"
}

{
  "level": "error",
  "msg": "webhook delivery failed after max retries",
  "endpoint": "audit-system",
  "event_type": "auth.failure",
  "attempts": 3,
  "error": "context deadline exceeded"
}

{
  "level": "warn",
  "msg": "webhook circuit breaker state changed",
  "endpoint": "analytics-service",
  "from": "closed",
  "to": "open"
}
```

### Metrics (Future)

Planned Prometheus metrics:

- `mqtt_webhook_events_total{endpoint, event_type, status}` - Total events
- `mqtt_webhook_queue_depth` - Current queue depth
- `mqtt_webhook_duration_seconds{endpoint}` - Delivery latency histogram
- `mqtt_webhook_circuit_breaker_state{endpoint}` - Circuit breaker state (0=closed, 1=open, 2=half-open)

## Security

### Authentication

Use custom headers for authentication:

```yaml
headers:
  Authorization: "Bearer secret-token-xyz"
  X-API-Key: "your-api-key"
```

### TLS/HTTPS

Always use HTTPS URLs in production:

```yaml
url: "https://secure-endpoint.example.com/events"
```

### Secrets Management

**Don't hardcode secrets** in config files. Use environment variables:

```yaml
headers:
  Authorization: "${WEBHOOK_AUTH_TOKEN}"
```

Or use secrets management systems (HashiCorp Vault, AWS Secrets Manager, etc.).

## Troubleshooting

### Webhook not firing

1. Check if webhooks are enabled: `webhook.enabled: true`
2. Verify endpoint configuration (name, URL, type)
3. Check event type filter matches expected events
4. Check topic filter matches message topics
5. Review logs for errors

### High latency

1. Check webhook endpoint response times
2. Increase worker count: `workers: 10`
3. Increase timeout if endpoint is slow: `timeout: 30s`
4. Check circuit breaker state (may be failing fast)

### Events being dropped

1. Check queue size vs event rate
2. Increase queue size: `queue_size: 50000`
3. Review drop policy: consider switching to "newest"
4. Check logs for overflow messages
5. Reduce retry attempts to fail faster

### Circuit breaker frequently opening

1. Check webhook endpoint health
2. Increase timeout if endpoint needs more time
3. Increase failure threshold: `failure_threshold: 10`
4. Check network connectivity
5. Review endpoint logs for errors

## Examples

### Analytics Integration

Track all published messages:

```yaml
endpoints:
  - name: "message-analytics"
    type: "http"
    url: "https://analytics.example.com/mqtt/messages"
    events:
      - "message.published"
    headers:
      Authorization: "Bearer analytics-token"
```

### Security Audit Log

Track authentication and authorization events:

```yaml
endpoints:
  - name: "security-audit"
    type: "http"
    url: "https://audit.example.com/security/events"
    events:
      - "auth.success"
      - "auth.failure"
      - "authz.publish_denied"
      - "authz.subscribe_denied"
    headers:
      Authorization: "Bearer audit-token"
```

### IoT Device Monitoring

Track specific device connections:

```yaml
endpoints:
  - name: "device-monitoring"
    type: "http"
    url: "https://iot-monitor.example.com/devices/events"
    events:
      - "client.connected"
      - "client.disconnected"
      - "message.published"
    topic_filters:
      - "devices/+/telemetry"
      - "devices/+/status"
    headers:
      X-API-Key: "iot-monitor-key"
```

### Multi-Endpoint Setup

Send different events to different services:

```yaml
endpoints:
  # Analytics - all messages
  - name: "analytics"
    url: "https://analytics.example.com/events"
    events: ["message.published", "message.delivered"]

  # Audit - security events
  - name: "audit"
    url: "https://audit.example.com/events"
    events: ["auth.failure", "authz.publish_denied"]

  # Monitoring - connection events
  - name: "monitoring"
    url: "https://monitoring.example.com/events"
    events: ["client.connected", "client.disconnected"]
```

## Architecture

### Component Diagram

```
Broker → Events → Notifier (Queue + Workers) → Sender (HTTP/gRPC) → External Service
```

### Design Principles

1. **Protocol-Agnostic**: Sender interface allows any protocol
2. **Non-Blocking**: `Notify()` returns immediately, no broker impact
3. **Resilient**: Circuit breaker, retry, graceful degradation
4. **Testable**: Clean separation, comprehensive test coverage
5. **Extensible**: Easy to add gRPC, AMQP, Kafka, etc.

### Adding gRPC Support (Future)

Implement the `Sender` interface:

```go
type GRPCSender struct {
    conn *grpc.ClientConn
}

func (s *GRPCSender) Send(ctx context.Context, url string,
    headers map[string]string, payload []byte, timeout time.Duration) error {
    // gRPC-specific implementation
}
```

Then use in `main.go`:

```go
sender := webhook.NewGRPCSender(config)
notifier := webhook.NewNotifier(cfg.Webhook, nodeID, sender, logger)
```

## API Specification

### Webhook Endpoint Requirements

Your webhook endpoint must:

1. Accept HTTP POST requests
2. Accept `Content-Type: application/json`
3. Return 2xx status code for success
4. Respond within configured timeout (default: 5s)
5. Handle duplicate events idempotently (retries may cause duplicates)

### Example Webhook Handler (Go)

```go
func webhookHandler(w http.ResponseWriter, r *http.Request) {
    var envelope struct {
        EventType string          `json:"event_type"`
        EventID   string          `json:"event_id"`
        Timestamp string          `json:"timestamp"`
        BrokerID  string          `json:"broker_id"`
        Data      json.RawMessage `json:"data"`
    }

    if err := json.NewDecoder(r.Body).Decode(&envelope); err != nil {
        http.Error(w, "Invalid JSON", http.StatusBadRequest)
        return
    }

    // Process event based on type
    switch envelope.EventType {
    case "client.connected":
        // Handle connection event
    case "message.published":
        // Handle message event
    default:
        // Unknown event type
    }

    w.WriteHeader(http.StatusOK)
}
```

### Example Webhook Handler (Python/Flask)

```python
from flask import Flask, request, jsonify

app = Flask(__name__)

@app.route('/mqtt/events', methods=['POST'])
def webhook():
    envelope = request.json
    event_type = envelope['event_type']
    data = envelope['data']

    if event_type == 'client.connected':
        # Handle connection event
        client_id = data['client_id']
        print(f"Client {client_id} connected")

    elif event_type == 'message.published':
        # Handle message event
        topic = data['topic']
        print(f"Message published to {topic}")

    return jsonify({"status": "ok"}), 200
```

## Testing

### Test Your Webhook Endpoint

Use curl to simulate webhook delivery:

```bash
curl -X POST https://your-endpoint.com/events \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer your-token" \
  -d '{
    "event_type": "message.published",
    "event_id": "test-123",
    "timestamp": "2025-12-23T10:00:00Z",
    "broker_id": "test-broker",
    "data": {
      "client_id": "test-client",
      "topic": "test/topic",
      "qos": 1,
      "retained": false,
      "payload_size": 100
    }
  }'
```

### Mock Webhook Server

For testing, use httpbin.org:

```yaml
endpoints:
  - name: "test-endpoint"
    url: "https://httpbin.org/post"
    events: ["message.published"]
```

Visit https://httpbin.org to see received requests.

## Support

For questions, issues, or feature requests:

- GitHub Issues: https://github.com/absmach/mqtt/issues
- Documentation: https://github.com/absmach/mqtt/tree/main/docs

## License

Copyright (c) Abstract Machines
SPDX-License-Identifier: Apache-2.0
