# FluxMQ Console

A new FluxMQ identity and management console, built with Svelte 5, TypeScript,
and Vite. The frontend is compiled into static files and embedded in a small Go
server. There is no Node.js runtime in production, no external frontend service,
and no charting or component library. Go uses only the standard library.

This is an independent console binary beside the Go broker, not yet part of the
broker executable. The existing Next.js UI remains in `../ui` for comparison.

## Run the preview

For real data, use the [live broker deployment](../deployments/console/README.md).
It builds this console and the current backend together, with standalone and
three-node configurations and a real MQTT/AMQP browser regression test.

Requires Node.js 22.12+ and Go 1.23+.

```sh
cd ui-svelte
make build
./fluxmq-console -demo
```

Open http://localhost:3002/dashboard. Demo mode is explicit and visibly labeled.
It simulates three nodes, moving traffic, 36 example sessions, and topic filters.
The summary metrics represent the larger simulated workload; the session and
subscription tables contain a representative sample. It never connects to or
modifies a broker. The server defaults to loopback.

For real brokers, omit `-demo` and provide their trusted Admin API origins:

```sh
FLUXMQ_NODE_URLS=http://127.0.0.1:9081,http://127.0.0.1:9082,http://127.0.0.1:9083 \
  ./fluxmq-console
```

`FLUXMQ_API_URL` is the single-node fallback. URLs are configured at startup;
the browser cannot change the upstream destination. This console is read-only.
It does not add authentication to the privileged Admin API: keep the console
local or behind your existing authenticated management gateway.

## Views and interactions

- Overview: explicit cluster/node scope, per-node load comparison, message/bandwidth/connection history,
  1/5/15-minute ranges, always-visible cumulative counters with directional icons and exact-value tooltips, retained messages, protocol distribution.
- Publish traffic by protocol on Overview: separate measured incoming
  publish rates, payload totals and history for MQTT TCP, MQTT WebSocket, HTTP,
  CoAP and both AMQP versions. This is independent of session inventory.
- Sessions: per-node distribution/filtering, search by ID/name/node, protocol/state filters, sorting, configurable
  pagination, TCP/WebSocket transport counts and filtering, and a client detail
  page with connection details, transport, MQTT settings, and
  subscriptions or AMQP consumers.
- Subscriptions: aggregated topic filters, QoS, subscriber counts, per-node distribution/filtering,
  and a detail page with node filtering, client search, pagination, retry/load-more, and links to client details.
- Queues: durable queue inventory; queue-depth and ACK/NACK/reject/dead-letter rate charts;
  capture failures/drops; pending and poison-message gauges; consumer-group progress;
  pending delivery offsets, consumers, attempts and heartbeats; read-only payload,
  key and header inspection; routing, retention and replication-group configuration.
- Cluster: Activity/Counters comparison with leader status, reachability, connections, throughput,
  uptime, and a dedicated node drawer with metadata, counters, errors, and links
  to that node’s dashboard, clients, and subscriptions. Inventory includes configured nodes and reported cluster peers;
  inspecting a node does not change scope or navigate away.
- Health: broker reachability, uptime, cumulative message counters, and separate
  protocol/authentication/authorization/packet error counts, queue diagnostics,
  and protocol traffic/bandwidth/connection/error charts with expandable internals
  (MQTT PUBLISH, AMQP channels/consumers and local-principal
  authentication, authorization and reload results when reported).
- Light/dark appearance persisted locally, responsive sidebar navigation, CSV
  export, live pause/resume, and visible reconnect states. Search stays within
  each data view; there is no global command palette. Documentation and
  appearance controls are labeled items at the bottom of the sidebar. A sticky
  status bar keeps cluster-wide reachability, total node count, scope, and stream
  controls visible on every page. Summary cards share the overview number scale.

The charts use lightweight SVG. All fonts and brand assets are served locally.
API strings are rendered as text, never injected as HTML.

## Streaming and operational limits

One shared collector polls each configured broker's overview every two seconds
after the previous collection completes. It refreshes session/subscription
inventory every five cycles. All browser clients share this collector through
Server-Sent Events; opening another tab does not add overview polling. On-demand
inventory and queue inspection requests are per browser.

The initial SSE event includes the in-memory history. Subsequent events include
the newest sample. Reconnection supplies a fresh snapshot; resuming after a pause
also restores the history. Rates use actual elapsed time and clamp counter
resets. Failed nodes retain their last values, are marked unavailable, and are
excluded from current throughput. The console retains at most 450 samples per
scope (about 15 minutes at the normal cadence); history is not durable and starts
empty for real brokers. Demo mode seeds simulated history.

The shared SSE snapshot samples at most 200 sessions and 200 subscription filters
per node to bound background collection and stream size. Sessions and Subscriptions
use a separate paginated endpoint: one bounded 200-record page per node per request.
Normal session browsing continues on demand; search and protocol/queue-depth sorting
traverse all pages. Subscription aggregation also continues across pages. Incomplete
counts are labeled as loaded. Query changes cancel requests; partial failures remain
retryable. Active inventories refresh every 20 seconds and pause with the console.
A six-query cache preserves loaded results on Back navigation.

Reported cluster peers without configured admin endpoints appear as **Not monitored**.
Their unavailable metrics are not rendered as zero. The console never probes discovered
transport addresses; configure their admin URLs to monitor them.

Queue details refresh metadata, log bounds and groups every ten seconds while live.
Message reads and pending inspection are explicit snapshots. The adapter allowlists
six read-only Connect RPCs, accepts only configured node IDs, and never calls Consume,
Ack, Nack or cursor mutations. Queue listings use 100-item pages; message reads use
20 records with an 8 MiB response ceiling and 16 KiB payload previews. Oversized
responses fail explicitly. The current broker ignores ReadBatch.max_bytes, so the
ceiling limits the console response, not work already performed in the broker.
Group/pending RPCs return entire lists, subject to the same response ceiling.
Decimal strings and browser BigInt preserve uint64 offsets.

Queue and protocol charts use the shared 450-sample history, including reconnect
replay and 1/5/15-minute ranges. Gauge values (pending entries and connections) are
sampled directly. Rates use consecutive healthy-node counter differences divided
by actual elapsed time. First samples, counter resets and unavailable-node intervals
produce gaps. Live history is never synthesized; demo counters are generated from
integrated simulation curves. Detailed counters and node/protocol tables remain
available in collapsed sections below the plots. Compact plots overlay all series by
default; the Series menu can isolate a metric or toggle any combination, and the
legend also toggles visibility. Queue counts and event rates have separately
labeled axes. Queue rates default to a zero-preserving logarithmic scale (log1p),
so low retry/error rates remain visible next to high ACK throughput. The Rates
control restores a linear scale; counts always remain linear and tooltips retain
actual measured values. Signals too small to read at the shared scale also appear
in a compact, labeled linear detail plot; this applies to either mode and never
rescales a signal silently. Zero samples are identified in the legend.
`npm run test:filters` exhaustively checks every series subset and ACK transition
across node scopes and time ranges, plus missing data and live refresh. Protocols overlay within a selected metric (messages, bandwidth,
connections or errors), keeping units consistent.

Subscription clients are fetched on demand from the broker's dedicated endpoint,
200 per node per request. Continuation tokens support loading further pages;
partial failures remain visible and retryable. Clients are identified by both
node ID and client ID, and detail lookup works beyond the sampled session list.
Demo topic counts are derived from the connected sample sessions so drilldown
counts agree with the displayed subscription inventory.

Sidebar navigation starts at cluster scope. Explicit node actions open that node’s
dashboard, clients, or subscriptions. Node scope is encoded in the URL; a node
view identifies its scope beside the heading and offers a return to all nodes.
Health shows cluster availability at cluster scope and node uptime at node scope.

## Previous UI coverage audit

Compared the Next.js dashboard, node table, broker-info page, sessions page and
session detail dialog, subscriptions directory, and subscription-client page.

| Previous capability                                             | Current coverage                                                                                                                  |
| --------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------- |
| Message, directional bandwidth, and active-session trends       | Restored as selectable history metrics with the same time window                                                                  |
| Message/byte totals, retained messages, uptime                  | Overview cumulative counters and per-node details; cluster uptime is explicitly the shortest node uptime                          |
| Node address, role, counters, uptime                            | Dedicated drawer; cluster address and version are retained when reported                                                          |
| Protocol, authentication, authorization, packet errors          | Separate health counters and per-node detail fields                                                                               |
| Client connection name, protocol, timestamps, MQTT settings     | Preserved through the Go adapter and shown on the client page; AMQP uses consumer-specific presentation                           |
| Client subscriptions and QoS                                    | Shown with available subscription options and a link to connected clients                                                         |
| Subscription → clients → client details                         | Restored, including direct lookup outside the sampled inventory, multi-node identity, partial-failure reporting, and continuation |
| Search, state/scope filtering, page size, first/last navigation | Available on relevant tables; node inspection no longer mutates scope                                                             |
| Broker-info and subscription-detail bookmarks                   | Legacy paths continue to open the corresponding page                                                                              |
| Remove Selected                                                 | The old button had no action handler; this console remains read-only                                                              |

Large-inventory continuation and reported cluster membership now cover the two
previous gaps. MQTT inflight and offline-buffer counts share a Delivery column;
AMQP shows unavailable rather than invented zeros. Node cumulative counters have
an optional comparison view. The previous Remove Selected button had no handler.

## Operator capability audit

The console follows implemented handlers, not just protobuf declarations:

| Operator question                                 | Code and presentation                                                                                                                               | API limits                                                                             |
| ------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------- |
| Which nodes are unavailable or unmonitored?       | `server/api/cluster.go`: membership, role, address, version, uptime; node inventory and inspector                                                   | No consensus term, quorum, replication lag or election history                         |
| Where are MQTT messages waiting?                  | `server/api/sessions.go`: offline/inflight counts, expiry, receive limits, Will, subscriptions; protocol lanes and client pages                     | No buffer payload peek, per-packet ACK history, client IP, username or TLS metadata    |
| What is stored in durable queues?                 | `server/queue/handler.go`: GetQueue, GetQueueInfo, ReadBatch; message payload/key/header browser                                                    | Reader omits originating protocol and original topic; neither is inferred              |
| Which groups are behind or awaiting ACK?          | ListConsumerGroups/GetPending; offset tracks, pending ownership, delivery attempts, heartbeats                                                      | Group mode/protocol absent; no invented active/stalled classification                  |
| Are messages being dropped or repeatedly failing? | `server/api/stats.go`: capture failures/drops, ACK/NACK/reject/DLQ, DLQ transfer failures, poison-without-DLQ counters and poison gauges            | Counters are cumulative observations, not an event log                                 |
| How do protocols differ?                          | MQTT connection/PUBLISH/error counters; AMQP 0.9.1 channel/consumer/local-principal counters; sessions distinguish MQTT versions and AMQP 1.0/0.9.1 | Stats handler has no separate AMQP 1.0, HTTP or CoAP section                           |
| Can operators compare nodes?                      | Activity/Counters view, node dashboards, queue diagnostic comparison and protocol breakdown                                                         | Node-local counters may overlap across replicated copies; not a unique logical backlog |

`queue/types/log.go` defines committed as the oldest unacknowledged offset (safe
truncation point), and cursor as the next delivery offset. The graphic follows that
implementation. Uncommitted ranges may contain out-of-order acknowledgements;
range width is not the pending count. Acknowledgement does not remove retained log
records. Stream groups may advance without pending entries. Consumer-group IDs
are not assumed to equal client IDs. MQTT offline buffers remain distinct from
durable queues.

Useful backend additions: bounded MQTT offline-message peek; AMQP 1.0 connection
and settlement diagnostics; explicit consumer-group mode/protocol metadata; and a
structured event/alert feed correlated with nodes and messages. These are not
fabricated from aggregate counters.

## Development

Run the Go demo server, then `npm run dev`. Vite proxies `/console-api` to the
server on port 3002. The Go binary must be rebuilt after frontend changes for
production previews because its assets are embedded.

```sh
npm run check
npm run build
go test -race ./...
```

With the demo console running, `npm run test:browser` runs the Chromium smoke
suite. Install its browser once with `npx playwright install chromium` if it is
not already available. It checks streaming updates, pause/resume, navigation,
filters, session details, pagination, CSV exports, theme persistence, and mobile
overflow. Operator tests cover queue/pending inspection, exact uint64 offsets,
escaped payloads, inventory beyond 200 records, and protocol visuals. Screenshots go to `/tmp/fluxmq-console-screenshots`; override with
`FLUXMQ_SCREENSHOT_DIR`. `FLUXMQ_TEST_URL` selects a different preview origin.

Build the assets before Go commands: `server.go` embeds `dist/`.

```sh
docker build -t fluxmq-console:preview ui-svelte  # from repository root
docker run --rm -p 127.0.0.1:3002:3002 fluxmq-console:preview \
  -demo -listen 0.0.0.0:3002
```

## Identity

FluxMQ uses a message-routing symbol and the exact product name **FluxMQ**.
Graphite surfaces and cool blue accents accompany IBM Plex Sans interface text
and larger IBM Plex Mono metric values. All graphics are upright. Client and subscription details use full pages with
addressable URLs and return navigation that preserves filters, pagination, and
scroll position. Node inspection uses a responsive side drawer. The sidebar is dedicated to navigation;
there are no accounts or workspace controls. Green denotes operational state.

The logo is editable SVG at `public/brand/fluxmq-symbol.svg`; the wordmark is
`public/brand/fluxmq-wordmark.svg` (IBM Plex Sans should be installed when editing).
The Abstract Machines logotype was sourced from https://www.absmach.eu/.
IBM Plex fonts were sourced through Google Fonts on 2026-11-22 and are hosted
locally. Their SIL Open Font Licenses are included in `public/fonts/`.
