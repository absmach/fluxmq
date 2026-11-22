# FluxMQ Console

A new FluxMQ identity and management console, built with Svelte 5, TypeScript,
and Vite. The frontend is compiled into static files and embedded in a small Go
server. There is no Node.js runtime in production, no external frontend service,
and no charting or component library. Go uses only the standard library.

This is an independent console binary beside the Go broker, not yet part of the
broker executable. The existing Next.js UI remains in `../ui` for comparison.

## Run the preview

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
  1/5/15-minute ranges, cumulative counters, retained messages, protocol distribution.
- Sessions: per-node distribution/filtering, search by ID/name/node, protocol/state filters, sorting, configurable
  pagination, and a client detail page with connection details, MQTT settings, and
  subscriptions or AMQP consumers.
- Subscriptions: aggregated topic filters, QoS, subscriber counts, per-node distribution/filtering,
  and a detail page with node filtering, client search, pagination, retry/load-more, and links to client details.
- Cluster: node table with leader status, reachability, connections, throughput,
  uptime, and a dedicated node drawer with metadata, counters, errors, and links
  to that node’s dashboard, clients, and subscriptions. Inventory always shows every configured node;
  inspecting a node does not change scope or navigate away.
- Health: broker reachability, uptime, cumulative message counters, and separate
  protocol/authentication/authorization/packet error counts.
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
Server-Sent Events; opening another tab does not add broker polling.

The initial SSE event includes the in-memory history. Subsequent events include
the newest sample. Reconnection supplies a fresh snapshot; resuming after a pause
also restores the history. Rates use actual elapsed time and clamp counter
resets. Failed nodes retain their last values, are marked unavailable, and are
excluded from current throughput. The console retains at most 450 samples per
scope (about 15 minutes at the normal cadence); history is not durable and starts
empty for real brokers. Demo mode seeds simulated history.

Inventory is capped at 200 sessions and 200 subscription filters per node to
bound collection work and stream size. A truncation notice is shown when the
API returns a continuation token. Pagination/filtering applies to the collected
inventory. A cluster-wide paginated inventory API and durable metrics history
are future work, not implied by this preview. Only configured node URLs are
collected; cluster membership is not automatically discovered.

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

Two differences remain: the shared session/topic snapshot has the 200-record
per-node cap described above (the old proxy fetched all pages), and the node
inventory contains configured admin endpoints (the old overview also listed
reported cluster peers without configured endpoints, with missing metrics).
Configure each broker's admin URL to monitor it. These are explicit limits,
not a claim of complete large-cluster inventory parity. Client IP addresses,
usernames, and transport/TLS metadata are not exposed by the current session API
and are not invented in the UI.

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
overflow. Screenshots go to `/tmp/fluxmq-console-screenshots`; override with
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
