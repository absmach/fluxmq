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

- Overview: node scope, live rates, 1/5/15-minute charts, protocol distribution.
- Sessions: search, protocol/state filters, sorting, pagination, centered detail dialog.
- Subscriptions: aggregated topic filters, QoS, subscriber counts, node scopes.
- Cluster: canvas node map with pan, zoom, fit, node search, and health drilldown.
  The map arranges configured nodes without implying network relationships.
  The node inventory provides an accessible table alternative.
- Health: broker reachability, uptime, cumulative error and message counters.
- Light/dark appearance persisted locally, responsive sidebar navigation, CSV
  export, live pause/resume, and visible reconnect states. Search stays within
  each data view; there is no global command palette. Documentation and
  appearance controls sit beside Export at the top of each page.

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
overflow. The canvas suite also checks 500 nodes, pan hit-testing, search,
keyboard controls, and empty/single-node layouts. Screenshots go to `/tmp/fluxmq-console-screenshots`; override with
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
and larger IBM Plex Mono metric values. All graphics are upright and session
details use a centered, responsive dialog. The sidebar is dedicated to navigation;
there are no accounts or workspace controls. Green denotes operational state.

The logo is editable SVG at `public/brand/fluxmq-symbol.svg`; the wordmark is
`public/brand/fluxmq-wordmark.svg` (IBM Plex Sans should be installed when editing).
The Abstract Machines logotype was sourced from https://www.absmach.eu/.
IBM Plex fonts were sourced through Google Fonts on 2026-11-22 and are hosted
locally. Their SIL Open Font Licenses are included in `public/fonts/`.
