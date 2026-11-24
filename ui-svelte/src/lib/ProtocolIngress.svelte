<script lang="ts">
  import DiagnosticChart from './DiagnosticChart.svelte';
  import { compact, number, bytes, type BrokerNode, type Point } from './types';

  let { nodes, points = [] }: { nodes: BrokerNode[]; points?: Point[] } = $props();
  import { ingressProtocols as definitions } from './ingressProtocols';
  let range = $state(5);
  let hidden = $state<string[]>([]);
  function toggle(key: string) {
    hidden = hidden.includes(key) ? hidden.filter((item) => item !== key) : [...hidden, key];
  }
  let protocols = $derived(
    definitions.filter((p) => nodes.some((n) => n.ingress_metrics?.[p.key])),
  );
  let latest = $derived(points.at(-1)?.metrics);
  let visible = $derived(
    points.filter((p) => p.time >= (points.at(-1)?.time ?? 0) - range * 60000),
  );
  let series = $derived(
    protocols.map((p) => ({
      key: `ingress.${p.key}.publishes`,
      label: p.label,
      color: p.color,
    })),
  );
  const totals = (key: string) =>
    nodes.reduce(
      (sum, n) => {
        const counters = n.ingress_metrics?.[key];
        return {
          publishes: sum.publishes + (counters?.publishes ?? 0),
          bytes: sum.bytes + (counters?.payload_bytes ?? 0),
        };
      },
      { publishes: 0, bytes: 0 },
    );
</script>

{#if protocols.length}
  <section class="panel protocol-ingress" aria-label="Publish traffic by protocol">
    <div class="panel-heading">
      <div>
        <h2>Publish traffic by protocol</h2>
        <p class="muted">Incoming application messages · MQTT TCP and WebSocket shown separately</p>
      </div>
      <div class="range-control segmented" aria-label="Publish traffic history range">
        {#each [1, 5, 15] as minutes}
          <button
            class:chosen={range === minutes}
            aria-pressed={range === minutes}
            onclick={() => (range = minutes)}
          >
            {minutes}m
          </button>
        {/each}
      </div>
    </div>
    <div class="protocol-rates">
      {#each protocols as protocol}
        {@const key = `ingress.${protocol.key}.publishes`}
        {@const shown = !hidden.includes(key)}
        {@const rate = latest?.[key]}
        {@const total = totals(protocol.key)}
        {@const reporting = nodes.filter(
          (n) => !n.unmonitored && n.ingress_metrics?.[protocol.key],
        )}
        <article data-protocol={protocol.key}>
          <button
            class="protocol-filter"
            class:series-hidden={!shown}
            aria-label={`Toggle ${protocol.label}`}
            aria-pressed={shown}
            title={`${shown ? 'Hide' : 'Show'} ${protocol.label} in chart`}
            onclick={() => toggle(key)}
          >
            <span class="protocol-name">
              <i style:background={protocol.color}></i>{protocol.label}
            </span>
            <strong
              title={rate === undefined
                ? 'Waiting for consecutive healthy samples'
                : `${number(rate)} publishes per second`}
            >
              {rate === undefined ? '—' : compact(rate)}<small>msg/s</small>
            </strong>
            <span title={`${number(total.publishes)} received publishes`}>
              {compact(total.publishes)} received · {bytes(total.bytes)} payload
            </span>
            {#if reporting.length < nodes.length}<span>
                {reporting.length}/{nodes.length} nodes reporting
              </span>{/if}
            {#if reporting.some((n) => !n.healthy)}<span class="stale">
                Includes last-known totals
              </span>{/if}
            <span class="filter-state">{shown ? 'Shown in chart' : 'Hidden from chart'}</span>
          </button>
        </article>
      {/each}
    </div>
    <DiagnosticChart
      points={visible}
      {series}
      showReadout={false}
      bind:hidden
      label="Incoming publishes by protocol"
      unit="msg / s"
    />
    <p class="ingress-note">
      HTTP and CoAP publish into the broker without MQTT sessions. Rates count incoming publishes,
      including attempts that may fail; payload totals exclude protocol headers.
    </p>
  </section>
{/if}

<style>
  .protocol-ingress {
    min-width: 0;
  }
  .panel-heading {
    padding: 20px 22px 14px;
    gap: 16px;
    flex-wrap: wrap;
  }
  .panel-heading p {
    font-size: 12px;
    margin-top: 5px;
  }
  .protocol-rates {
    display: grid;
    grid-template-columns: repeat(6, minmax(0, 1fr));
    padding: 0 12px 18px;
    gap: 4px;
  }
  article {
    min-width: 0;
  }
  .protocol-filter {
    display: block;
    width: 100%;
    height: 100%;
    padding: 12px 10px;
    text-align: left;
    border: 1px solid var(--border);
    border-radius: 8px;
    background: var(--panel);
  }
  .protocol-filter:hover,
  .protocol-filter:focus-visible {
    border-color: var(--accent);
  }
  .protocol-filter:focus-visible {
    outline: 2px solid var(--accent);
    outline-offset: 2px;
  }
  .protocol-filter.series-hidden {
    background: var(--panel-raised);
  }
  .series-hidden .protocol-name {
    text-decoration: line-through;
    color: var(--muted);
  }
  .series-hidden strong {
    color: var(--muted);
  }
  .protocol-filter > .filter-state {
    margin-top: 8px;
    color: var(--accent);
  }
  .series-hidden > .filter-state {
    color: var(--muted);
  }
  .protocol-name {
    display: flex;
    align-items: center;
    gap: 7px;
    font-size: 12px;
    font-weight: 500;
    white-space: nowrap;
  }
  .protocol-name i {
    width: 7px;
    height: 7px;
    border-radius: 50%;
    flex-shrink: 0;
  }
  strong {
    display: flex;
    align-items: baseline;
    gap: 6px;
    margin: 10px 0 7px;
    font: 400 30px var(--mono);
  }
  strong small {
    font:
      400 11px 'IBM Plex Sans',
      sans-serif;
    color: var(--muted);
  }
  .protocol-filter > span:not(.protocol-name) {
    display: block;
    font-size: 11px;
    color: var(--muted);
    line-height: 1.6;
  }
  .protocol-filter > .stale {
    color: var(--warning);
  }
  .ingress-note {
    padding: 12px 22px 18px;
    color: var(--muted);
    font-size: 11px;
    line-height: 1.6;
  }
  @media (max-width: 1200px) {
    .protocol-rates {
      grid-template-columns: repeat(3, minmax(0, 1fr));
    }
  }
  @media (max-width: 600px) {
    .protocol-rates {
      grid-template-columns: repeat(2, minmax(0, 1fr));
    }
  }
</style>
