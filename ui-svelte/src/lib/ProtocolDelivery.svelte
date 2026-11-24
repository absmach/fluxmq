<script lang="ts">
  import { number, protocolLabel, sessionProtocol, type Session } from './types';
  import Icon from './Icon.svelte';
  let {
    sessions,
    onselect,
    onqueues,
    partial = false,
    selected = 'all',
    selectedTransport = 'all',
  }: {
    sessions: Session[];
    onselect: (protocol: string, transport: string) => void;
    onqueues: () => void;
    partial?: boolean;
    selected?: string;
    selectedTransport?: string;
  } = $props();
  let rows = $derived.by(() => {
    const groups = new Map<
      string,
      {
        key: string;
        protocol: string;
        transport: string;
        label: string;
        mqtt: boolean;
        versions: Record<string, number>;
        connected: number;
        disconnected: number;
        inflight: number;
        queued: number;
      }
    >();
    for (const s of sessions) {
      const p = sessionProtocol(s);
      const transport = s.transport || 'unknown';
      const ws = transport === 'websocket';
      const key = ws ? 'websocket' : `${p}:${transport}`;
      const row = groups.get(key) ?? {
        key,
        protocol: ws ? 'all' : p,
        transport,
        label: ws ? 'WebSocket' : `${protocolLabel(p)}${transport === 'tcp' ? ' · TCP' : ''}`,
        mqtt: p.startsWith('mqtt'),
        versions: {} as Record<string, number>,
        connected: 0,
        disconnected: 0,
        inflight: 0,
        queued: 0,
      };
      row[s.connected ? 'connected' : 'disconnected']++;
      row.versions[p] = (row.versions[p] ?? 0) + 1;
      if (p.startsWith('mqtt')) {
        row.inflight += s.inflight_count;
        row.queued += s.offline_queue_depth;
      }
      groups.set(key, row);
    }
    return [...groups.values()].sort(
      (a, b) => b.connected + b.disconnected - (a.connected + a.disconnected),
    );
  });
</script>

<section class="protocol-delivery" aria-labelledby="protocol-delivery-title">
  <h2 id="protocol-delivery-title" class="sr-only">Protocols & delivery</h2>
  <div class="protocol-lanes">
    {#each rows as row (row.key)}
      {@const total = row.connected + row.disconnected}
      <button
        class="panel protocol-lane"
        class:selected={selected === row.protocol && selectedTransport === row.transport}
        aria-label={`Filter sessions by ${row.label}`}
        aria-pressed={selected === row.protocol && selectedTransport === row.transport}
        aria-describedby={!row.mqtt ? `delivery-note-${row.key}` : undefined}
        onclick={() => onselect(row.protocol, row.transport)}
      >
        <span class="protocol-heading">
          <span class="protocol-name">{row.label}</span>
          <span class="protocol-count">{number(total)} <small>clients</small></span>
        </span>
        <span
          class="protocol-bar"
          role="img"
          aria-label={`${row.label}: ${row.connected} connected, ${row.disconnected} disconnected`}
        >
          <i class="connected" style:width={`${(100 * row.connected) / total}%`}></i>
          <i
            class="disconnected"
            class:split={row.connected > 0}
            style:width={`${(100 * row.disconnected) / total}%`}
          ></i>
        </span>
        <span class="state-counts">
          <span class:has-connected={row.connected > 0}>{number(row.connected)} connected</span>
          <span class:has-disconnected={row.disconnected > 0}>
            {number(row.disconnected)} disconnected
          </span>
        </span>
        <span class="state-counts transport-counts">
          {#if row.transport === 'websocket'}
            {#each Object.entries(row.versions) as [version, count]}<span>
                {number(count)}
                {protocolLabel(version)}
              </span>{/each}
          {:else if row.transport === 'unknown'}<span>Transport not reported</span>{/if}
        </span>
        <span class="protocol-delivery-counts">
          {#if row.mqtt}
            <span title="MQTT inflight messages"><b>{number(row.inflight)}</b> inflight</span>
            <span title="MQTT offline session buffer">
              <b>{number(row.queued)}</b> offline queued
            </span>
          {:else}
            <span class="delivery-unavailable">
              Delivery <span aria-hidden="true">—</span><Icon name="info" size={12} />
              <span class="delivery-tooltip" role="tooltip" id={`delivery-note-${row.key}`}>
                Delivery counts are not exposed for {protocolLabel(row.protocol)}.
              </span>
            </span>
          {/if}
        </span>
      </button>
    {:else}<p class="empty-protocols muted">No clients in the current inventory.</p>{/each}
  </div>
  <p class="muted" style="font-size: 12px; margin: 10px 0">
    Session inventory includes MQTT over TCP/WebSocket and both AMQP versions. HTTP and CoAP publish
    traffic is shown on Overview and Broker health.
  </p>
  <div class="protocol-footer">
    {#if partial}<span>Loaded sessions · inventory incomplete</span>{/if}
    <button class="text-button" onclick={onqueues}>
      View queues<Icon name="chevron" size={12} />
    </button>
  </div>
</section>

<style>
  .protocol-delivery {
    margin-bottom: 20px;
  }
  .protocol-lanes {
    display: grid;
    grid-template-columns: repeat(auto-fit, minmax(240px, 1fr));
    gap: 16px;
  }
  .protocol-lane {
    display: flex;
    flex-direction: column;
    width: 100%;
    padding: 16px 18px;
    text-align: left;
    min-width: 0;
    overflow: visible;
    position: relative;
  }
  .protocol-lane:hover {
    background: var(--hover);
    border-color: var(--accent);
  }
  .protocol-lane.selected {
    border-color: var(--accent);
    background: var(--accent-bg);
  }
  .protocol-heading {
    display: flex;
    align-items: baseline;
    justify-content: space-between;
    gap: 12px;
    width: 100%;
    margin-bottom: 12px;
  }
  .protocol-name {
    font-size: 13px;
    font-weight: 500;
    color: var(--text);
  }
  .protocol-count {
    font: 400 18px var(--mono);
    color: var(--text);
    white-space: nowrap;
  }
  .protocol-count small {
    font:
      400 11px 'IBM Plex Sans',
      sans-serif;
    color: var(--muted);
  }
  .protocol-bar {
    display: flex;
    width: 100%;
    height: 8px;
    border-radius: 3px;
    overflow: hidden;
    background: var(--panel-raised);
  }
  .protocol-bar i {
    height: 100%;
    transition: width 0.3s;
  }
  .connected {
    background: var(--mint);
  }
  .disconnected {
    background:
      repeating-linear-gradient(135deg, transparent 0 4px, #00000020 4px 6px),
      var(--disconnected-color);
  }
  .disconnected.split {
    box-shadow: inset 2px 0 var(--panel);
  }
  .state-counts .has-connected {
    color: var(--mint);
  }
  .state-counts .has-disconnected {
    color: var(--disconnected-color);
    font-weight: 500;
  }
  .state-counts {
    display: flex;
    justify-content: space-between;
    gap: 12px;
    flex-wrap: wrap;
    width: 100%;
    margin-top: 7px;
    font-size: 11px;
    color: var(--muted);
  }
  .protocol-delivery-counts {
    display: flex;
    gap: 16px;
    margin-top: 12px;
    font-size: 11px;
    color: var(--muted);
  }
  .protocol-delivery-counts b {
    font: 400 12px var(--mono);
    color: var(--text);
    margin-right: 3px;
  }
  .delivery-unavailable {
    display: flex;
    align-items: center;
    gap: 6px;
  }
  .delivery-tooltip {
    display: none;
    position: absolute;
    left: 12px;
    right: 12px;
    bottom: calc(100% + 8px);
    padding: 10px 12px;
    border-radius: 6px;
    border: 1px solid var(--border);
    background: var(--panel);
    color: var(--text);
    box-shadow: var(--shadow);
    z-index: 10;
    line-height: 1.5;
    font-size: 12px;
  }
  .delivery-unavailable:hover .delivery-tooltip,
  .protocol-lane:focus-visible .delivery-tooltip {
    display: block;
  }
  .protocol-footer {
    display: flex;
    align-items: center;
    gap: 12px;
    margin-top: 8px;
    font-size: 11px;
    color: var(--muted);
  }
  .protocol-footer button {
    margin-left: auto;
    font-size: 11px;
  }
  .empty-protocols {
    margin: 0;
    padding: 16px;
  }
  @media (max-width: 600px) {
    .protocol-lanes {
      grid-template-columns: 1fr;
      gap: 10px;
    }
  }
</style>
