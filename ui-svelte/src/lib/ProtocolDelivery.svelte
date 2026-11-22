<script lang="ts">
  import { number, protocolLabel, sessionProtocol, type Session } from './types';
  import Icon from './Icon.svelte';
  let {
    sessions,
    onselect,
    onqueues,
    partial = false,
  }: {
    sessions: Session[];
    onselect: (protocol: string) => void;
    onqueues: () => void;
    partial?: boolean;
  } = $props();
  let rows = $derived.by(() => {
    const groups = new Map<
      string,
      {
        protocol: string;
        connected: number;
        disconnected: number;
        inflight: number;
        queued: number;
      }
    >();
    for (const s of sessions) {
      const p = sessionProtocol(s);
      const row = groups.get(p) ?? {
        protocol: p,
        connected: 0,
        disconnected: 0,
        inflight: 0,
        queued: 0,
      };
      row[s.connected ? 'connected' : 'disconnected']++;
      if (p.startsWith('mqtt')) {
        row.inflight += s.inflight_count;
        row.queued += s.offline_queue_depth;
      }
      groups.set(p, row);
    }
    return [...groups.values()].sort(
      (a, b) => b.connected + b.disconnected - (a.connected + a.disconnected),
    );
  });
  let max = $derived(Math.max(1, ...rows.map((r) => r.connected + r.disconnected)));
</script>

<section class="panel protocol-delivery" aria-labelledby="protocol-delivery-title">
  <div class="panel-heading">
    <h2 id="protocol-delivery-title">Protocols & delivery</h2>
    <span class="muted">{partial ? 'Loaded inventory' : 'Listed inventory'}</span>
  </div>
  <div class="protocol-legend">
    <span><i class="connected"></i>Connected</span><span>
      <i class="disconnected"></i>Disconnected
    </span><button class="text-button" onclick={onqueues}>
      Inspect durable queues <Icon name="chevron" size={13} />
    </button>
  </div>
  <div class="protocol-lanes">
    {#each rows as row}<div class="protocol-lane">
        <button class="text-button protocol-name" onclick={() => onselect(row.protocol)}>
          {protocolLabel(row.protocol)}<Icon name="chevron" size={13} />
        </button>
        <div class="protocol-state">
          <div
            class="protocol-bar"
            role="img"
            aria-label={`${protocolLabel(row.protocol)}: ${row.connected} connected, ${row.disconnected} disconnected`}
          >
            <i class="connected" style:width={`${(100 * row.connected) / max}%`}></i><i
              class="disconnected"
              style:width={`${(100 * row.disconnected) / max}%`}
            ></i>
          </div>
          <div class="state-counts">
            <span>{number(row.connected)} connected</span><span>
              {number(row.disconnected)} disconnected
            </span>
          </div>
        </div>
        <span class="protocol-count">
          {number(row.connected + row.disconnected)} <small>clients</small>
        </span>
        <div class="protocol-delivery-counts">
          {#if row.protocol.startsWith('mqtt')}<span title="MQTT inflight messages">
              {number(row.inflight)} <small>inflight</small>
            </span><span title="MQTT offline session buffer">
              {number(row.queued)} <small>offline queued</small>
            </span>{:else}<span class="muted">Delivery counts not exposed</span>{/if}
        </div>
      </div>{:else}<p class="muted">No clients in the current inventory.</p>{/each}
  </div>
</section>

<style>
  .protocol-delivery {
    margin-bottom: 24px;
  }
  .protocol-legend {
    display: flex;
    gap: 20px;
    padding: 0 24px 20px;
    font-size: 12px;
    color: var(--muted);
    align-items: center;
    flex-wrap: wrap;
  }
  .protocol-legend > span {
    display: flex;
    gap: 6px;
    align-items: center;
  }
  .protocol-legend i {
    width: 8px;
    height: 8px;
    border-radius: 2px;
  }
  .protocol-legend button {
    margin-left: auto;
  }
  .connected {
    background: var(--accent);
  }
  .disconnected {
    background:
      repeating-linear-gradient(135deg, transparent 0 3px, #ffffff40 3px 5px), var(--violet);
  }
  .protocol-lanes {
    padding: 0 24px 16px;
  }
  .protocol-lane {
    display: grid;
    grid-template-columns: 130px minmax(80px, 1fr) 90px 240px;
    gap: 20px;
    align-items: center;
    padding: 16px 0;
    border-top: 1px solid var(--subtle-border);
    font-size: 13px;
  }
  .protocol-name {
    justify-content: space-between;
  }
  .state-counts {
    display: flex;
    gap: 14px;
    flex-wrap: wrap;
    font-size: 11px;
    color: var(--muted);
    margin-top: 6px;
  }
  .protocol-state {
    min-width: 0;
  }
  .protocol-bar {
    display: flex;
    height: 10px;
    border-radius: 3px;
    overflow: hidden;
    background: var(--panel-raised);
  }
  .protocol-bar i {
    height: 100%;
    transition: width 0.3s;
  }
  .protocol-count {
    font-family: 'IBM Plex Mono', monospace;
    text-align: right;
  }
  .protocol-count small,
  .protocol-delivery-counts small {
    color: var(--muted);
    font-family: 'IBM Plex Sans', sans-serif;
    font-size: 11px;
  }
  .protocol-delivery-counts {
    display: flex;
    justify-content: flex-end;
    gap: 20px;
    font-family: 'IBM Plex Mono', monospace;
    font-size: 13px;
  }
  .protocol-delivery-counts > .muted {
    font-family: 'IBM Plex Sans', sans-serif;
    font-size: 11px;
  }
  @media (max-width: 1100px) {
    .protocol-lane {
      grid-template-columns: 120px 1fr 80px;
    }
    .protocol-delivery-counts {
      grid-column: 2/4;
      justify-content: flex-end;
    }
  }
  @media (max-width: 600px) {
    .protocol-lane {
      gap: 12px;
      grid-template-columns: 105px 1fr 70px;
    }
    .protocol-legend button {
      margin-left: 0;
    }
  }
</style>
