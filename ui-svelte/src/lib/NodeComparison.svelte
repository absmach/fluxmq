<script lang="ts">
  import { number, compact, type BrokerNode, type Session, type Subscription } from './types';
  import Icon from './Icon.svelte';
  let {
    nodes,
    sessions = [],
    subscriptions = [],
    kind = 'traffic',
    onselect,
  }: {
    nodes: BrokerNode[];
    sessions?: Session[];
    subscriptions?: Subscription[];
    kind?: 'traffic' | 'sessions' | 'subscriptions';
    onselect: (id: string) => void;
  } = $props();
  let metric = $state<'connections' | 'incoming' | 'outgoing'>('connections');
  let rows = $derived(
    nodes.map((node) => ({
      ...node,
      value:
        kind === 'sessions'
          ? sessions.filter((s) => s.node_id === node.id).length
          : kind === 'subscriptions'
            ? subscriptions
                .filter((s) => s.node_id === node.id)
                .reduce((n, s) => n + s.subscriber_count, 0)
            : node.healthy
              ? node[metric]
              : 0,
    })),
  );
  let total = $derived(rows.reduce((n, row) => n + row.value, 0));
</script>

<section
  class="panel node-comparison"
  class:compact-comparison={kind !== 'traffic'}
  aria-label={kind === 'traffic'
    ? 'Node comparison'
    : `${kind === 'sessions' ? 'Sessions' : 'Bindings'} by node`}
>
  <div class="panel-heading">
    <h2>
      {kind === 'traffic'
        ? 'Node comparison'
        : kind === 'sessions'
          ? 'Sessions by node'
          : 'Bindings by node'}
    </h2>
    {#if kind !== 'traffic'}<span class="muted">Listed inventory</span>{/if}
  </div>
  {#if kind === 'traffic'}<div class="tabs comparison-tabs" aria-label="Node comparison metric">
      {#each [{ id: 'connections', label: 'Clients' }, { id: 'incoming', label: 'Incoming' }, { id: 'outgoing', label: 'Outgoing' }] as option}
        <button
          class:chosen={metric === option.id}
          aria-pressed={metric === option.id}
          onclick={() => (metric = option.id as typeof metric)}
        >
          {option.label}
        </button>
      {/each}
    </div>{/if}
  <div class="comparison-rows">
    {#each rows as row}
      <button
        class="comparison-row"
        disabled={row.unmonitored}
        onclick={() => onselect(row.id)}
        aria-label={`${kind === 'traffic' ? 'View dashboard for' : 'Filter by'} ${row.id}`}
      >
        <span class="comparison-identity">
          <span class="status-dot" class:unavailable={!row.healthy}></span><span>
            {row.id}
          </span><Icon name="chevron" size={13} />
        </span>
        <span class="comparison-values">
          <strong>
            {row.unmonitored
              ? 'Not monitored'
              : kind === 'traffic' && !row.healthy
                ? 'Unavailable'
                : kind === 'traffic' && metric !== 'connections'
                  ? compact(row.value)
                  : number(row.value)}
          </strong><small>
            {row.unmonitored
              ? ''
              : kind === 'traffic' && metric !== 'connections'
                ? 'msg/s'
                : kind === 'subscriptions'
                  ? 'bindings'
                  : 'clients'}
          </small>
        </span>
        <span class="comparison-track" aria-hidden="true">
          <span style:width={`${total ? (row.value / total) * 100 : 0}%`}></span>
        </span>
        <span class="comparison-share">
          {row.unmonitored ? '—' : `${total ? ((row.value / total) * 100).toFixed(0) : 0}%`}
        </span>
      </button>
    {/each}
  </div>
  {#if kind === 'traffic'}<div class="comparison-caption">Share of cluster total</div>{/if}
</section>
