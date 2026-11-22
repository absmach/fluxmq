<script lang="ts">
  import Icon from './Icon.svelte';
  import { number, compact, bytes, uptime, type BrokerNode } from './types';
  let { nodes, loaded }: { nodes: BrokerNode[]; loaded: boolean } = $props();
  let counters = $derived.by(() => {
    const sum = (key: 'received' | 'sent' | 'bytes_in' | 'bytes_out' | 'retained_messages') =>
      nodes.reduce((total, node) => total + (node[key] ?? 0), 0);
    const shortestUptime = nodes.length ? Math.min(...nodes.map((node) => node.uptime)) : 0;
    return [
      {
        label: 'Messages received',
        icon: 'receive',
        tone: 'incoming',
        value: compact(sum('received')),
        exact: `${number(sum('received'))} messages`,
      },
      {
        label: 'Messages sent',
        icon: 'send',
        tone: 'outgoing',
        value: compact(sum('sent')),
        exact: `${number(sum('sent'))} messages`,
      },
      {
        label: 'Data received',
        icon: 'receive',
        tone: 'incoming',
        value: bytes(sum('bytes_in')),
        exact: `${number(sum('bytes_in'))} bytes`,
      },
      {
        label: 'Data sent',
        icon: 'send',
        tone: 'outgoing',
        value: bytes(sum('bytes_out')),
        exact: `${number(sum('bytes_out'))} bytes`,
      },
      {
        label: 'Retained messages',
        icon: 'archive',
        tone: 'neutral',
        value: compact(sum('retained_messages')),
        exact: `${number(sum('retained_messages'))} retained messages`,
      },
      {
        label: nodes.length > 1 ? 'Shortest node uptime' : 'Node uptime',
        icon: 'clock',
        tone: 'neutral',
        value: uptime(shortestUptime),
        exact: `${number(shortestUptime)} seconds${nodes.length > 1 ? ' · minimum across nodes' : ''}`,
      },
    ];
  });
</script>

<section class="panel broker-totals" aria-labelledby="cumulative-title">
  <h2 id="cumulative-title">Cumulative counters</h2>
  <dl>
    {#each counters as counter}
      <div class="counter-cell">
        <dt>{counter.label}</dt>
        <dd title={loaded && nodes.length ? counter.exact : undefined}>
          <span class="counter-icon {counter.tone}"><Icon name={counter.icon} size={18} /></span>
          <span class="counter-value">
            {#if loaded && nodes.length}
              <span class="sr-only">{counter.exact}</span>
              <span aria-hidden="true">
                {#each counter.value.split(/([A-Za-z]+)/) as part}{#if /^[A-Za-z]+$/.test(part)}<small>
                      {part}
                    </small>{:else}{part}{/if}{/each}
              </span>
            {:else}—{/if}
          </span>
        </dd>
      </div>
    {/each}
  </dl>
</section>
