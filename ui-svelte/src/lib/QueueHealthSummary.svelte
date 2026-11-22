<script lang="ts">
  import Icon from './Icon.svelte';
  import { compact, number, type BrokerNode, type Point } from './types';

  let {
    nodes,
    points,
    href,
    onopen,
  }: {
    nodes: BrokerNode[];
    points: Point[];
    href: string;
    onopen: (event: MouseEvent) => void;
  } = $props();

  let reporting = $derived(nodes.filter((n) => !n.unmonitored && n.queue_metrics));
  const total = (read: (n: BrokerNode) => number | undefined) => {
    const values = reporting.map(read).filter((v): v is number => v !== undefined);
    return values.length ? values.reduce((sum, v) => sum + v, 0) : undefined;
  };
  let pending = $derived(total((n) => n.queue_metrics?.pending?.current));
  let poison = $derived(total((n) => n.queue_metrics?.acknowledgments?.poison_pending));
  let noDestination = $derived(
    total((n) => n.queue_metrics?.acknowledgments?.poison_pending_no_destination),
  );
  let latest = $derived(points.at(-1)?.metrics);
  const rates = [
    { label: 'NACK', key: 'queue.acknowledgments.nack', icon: 'refresh' },
    { label: 'Rejected', key: 'queue.acknowledgments.reject', icon: 'close' },
    { label: 'Dead-lettered', key: 'queue.acknowledgments.dlq', icon: 'archive' },
    { label: 'Capture failed', key: 'queue.capture_failures', icon: 'alert' },
    { label: 'Capture dropped', key: 'queue.capture_dropped', icon: 'disconnected' },
  ];
  const format = (v?: number) =>
    v === undefined
      ? '—'
      : v > 0 && v < 0.1
        ? new Intl.NumberFormat('en-US', { maximumSignificantDigits: 2 }).format(v)
        : compact(v);
</script>

<section class="panel queue-health-summary" aria-label="Queue health summary">
  <div class="panel-heading">
    <h2>Queue health</h2>
    <Icon name="queues" size={18} />
  </div>
  {#if reporting.length < nodes.length || !reporting.length}
    <p class="reporting-note">{reporting.length} / {nodes.length} nodes reporting queue metrics</p>
  {/if}
  {#if reporting.some((n) => !n.healthy)}
    <p class="reporting-note">Includes last-known values from unavailable nodes.</p>
  {/if}
  <div class="pending-summary">
    <span>Awaiting acknowledgement</span>
    <strong title={pending === undefined ? 'Not reported' : number(pending)}>
      {format(pending)}<small>messages</small>
    </strong>
    {#if poison !== undefined}
      <span class:warning={poison > 0}>
        <i class:active={poison > 0}></i>{number(poison)} poison messages pending
        {#if (noDestination ?? 0) > 0}<small>
            · {number(noDestination!)} have no dead-letter destination
          </small>{/if}
      </span>
    {/if}
  </div>
  <dl>
    {#each rates as rate}
      {@const value = reporting.length ? latest?.[rate.key] : undefined}
      <div class:warning={(value ?? 0) > 0}>
        <dt><Icon name={rate.icon} size={15} />{rate.label}</dt>
        <dd>{format(value)}<small>msg/s</small></dd>
      </div>
    {/each}
  </dl>
  <a class="queue-link" {href} onclick={onopen}>
    View queues<Icon name="chevron" size={15} />
  </a>
</section>

<style>
  .queue-health-summary {
    display: flex;
    flex-direction: column;
    min-width: 0;
  }
  .panel-heading {
    padding: 18px 22px;
    color: var(--muted);
  }
  .panel-heading h2 {
    color: var(--text);
  }
  .reporting-note {
    margin: 0;
    padding: 0 22px 12px;
    font-size: 12px;
    color: var(--warning);
  }
  .pending-summary {
    padding: 6px 22px 20px;
    display: flex;
    flex-direction: column;
    gap: 10px;
  }
  .pending-summary > span {
    font-size: 12px;
    color: var(--muted);
  }
  .pending-summary strong {
    display: flex;
    align-items: baseline;
    gap: 8px;
    font: 400 32px var(--mono);
    color: var(--text);
  }
  small {
    font:
      400 11px 'IBM Plex Sans',
      sans-serif;
    color: var(--muted);
  }
  i {
    display: inline-block;
    width: 5px;
    height: 5px;
    background: var(--faint);
    border-radius: 50%;
    margin-right: 6px;
  }
  i.active {
    background: var(--warning);
  }
  dl {
    margin: 0;
    padding: 0 22px 12px;
  }
  dl > div {
    display: flex;
    align-items: center;
    justify-content: space-between;
    gap: 12px;
    padding: 13px 0;
    border-top: 1px solid var(--subtle-border);
  }
  dt {
    display: flex;
    align-items: center;
    gap: 9px;
    font-size: 12px;
    color: var(--muted);
  }
  dd {
    margin: 0;
    font: 400 14px var(--mono);
    white-space: nowrap;
  }
  dd small {
    margin-left: 6px;
  }
  .pending-summary .warning,
  .warning dd {
    color: var(--warning);
  }
  .queue-link {
    margin-top: auto;
    padding: 16px 22px;
    border-top: 1px solid var(--subtle-border);
    display: flex;
    align-items: center;
    justify-content: space-between;
    font-size: 12px;
    color: var(--accent);
  }
  .queue-link:hover {
    background: var(--accent-bg);
  }
</style>
