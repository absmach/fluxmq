<script lang="ts">
  import { number, compact, type BrokerNode, type Point } from './types';
  import Icon from './Icon.svelte';
  import DiagnosticChart from './DiagnosticChart.svelte';
  let {
    nodes,
    points = [],
    demo = false,
    onnode,
  }: {
    nodes: BrokerNode[];
    points?: Point[];
    demo?: boolean;
    onnode?: (id: string) => void;
  } = $props();
  let range = $state(5);
  let rateScale = $state('log');
  let visible = $derived(
    points.filter((p) => p.time >= (points.at(-1)?.time ?? 0) - range * 60000),
  );
  const series = [
    { key: 'queue.pending.current', label: 'Pending', color: 'var(--accent)', unit: 'messages' },
    {
      key: 'queue.acknowledgments.poison_pending',
      label: 'Poison pending',
      color: 'var(--violet)',
      unit: 'messages',
      dash: '5 3',
    },
    {
      key: 'queue.acknowledgments.ack',
      label: 'ACK',
      color: 'var(--mint)',
      unit: 'msg/s',
      axis: 'right' as const,
    },
    {
      key: 'queue.acknowledgments.nack',
      label: 'NACK',
      color: '#4c9cb5',
      unit: 'msg/s',
      axis: 'right' as const,
      dash: '6 3',
    },
    {
      key: 'queue.acknowledgments.reject',
      label: 'Rejected',
      color: '#b66e9b',
      unit: 'msg/s',
      axis: 'right' as const,
    },
    {
      key: 'queue.acknowledgments.dlq',
      label: 'Dead-lettered',
      color: '#8b85c6',
      unit: 'msg/s',
      axis: 'right' as const,
      dash: '3 3',
    },
    {
      key: 'queue.capture_failures',
      label: 'Capture failed',
      color: 'var(--warning)',
      unit: 'msg/s',
      axis: 'right' as const,
    },
    {
      key: 'queue.capture_dropped',
      label: 'Capture dropped',
      color: '#9a7986',
      unit: 'msg/s',
      axis: 'right' as const,
      dash: '6 3',
    },
  ];
  let reporting = $derived(nodes.filter((n) => !n.unmonitored && n.queue_metrics));
  const value = (node: BrokerNode, path: string): number | undefined => {
    let v: unknown = node.queue_metrics;
    for (const part of path.split('.'))
      v = v && typeof v === 'object' ? (v as Record<string, unknown>)[part] : undefined;
    return typeof v === 'number' ? v : undefined;
  };
  const sum = (path: string) => reporting.reduce((n, node) => n + (value(node, path) ?? 0), 0);
  const reported = (path: string) => reporting.some((n) => value(n, path) !== undefined);
  let faults = $derived([
    {
      label: 'Capture failures',
      path: 'capture_failures',
      help: 'Matching queues that a captured publish failed to reach.',
    },
    {
      label: 'Capture dropped',
      path: 'capture_dropped',
      help: 'Capture jobs discarded before they could be attempted.',
    },
    {
      label: 'DLQ transfer failures',
      path: 'acknowledgments.dlq_transfer_failures',
      help: 'Failed dead-letter transfers. Cumulative, not a current backlog.',
    },
    {
      label: 'Poison without DLQ',
      path: 'acknowledgments.poison_without_dlq',
      help: 'Poison messages returned to redelivery because no dead-letter destination was configured.',
    },
    {
      label: 'Claim failures',
      path: 'claims.failures',
      help: 'Failed attempts to claim deliveries.',
    },
    { label: 'Steal failures', path: 'steals.failures', help: 'Failed work-stealing attempts.' },
  ]);
</script>

{#if reporting.length}
  <section class="panel queue-diagnostics" aria-label="Queue delivery diagnostics">
    <div class="panel-heading">
      <div>
        <h2>Queue activity</h2>
        <p class="muted">{nodes.length === 1 ? nodes[0].id : 'Across reporting nodes'}</p>
      </div>
      <div class="chart-controls">
        <label
          class="scale-control"
          title="Logarithmic rate scale keeps small rates visible alongside high throughput. Zero remains zero; counts use a linear scale."
        >
          Rates <select aria-label="Queue rate scale" bind:value={rateScale}>
            <option value="log">Log</option><option value="linear">Linear</option>
          </select>
        </label>
        <div class="range-control segmented" aria-label="Queue history range">
          {#each [1, 5, 15] as minutes}<button
              class:chosen={range === minutes}
              aria-pressed={range === minutes}
              onclick={() => (range = minutes)}
            >
              {minutes}m
            </button>{/each}
        </div>
      </div>
    </div>
    {#if nodes.some((n) => !n.healthy && !n.unmonitored)}<p class="diagnostic-notice">
        Includes last-known values from unavailable nodes.
      </p>{/if}
    {#if sum('acknowledgments.poison_pending') > 0}<div class="poison-warning" role="status">
        <Icon name="info" size={18} /><span>
          <strong>
            {number(sum('acknowledgments.poison_pending'))} poison messages pending
          </strong>{#if sum('acknowledgments.poison_pending_no_destination') > 0}
            · {number(sum('acknowledgments.poison_pending_no_destination'))} have no dead-letter destination{/if}
        </span>
      </div>{/if}
    <DiagnosticChart
      points={visible}
      {series}
      label="Queue activity history"
      logRates={rateScale === 'log'}
    />
    <div class="diagnostic-chart-footer">
      <span>
        <Icon name="clock" size={13} />Last {range}
        {range === 1 ? 'minute' : 'minutes'}
      </span><span>{demo ? 'Simulated history' : 'Collected history'}</span>
    </div>
    <details>
      <summary>Counters & node comparison <Icon name="down" size={13} /></summary>
      <div class="diagnostic-totals">
        {#each [{ label: 'ACK', path: 'acknowledgments.ack' }, { label: 'NACK', path: 'acknowledgments.nack' }, { label: 'Rejected', path: 'acknowledgments.reject' }, { label: 'Dead-lettered', path: 'acknowledgments.dlq' }] as o}<div>
            <span>{o.label}</span><strong
              title={reported(o.path) ? number(sum(o.path)) : 'Not reported'}
            >
              {reported(o.path) ? compact(sum(o.path)) : '—'}
            </strong>
          </div>{/each}
      </div>
      <p class="diagnostic-note">
        Cumulative outcomes; one message may have multiple delivery attempts.
      </p>
      <div class="signal-grid">
        {#each faults as f}<div class:has-fault={sum(f.path) > 0} title={f.help}>
            <span>{f.label}</span><strong>{reported(f.path) ? number(sum(f.path)) : '—'}</strong>
          </div>{/each}
      </div>
      <div class="table-scroll">
        <table>
          <thead>
            <tr>
              <th>Node</th><th class="numeric">Pending</th><th class="numeric">
                Poison pending
              </th><th class="numeric">Capture failed / dropped</th><th class="numeric">
                Claims successful / attempted
              </th><th class="numeric">Steals successful / attempted</th>
            </tr>
          </thead><tbody>
            {#each reporting as n}<tr>
                <td>
                  {#if onnode}<button class="text-button" onclick={() => onnode?.(n.id)}>
                      {n.id}
                    </button>{:else}{n.id}{/if}
                </td>{#each ['pending.current', 'acknowledgments.poison_pending'] as path}<td
                    class="numeric"
                  >
                    {value(n, path) === undefined ? '—' : number(value(n, path)!)}
                  </td>{/each}{#each [['capture_failures', 'capture_dropped'], ['claims.successes', 'claims.attempts'], ['steals.successes', 'steals.attempts']] as pair}<td
                    class="numeric"
                  >
                    {pair
                      .map((p) => (value(n, p) === undefined ? '—' : number(value(n, p)!)))
                      .join(' / ')}
                  </td>{/each}
              </tr>{/each}
          </tbody>
        </table>
      </div>
    </details>
  </section>
{/if}

<style>
  .chart-controls {
    display: flex;
    align-items: center;
    gap: 16px;
    flex-wrap: wrap;
    justify-content: flex-end;
  }
  .scale-control {
    display: flex;
    align-items: center;
    gap: 6px;
    font-size: 11px;
    color: var(--muted);
  }
  .scale-control select {
    font: inherit;
    color: var(--text);
    background: var(--panel);
    border: 1px solid var(--border);
    border-radius: 6px;
    padding: 6px 8px;
  }
  @media (max-width: 600px) {
    .panel-heading {
      flex-wrap: wrap;
      gap: 12px;
    }
    .chart-controls {
      gap: 10px;
    }
  }

  .queue-diagnostics {
    overflow: visible;
  }
  .queue-diagnostics {
    margin-bottom: 24px;
  }
  .panel-heading {
    padding: 18px 22px;
  }
  .panel-heading p {
    font-size: 13px;
    margin-top: 6px;
  }
  .diagnostic-chart-footer {
    display: flex;
    align-items: center;
    justify-content: space-between;
    color: var(--faint);
    padding: 0 22px 14px;
    font-size: 11px;
  }
  .diagnostic-chart-footer span {
    display: flex;
    gap: 7px;
    align-items: center;
  }
  .diagnostic-totals {
    display: grid;
    grid-template-columns: repeat(4, 1fr);
    gap: 24px;
    padding: 24px 28px 16px;
  }
  .diagnostic-totals span {
    display: block;
    font-size: 12px;
    color: var(--muted);
    margin-bottom: 8px;
  }
  .diagnostic-totals strong {
    font-size: 24px;
    font-family: 'IBM Plex Mono', monospace;
    font-weight: 400;
  }
  .diagnostic-note {
    font-size: 11px;
    color: var(--muted);
    padding: 0 28px 20px;
  }
  .signal-grid {
    display: grid;
    grid-template-columns: repeat(3, 1fr);
    gap: 12px;
    padding: 0 22px 14px;
  }
  .signal-grid > div {
    display: flex;
    align-items: center;
    justify-content: space-between;
    gap: 10px;
    padding: 12px;
    background: var(--panel-raised);
    border-radius: 5px;
    font-size: 11px;
    color: var(--muted);
  }
  .signal-grid strong {
    font-size: 14px;
    font-family: 'IBM Plex Mono', monospace;
    font-weight: 400;
  }
  .signal-grid .has-fault {
    color: var(--warning);
  }
  .poison-warning {
    margin: 0 28px 24px;
    display: flex;
    gap: 10px;
    color: var(--warning);
    padding: 12px 16px;
    background: color-mix(in srgb, var(--warning) 8%, var(--panel));
    border-left: 2px solid var(--warning);
    font-size: 13px;
  }
  .diagnostic-notice {
    font-size: 12px;
    color: var(--warning);
    padding: 0 28px 18px;
  }
  summary {
    padding: 16px 28px;
    border-top: 1px solid var(--subtle-border);
    font-size: 12px;
    color: var(--muted);
    cursor: pointer;
    display: flex;
    align-items: center;
    justify-content: space-between;
  }
  details[open] summary {
    color: var(--text);
  }
  @media (max-width: 600px) {
    .diagnostic-chart-footer {
      padding: 0 16px 18px;
    }
    .diagnostic-totals {
      grid-template-columns: 1fr 1fr;
    }
    .signal-grid {
      grid-template-columns: 1fr;
    }
    .panel-heading {
      align-items: flex-start;
    }
  }
</style>
