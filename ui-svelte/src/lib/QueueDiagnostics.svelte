<script lang="ts">
  import { number, compact, type BrokerNode } from './types';
  import Icon from './Icon.svelte';
  let { nodes, onnode }: { nodes: BrokerNode[]; onnode?: (id: string) => void } = $props();
  let reporting = $derived(nodes.filter((n) => !n.unmonitored && n.queue_metrics));
  const value = (node: BrokerNode, path: string): number | undefined => {
    let v: unknown = node.queue_metrics;
    for (const part of path.split('.'))
      v = v && typeof v === 'object' ? (v as Record<string, unknown>)[part] : undefined;
    return typeof v === 'number' ? v : undefined;
  };
  const sum = (path: string) => reporting.reduce((n, node) => n + (value(node, path) ?? 0), 0);
  const reported = (path: string) => reporting.some((n) => value(n, path) !== undefined);
  let outcomes = $derived(
    [
      { label: 'ACK', path: 'acknowledgments.ack', color: 'var(--mint)' },
      { label: 'NACK', path: 'acknowledgments.nack', color: 'var(--accent)' },
      { label: 'Rejected', path: 'acknowledgments.reject', color: 'var(--violet)' },
      { label: 'Dead-lettered', path: 'acknowledgments.dlq', color: 'var(--warning)' },
    ].map((o) => ({ ...o, value: sum(o.path), reported: reported(o.path) })),
  );
  let max = $derived(Math.max(1, ...outcomes.map((o) => o.value)));
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
        <h2>Queue delivery</h2>
        <p class="muted">
          Node-local counters · {reporting.length} reporting {reporting.length === 1
            ? 'node'
            : 'nodes'}
        </p>
      </div>
      <Icon name="queues" size={20} />
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
    <div class="diagnostic-columns">
      <div class="outcome-chart">
        <h3>Cumulative outcomes</h3>
        {#each outcomes as o}<div class="outcome-row">
            <span>{o.label}</span>
            <div class="outcome-track">
              <i style:width={`${(o.value / max) * 100}%`} style:background={o.color}></i>
            </div>
            <strong title={o.reported ? number(o.value) : 'Not reported'}>
              {o.reported ? compact(o.value) : '—'}
            </strong>
          </div>{/each}
        <p class="diagnostic-note">
          A message can have multiple outcomes across delivery attempts.
        </p>
      </div>
      <div class="delivery-signals">
        <div class="pending-gauge">
          <Icon name="clock" size={18} /><span>Pending now</span><strong>
            {reported('pending.current') ? number(sum('pending.current')) : '—'}
          </strong><small>Classic queue pending entries</small>
        </div>
        <div class="signal-grid">
          {#each faults as f}<div class:has-fault={sum(f.path) > 0} title={f.help}>
              <span>{f.label}</span><strong>{reported(f.path) ? number(sum(f.path)) : '—'}</strong>
            </div>{/each}
        </div>
      </div>
    </div>
    <details>
      <summary>Compare node diagnostics <Icon name="down" size={13} /></summary>
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
  .queue-diagnostics {
    margin-bottom: 24px;
  }
  .panel-heading p {
    font-size: 12px;
    margin-top: 6px;
  }
  .panel-heading > :global(svg) {
    color: var(--muted);
  }
  .diagnostic-columns {
    display: grid;
    grid-template-columns: 1fr 1.1fr;
    gap: 36px;
    padding: 0 24px 24px;
  }
  .outcome-chart h3 {
    font-size: 12px;
    color: var(--muted);
    font-weight: 400;
    margin: 0 0 22px;
  }
  .outcome-row {
    display: grid;
    grid-template-columns: 85px 1fr 65px;
    align-items: center;
    gap: 16px;
    font-size: 12px;
    margin: 18px 0;
  }
  .outcome-row strong {
    text-align: right;
    font-family: 'IBM Plex Mono', monospace;
    font-weight: 400;
    font-size: 15px;
  }
  .outcome-track {
    height: 8px;
    border-radius: 3px;
    background: var(--panel-raised);
    overflow: hidden;
  }
  .outcome-track i {
    height: 100%;
    display: block;
    transition: width 0.3s;
  }
  .diagnostic-note {
    font-size: 11px;
    color: var(--muted);
    margin-top: 24px;
  }
  .pending-gauge {
    display: grid;
    grid-template-columns: 20px 1fr auto;
    gap: 8px;
    align-items: center;
    margin-bottom: 18px;
  }
  .pending-gauge > :global(svg) {
    color: var(--accent);
  }
  .pending-gauge strong {
    font-size: 30px;
    font-family: 'IBM Plex Mono', monospace;
    font-weight: 400;
    grid-column: 3;
    grid-row: 1/3;
  }
  .pending-gauge small {
    font-size: 11px;
    color: var(--muted);
    grid-column: 2;
  }
  .signal-grid {
    display: grid;
    grid-template-columns: 1fr 1fr;
    gap: 8px;
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
    background: color-mix(in srgb, var(--warning) 8%, var(--panel));
  }
  .poison-warning {
    margin: 0 24px 24px;
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
    padding: 0 24px 18px;
  }
  summary {
    padding: 14px 24px;
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
  @media (max-width: 1000px) {
    .diagnostic-columns {
      grid-template-columns: 1fr;
      gap: 24px;
    }
  }
  @media (max-width: 500px) {
    .signal-grid {
      grid-template-columns: 1fr;
    }
    .outcome-row {
      gap: 8px;
    }
  }
</style>
