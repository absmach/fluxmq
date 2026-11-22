<script lang="ts">
  import {
    number,
    compact,
    bytes,
    type BrokerNode,
    type ProtocolMetrics,
    type Point,
  } from './types';
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
    onnode: (id: string) => void;
  } = $props();
  let range = $state(5),
    metric = $state('messages');
  let available = $derived([
    ...new Set(nodes.flatMap((n) => Object.keys(n.protocol_metrics ?? {}))),
  ]);
  let visible = $derived(
    points.filter((p) => p.time >= (points.at(-1)?.time ?? 0) - range * 60000),
  );
  let series = $derived(
    available.flatMap((protocol, index) => {
      const name = label(protocol);
      const colors = index === 0 ? ['var(--accent)', 'var(--mint)'] : ['var(--violet)', '#b66e9b'];
      if (metric === 'connections')
        return [
          {
            key: `protocol.${protocol}.connections.current`,
            label: `${name} connections`,
            color: colors[0],
          },
        ];
      if (metric === 'errors')
        return (
          protocol === 'mqtt'
            ? (['protocol', 'auth', 'authz', 'packet'] as const)
            : (['protocol'] as const)
        ).map((error, i) => ({
          key: `protocol.${protocol}.errors.${error}`,
          label: `${name} ${{ protocol: 'protocol', auth: 'authentication', authz: 'authorization', packet: 'packet' }[error]}`,
          color: [colors[0], 'var(--warning)', colors[1], '#4c9cb5'][i],
        }));
      return ['received', 'sent'].map((direction, i) => ({
        key: `protocol.${protocol}.${metric === 'bandwidth' ? 'bytes' : 'messages'}.${direction}`,
        label: `${name} ${i === 0 ? 'incoming' : 'outgoing'}`,
        color: colors[i],
        dash: index ? '6 3' : undefined,
      }));
    }),
  );
  let rows = $derived(
    nodes.flatMap((n) =>
      Object.entries(n.protocol_metrics ?? {})
        .filter(([, m]) => !!m)
        .map(([protocol, metrics]) => ({ node: n.id, healthy: n.healthy, protocol, metrics })),
    ),
  );
  const label = (p: string) => (p === 'mqtt' ? 'MQTT' : p === 'amqp' ? 'AMQP 0.9.1' : p);
  const num = (n?: number) => (n === undefined ? '—' : number(n));
  const errors = (m: ProtocolMetrics) => Object.values(m.errors ?? {}).reduce((a, b) => a + b, 0);
</script>

{#if rows.length}<section class="panel protocol-diagnostics" aria-label="Protocol diagnostics">
    <div class="panel-heading">
      <div>
        <h2>Protocol traffic</h2>
        <p class="muted">{nodes.length === 1 ? nodes[0].id : 'Across reporting nodes'}</p>
      </div>
      <div class="range-control segmented" aria-label="Protocol history range">
        {#each [1, 5, 15] as minutes}<button
            class:chosen={range === minutes}
            aria-pressed={range === minutes}
            onclick={() => (range = minutes)}
          >
            {minutes}m
          </button>{/each}
      </div>
    </div>
    <div class="protocol-chart-controls">
      <label>
        Metric <select aria-label="Protocol chart metric" bind:value={metric}>
          {#each ['messages', 'bandwidth', 'connections', 'errors'] as option}<option
              value={option}
            >
              {option[0].toUpperCase() + option.slice(1)}
            </option>{/each}
        </select>
      </label>
    </div>
    <DiagnosticChart
      points={visible}
      {series}
      label={`Protocol ${metric} history`}
      unit={metric === 'connections' ? 'clients' : metric === 'messages' ? 'msg / s' : ' / s'}
      byteScale={metric === 'bandwidth'}
    />
    <div class="diagnostic-chart-footer">
      <span>
        <Icon name="clock" size={13} />Last {range}
        {range === 1 ? 'minute' : 'minutes'}
      </span><span>{demo ? 'Simulated history' : 'Collected history'}</span>
    </div>
    <details>
      <summary>Compare node counters <Icon name="down" size={13} /></summary>
      <div class="table-scroll">
        <table>
          <thead>
            <tr>
              <th>Node / protocol</th><th class="numeric">Connections</th><th class="numeric">
                Messages in / out
              </th><th class="numeric">Data in / out</th><th class="numeric">Errors</th>
            </tr>
          </thead><tbody>
            {#each rows as r}<tr>
                <td>
                  <button class="text-button" onclick={() => onnode(r.node)}>{r.node}</button><span
                    class="protocol-label"
                  >
                    {label(r.protocol)}{!r.healthy ? ' · Last known' : ''}
                  </span>
                </td><td class="numeric">{num(r.metrics.connections?.current)}</td><td
                  class="numeric"
                >
                  {r.metrics.messages?.received === undefined
                    ? '—'
                    : compact(r.metrics.messages.received)} / {r.metrics.messages?.sent ===
                  undefined
                    ? '—'
                    : compact(r.metrics.messages.sent)}
                </td><td class="numeric">
                  {r.metrics.bytes?.received === undefined ? '—' : bytes(r.metrics.bytes.received)} /
                  {r.metrics.bytes?.sent === undefined ? '—' : bytes(r.metrics.bytes.sent)}
                </td><td class="numeric" class:has-errors={errors(r.metrics) > 0}>
                  {r.metrics.errors ? number(errors(r.metrics)) : '—'}
                </td>
              </tr>{/each}
          </tbody>
        </table>
      </div>
    </details>
    <details>
      <summary>Protocol internals <Icon name="down" size={13} /></summary>
      <div class="protocol-internals">
        {#each rows as r}<article>
            <h3>{r.node} <span>{label(r.protocol)}</span></h3>
            <dl>
              <div>
                <dt>Total connections</dt>
                <dd>{num(r.metrics.connections?.total)}</dd>
              </div>
              <div>
                <dt>Disconnections</dt>
                <dd>{num(r.metrics.connections?.disconnections)}</dd>
              </div>
              {#if r.protocol === 'mqtt'}<div>
                  <dt>PUBLISH received / sent</dt>
                  <dd>
                    {num(r.metrics.messages?.publish_received)} / {num(
                      r.metrics.messages?.publish_sent,
                    )}
                  </dd>
                </div>{:else if r.protocol === 'amqp'}<div>
                  <dt>Channels / consumers</dt>
                  <dd>{num(r.metrics.channels)} / {num(r.metrics.consumers)}</dd>
                </div>{/if}{#each Object.entries(r.metrics.errors ?? {}) as [key, value]}<div>
                  <dt>
                    {(
                      {
                        protocol: 'Protocol errors',
                        auth: 'Authentication errors',
                        authz: 'Authorization errors',
                        packet: 'Packet errors',
                      } as Record<string, string>
                    )[key] ?? key}
                  </dt>
                  <dd class:has-errors={value > 0}>{number(value)}</dd>
                </div>{/each}
              {#if r.metrics.local_principals}{@const l = r.metrics.local_principals}
                <div>
                  <dt>Local principal connections</dt>
                  <dd>{num(l.active_connections)}</dd>
                </div>
                <div>
                  <dt>Publish timeouts / rejections</dt>
                  <dd>{num(l.publish_timeouts)} / {num(l.publish_rejections)}</dd>
                </div>
                <div>
                  <dt>Authentication success / failure</dt>
                  <dd>{num(l.authentication?.success)} / {num(l.authentication?.failure)}</dd>
                </div>
                <div>
                  <dt>Publish / subscribe / operation denied</dt>
                  <dd>
                    {num(l.authorization?.publish_denied)} / {num(
                      l.authorization?.subscribe_denied,
                    )} / {num(l.authorization?.operation_denied)}
                  </dd>
                </div>
                <div>
                  <dt>Reload success / failure</dt>
                  <dd>{num(l.reloads?.success)} / {num(l.reloads?.failure)}</dd>
                </div>
                <div>
                  <dt>Forced disconnects</dt>
                  <dd>{num(l.reloads?.forced_disconnects)}</dd>
                </div>{/if}
            </dl>
          </article>{/each}
      </div>
      <p class="footnote">
        Cumulative error counters are not an event log. Protocols absent from the broker stats API
        are not shown.
      </p>
    </details>
  </section>{/if}

<style>
  .protocol-diagnostics {
    overflow: visible;
  }
  .panel-heading p {
    font-size: 13px;
    margin-top: 6px;
  }
  .panel-heading {
    padding: 18px 22px 12px;
  }
  .protocol-chart-controls {
    padding: 0 22px 14px;
  }
  .protocol-chart-controls label {
    display: flex;
    align-items: center;
    gap: 10px;
    font-size: 12px;
    color: var(--muted);
  }
  .protocol-chart-controls select {
    padding: 6px 28px 6px 10px;
    border: 1px solid var(--border);
    border-radius: 6px;
    background: var(--panel);
    color: var(--text);
    font: inherit;
  }
  .diagnostic-chart-footer {
    display: flex;
    align-items: center;
    justify-content: space-between;
    padding: 0 22px 14px;
    color: var(--faint);
    font-size: 11px;
  }
  .diagnostic-chart-footer span {
    display: flex;
    gap: 7px;
    align-items: center;
  }
  @media (max-width: 600px) {
    .protocol-chart-controls {
      padding: 0 16px 16px;
      gap: 12px;
    }
    .diagnostic-chart-footer {
      padding: 0 16px 18px;
    }
  }

  .protocol-diagnostics {
    margin-bottom: 24px;
  }
  .protocol-label {
    display: block;
    font-size: 11px;
    color: var(--muted);
    margin-top: 5px;
  }
  .has-errors {
    color: var(--warning);
  }
  summary {
    display: flex;
    align-items: center;
    justify-content: space-between;
    cursor: pointer;
    font-size: 12px;
    padding: 16px 24px;
    border-top: 1px solid var(--subtle-border);
    color: var(--muted);
  }
  .protocol-internals {
    padding: 0 24px;
    display: grid;
    grid-template-columns: repeat(auto-fit, minmax(300px, 1fr));
    gap: 24px;
  }
  .protocol-internals article {
    min-width: 0;
    padding: 18px;
    background: var(--panel-raised);
    border-radius: 8px;
  }
  .protocol-internals h3 {
    font-size: 13px;
    overflow-wrap: anywhere;
  }
  .protocol-internals h3 span {
    font-weight: 400;
    color: var(--muted);
    margin-left: 8px;
  }
  .protocol-internals dl {
    margin: 16px 0 0;
    font-size: 12px;
  }
  .protocol-internals dl > div {
    display: flex;
    justify-content: space-between;
    gap: 16px;
    margin: 10px 0;
  }
  .protocol-internals dt {
    color: var(--muted);
  }
  .protocol-internals dd {
    font-family: 'IBM Plex Mono', monospace;
    margin: 0;
    text-align: right;
  }
  .footnote {
    font-size: 11px;
    color: var(--muted);
    padding: 16px 24px;
  }
  @media (max-width: 500px) {
    .protocol-internals {
      grid-template-columns: 1fr;
    }
  }
</style>
