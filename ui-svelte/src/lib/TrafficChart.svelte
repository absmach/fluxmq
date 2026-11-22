<script lang="ts">
  import { compact, number, bytes, type Point } from './types';
  let {
    points,
    mode = 'messages',
    small = false,
    color = 'blue',
    label = 'Message throughput',
  }: {
    points: Point[];
    mode?: 'messages' | 'bandwidth' | 'connections';
    small?: boolean;
    color?: string;
    label?: string;
  } = $props();
  let samples = $derived(
    points.map((p) =>
      mode === 'bandwidth'
        ? { ...p, incoming: p.bytes_in ?? 0, outgoing: p.bytes_out ?? 0 }
        : mode === 'connections'
          ? { ...p, incoming: p.connections, outgoing: 0 }
          : p,
    ),
  );
  const format = (value: number) =>
    mode === 'bandwidth'
      ? `${bytes(value)}/s`
      : mode === 'connections'
        ? number(value)
        : `${number(value)} msg/s`;
  const width = 800,
    height = 212;
  let hover = $state<number | null>(null);
  let max = $derived(Math.max(100, ...samples.flatMap((p) => [p.incoming, p.outgoing])) * 1.2);
  let current = $derived(hover === null ? null : samples[hover]);
  function line(key: 'incoming' | 'outgoing') {
    return samples
      .map(
        (p, i) =>
          `${i === 0 ? 'M' : 'L'}${((i / Math.max(1, samples.length - 1)) * width).toFixed(2)},${(height - (p[key] / max) * (height - 10)).toFixed(2)}`,
      )
      .join(' ');
  }
  let incoming = $derived(line('incoming'));
  let outgoing = $derived(line('outgoing'));
  const time = (n: number) =>
    new Date(n).toLocaleTimeString([], {
      hour: '2-digit',
      minute: '2-digit',
      second:
        (samples.at(-1)?.time ?? 0) - (samples[0]?.time ?? 0) < 120000 ? '2-digit' : undefined,
    });
</script>

<div class:mini={small} class="chart-wrap">
  {#if !small}<div class="y-axis">
      {#each [1, 0.75, 0.5, 0.25, 0] as f}<span>
          {mode === 'bandwidth' ? bytes(max * f) : compact(max * f)}
        </span>{/each}
    </div>{/if}
  <div class="chart-main">
    <svg
      class="traffic-chart"
      viewBox={`0 0 ${width} ${height}`}
      preserveAspectRatio="none"
      role="img"
      aria-label={label}
    >
      <defs>
        <linearGradient id={`area-${color}-${small}`} x1="0" y1="0" x2="0" y2="1">
          <stop offset="0%" stop-color="var(--accent)" stop-opacity=".2" /><stop
            offset="100%"
            stop-color="var(--accent)"
            stop-opacity="0"
          />
        </linearGradient>
      </defs>
      {#if !small}{#each [0, 0.25, 0.5, 0.75, 1] as f}<line
            x1="0"
            x2={width}
            y1={height * f}
            y2={height * f}
            class="grid-line"
          />{/each}{/if}
      {#if samples.length > 1}
        <path
          d={`${incoming} L${width},${height} L0,${height} Z`}
          fill={`url(#area-${color}-${small})`}
        />
        {#if !small && mode !== 'connections'}<path
            d={outgoing}
            class="line-out"
            fill="none"
            vector-effect="non-scaling-stroke"
          />{/if}
        <path
          d={incoming}
          class:green={color === 'green'}
          class="line-in"
          fill="none"
          vector-effect="non-scaling-stroke"
        />
        {#if hover !== null && current && !small}
          <line
            x1={(hover / (samples.length - 1)) * width}
            x2={(hover / (samples.length - 1)) * width}
            y1="0"
            y2={height}
            class="crosshair"
          />
          <circle
            cx={(hover / (samples.length - 1)) * width}
            cy={height - (current.incoming / max) * (height - 10)}
            r="4"
            fill="var(--accent)"
          />
        {/if}
      {/if}
    </svg>
    {#if !small}
      <div
        class="chart-hit-area"
        role="presentation"
        onpointermove={(e) => {
          const rect = e.currentTarget.getBoundingClientRect();
          hover = Math.min(
            samples.length - 1,
            Math.max(0, Math.round(((e.clientX - rect.left) / rect.width) * (samples.length - 1))),
          );
        }}
        onpointerleave={() => (hover = null)}
      ></div>
      {#if current}<div
          class="chart-tooltip"
          style:left={`${Math.min(76, Math.max(3, ((hover ?? 0) / Math.max(1, samples.length - 1)) * 100))}%`}
        >
          <strong>{time(current.time)}</strong><span>
            <i class="legend-dot blue"></i>
            {mode === 'connections' ? 'Connections' : 'In'}
            {format(current.incoming)}
          </span>{#if mode !== 'connections'}<span>
              <i class="legend-dot green"></i> Out {format(current.outgoing)}
            </span>{/if}
        </div>{/if}
      <div class="x-axis">
        {#each [0, 0.25, 0.5, 0.75, 1] as f}<span>
            {samples.length ? time(samples[Math.round((samples.length - 1) * f)].time) : '—'}
          </span>{/each}
      </div>
      {#if samples.length < 2}<div class="chart-empty">Collecting samples…</div>{/if}
    {/if}
  </div>
</div>
