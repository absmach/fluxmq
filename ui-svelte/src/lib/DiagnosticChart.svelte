<script lang="ts">
  import { compact, bytes, type Point } from './types';
  let {
    points,
    series,
    label,
    unit = ' / s',
    byteScale = false,
  }: {
    points: Point[];
    series: { key: string; label: string; color: string }[];
    label: string;
    unit?: string;
    byteScale?: boolean;
  } = $props();
  const uid = $props.id();
  const width = 960,
    height = 250;
  let hovered = $state<number | null>(null);
  let active = $derived(hovered === null ? points.at(-1) : points[hovered]);
  let max = $derived(
    Math.max(1, ...points.flatMap((p) => series.map((s) => p.metrics?.[s.key] ?? 0))) * 1.16,
  );
  let start = $derived(points[0]?.time ?? 0),
    end = $derived(points.at(-1)?.time ?? 1);
  let hasValues = $derived(
    points.some((p) => series.some((s) => p.metrics?.[s.key] !== undefined)),
  );
  const x = (time: number) => ((time - start) / Math.max(1, end - start)) * width;
  const y = (value: number) => height - (value / max) * height;
  const time = (t: number) =>
    new Date(t).toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' });
  const format = (v: number | undefined) =>
    v === undefined ? '—' : byteScale ? bytes(v) : compact(v);
  function segments(key: string) {
    const groups: { x: number; y: number }[][] = [];
    let group: { x: number; y: number }[] = [];
    points.forEach((p, i) => {
      const value = p.metrics?.[key];
      if (value === undefined || (i > 0 && p.time - points[i - 1].time > 12000)) {
        if (group.length) groups.push(group);
        group = [];
      }
      if (value !== undefined) group.push({ x: x(p.time), y: y(value) });
    });
    if (group.length) groups.push(group);
    return groups.map((g) => ({
      points: g,
      line: g
        .map((p, i) =>
          i === 0
            ? `M ${p.x} ${p.y}`
            : `C ${(g[i - 1].x + p.x) / 2} ${g[i - 1].y}, ${(g[i - 1].x + p.x) / 2} ${p.y}, ${p.x} ${p.y}`,
        )
        .join(' '),
    }));
  }
  function pointer(e: PointerEvent) {
    const r = e.currentTarget as HTMLElement;
    const rect = r.getBoundingClientRect();
    const t =
      start + Math.max(0, Math.min(1, (e.clientX - rect.left) / rect.width)) * (end - start);
    let best = 0;
    points.forEach((p, i) => {
      if (Math.abs(p.time - t) < Math.abs(points[best].time - t)) best = i;
    });
    hovered = points.length ? best : null;
  }
</script>

<div class="diagnostic-chart">
  <div class="series-readout">
    {#each series as s}<div>
        <span class="series-label"><i style:background={s.color}></i>{s.label}</span><strong
          style:color={s.color}
        >
          {format(active?.metrics?.[s.key])}<small>{unit}</small>
        </strong>
      </div>{/each}
  </div>
  <div class="plot-layout">
    <div class="plot-y">
      {#each [1, 0.75, 0.5, 0.25, 0] as f}<span>{format(max * f)}</span>{/each}
    </div>
    <div class="plot-main">
      <svg
        viewBox={`0 0 ${width} ${height}`}
        preserveAspectRatio="none"
        role="img"
        aria-label={label}
      >
        <defs>
          {#each series as s, i}<linearGradient id={`${uid}-fill-${i}`} x1="0" y1="0" x2="0" y2="1">
              <stop offset="0%" stop-color={s.color} stop-opacity=".24" /><stop
                offset="85%"
                stop-color={s.color}
                stop-opacity=".035"
              /><stop offset="100%" stop-color={s.color} stop-opacity="0" />
            </linearGradient>{/each}
        </defs>
        {#each [0, 0.25, 0.5, 0.75, 1] as f}<line
            x1="0"
            x2={width}
            y1={height * f}
            y2={height * f}
            class="plot-grid"
          />{/each}
        {#each series as s, i}{#each segments(s.key) as group}<path
              d={`${group.line} L ${group.points.at(-1)!.x} ${height} L ${group.points[0].x} ${height} Z`}
              fill={`url(#${uid}-fill-${i})`}
            /><path
              d={group.line}
              fill="none"
              stroke={s.color}
              stroke-width="2.4"
              stroke-linecap="round"
              vector-effect="non-scaling-stroke"
            />{#if group.points.length === 1}<circle
                cx={group.points[0].x}
                cy={group.points[0].y}
                r="3"
                fill={s.color}
              />{/if}{/each}{/each}
        {#if hovered !== null && active}<line
            x1={x(active.time)}
            x2={x(active.time)}
            y1="0"
            y2={height}
            class="plot-crosshair"
          />{#each series as s}{#if active.metrics?.[s.key] !== undefined}<circle
                cx={x(active.time)}
                cy={y(active.metrics[s.key])}
                r="4"
                fill={s.color}
                stroke="var(--panel)"
                stroke-width="2"
              />{/if}{/each}{/if}
      </svg>
      <div
        class="plot-input"
        role="slider"
        tabindex="0"
        aria-label={`Inspect ${label}`}
        aria-valuemin="0"
        aria-valuemax={Math.max(0, points.length - 1)}
        aria-valuenow={hovered ?? Math.max(0, points.length - 1)}
        aria-valuetext={active
          ? `${time(active.time)}: ${series.map((s) => `${s.label} ${format(active.metrics?.[s.key])}${unit}`).join(', ')}`
          : 'Collecting samples'}
        onpointermove={pointer}
        onpointerleave={() => (hovered = null)}
        onblur={() => (hovered = null)}
        onkeydown={(e) => {
          if (['ArrowLeft', 'ArrowRight', 'Home', 'End', 'Escape'].includes(e.key)) {
            e.preventDefault();
            hovered =
              e.key === 'Escape'
                ? null
                : e.key === 'Home'
                  ? 0
                  : e.key === 'End'
                    ? points.length - 1
                    : Math.max(
                        0,
                        Math.min(
                          points.length - 1,
                          (hovered ?? points.length - 1) + (e.key === 'ArrowLeft' ? -1 : 1),
                        ),
                      );
          }
        }}
      ></div>
      {#if !hasValues}<div class="plot-empty">Collecting history…</div>{/if}
      {#if hovered !== null && active}<div
          class="plot-tooltip"
          style:left={`${Math.min(80, Math.max(4, (x(active.time) / width) * 100))}%`}
        >
          <b>{new Date(active.time).toLocaleTimeString()}</b>{#each series as s}<span>
              <i style:background={s.color}></i>{s.label}<strong>
                {active.metrics?.[s.key] === undefined
                  ? '—'
                  : byteScale
                    ? bytes(active.metrics[s.key])
                    : new Intl.NumberFormat('en-US', { maximumFractionDigits: 2 }).format(
                        active.metrics[s.key],
                      )}{unit}
              </strong>
            </span>{/each}
        </div>{/if}
      <div class="plot-x">
        {#each [0, 0.25, 0.5, 0.75, 1] as f}<span>
            {points.length ? time(start + (end - start) * f) : '—'}
          </span>{/each}
      </div>
    </div>
  </div>
</div>

<style>
  .diagnostic-chart {
    padding: 0 28px 18px;
  }
  .series-readout {
    display: flex;
    gap: 42px;
    margin: 8px 0 32px;
    flex-wrap: wrap;
  }
  .series-readout > div {
    min-width: 125px;
  }
  .series-label {
    display: flex;
    align-items: center;
    gap: 8px;
    font-size: 12px;
    color: var(--muted);
    margin-bottom: 10px;
  }
  .series-label i {
    width: 7px;
    height: 7px;
    border-radius: 2px;
  }
  .series-readout strong {
    font-size: 30px;
    letter-spacing: -1px;
    font-weight: 400;
    font-family: 'IBM Plex Mono', monospace;
  }
  .series-readout small {
    font-size: 12px;
    letter-spacing: 0;
    color: var(--muted);
    margin-left: 7px;
    font-family: 'IBM Plex Sans', sans-serif;
  }
  .plot-layout {
    display: flex;
    gap: 14px;
  }
  .plot-y {
    width: 48px;
    flex-shrink: 0;
    height: 250px;
    display: flex;
    justify-content: space-between;
    flex-direction: column;
    text-align: right;
    color: var(--faint);
    font-size: 11px;
  }
  .plot-y span {
    transform: translateY(-50%);
  }
  .plot-main {
    position: relative;
    flex: 1;
    min-width: 0;
  }
  .plot-main svg {
    width: 100%;
    height: 250px;
    display: block;
    overflow: visible;
  }
  .plot-grid {
    stroke: var(--border);
    stroke-dasharray: 3 6;
    stroke-width: 1;
    vector-effect: non-scaling-stroke;
  }
  .plot-crosshair {
    stroke: var(--faint);
    stroke-dasharray: 3 4;
    stroke-width: 1;
    vector-effect: non-scaling-stroke;
  }
  .plot-input {
    position: absolute;
    inset: 0 0 32px;
    cursor: crosshair;
  }
  .plot-x {
    height: 32px;
    padding-top: 17px;
    display: flex;
    justify-content: space-between;
    color: var(--faint);
    font-size: 11px;
  }
  .plot-empty {
    position: absolute;
    inset: 0;
    display: grid;
    place-items: center;
    font-size: 13px;
    color: var(--muted);
    pointer-events: none;
  }
  .plot-tooltip {
    position: absolute;
    top: 8px;
    transform: translateX(-50%);
    background: var(--panel);
    border: 1px solid var(--border);
    box-shadow: 0 6px 24px #0002;
    border-radius: 8px;
    padding: 12px 14px;
    min-width: 190px;
    pointer-events: none;
    z-index: 2;
    font-size: 11px;
  }
  .plot-tooltip > b {
    display: block;
    margin-bottom: 10px;
    color: var(--muted);
    font-weight: 400;
  }
  .plot-tooltip span {
    display: flex;
    gap: 7px;
    align-items: center;
    margin-top: 8px;
  }
  .plot-tooltip i {
    width: 6px;
    height: 6px;
    border-radius: 50%;
  }
  .plot-tooltip strong {
    font-family: 'IBM Plex Mono', monospace;
    font-weight: 400;
    margin-left: auto;
    padding-left: 12px;
  }
  @media (min-width: 1600px) {
    .plot-main svg,
    .plot-y {
      height: 290px;
    }
    .series-readout strong {
      font-size: 36px;
    }
  }
  @media (max-width: 600px) {
    .diagnostic-chart {
      padding: 0 16px 12px;
    }
    .series-readout {
      gap: 20px;
      margin-bottom: 26px;
    }
    .series-readout strong {
      font-size: 24px;
    }
    .series-readout > div {
      min-width: 100px;
    }
    .plot-main svg,
    .plot-y {
      height: 210px;
    }
    .plot-y {
      width: 36px;
    }
    .plot-x span:nth-child(even) {
      display: none;
    }
    .plot-tooltip {
      min-width: 150px;
    }
    .plot-layout {
      gap: 10px;
    }
  }
</style>
