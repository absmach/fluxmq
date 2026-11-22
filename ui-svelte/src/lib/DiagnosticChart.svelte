<script lang="ts">
  import { onMount } from 'svelte';
  import Icon from './Icon.svelte';
  import { smoothPath } from './chartCurve.js';
  import { scalePosition, scaleTicks, linearDetails } from './chartScale.js';
  import { compact, bytes, type Point } from './types';
  let {
    points,
    series,
    label,
    unit = ' / s',
    byteScale = false,
    logRates = false,
  }: {
    points: Point[];
    series: {
      key: string;
      label: string;
      color: string;
      unit?: string;
      axis?: 'right';
      dash?: string;
    }[];
    label: string;
    unit?: string;
    byteScale?: boolean;
    logRates?: boolean;
  } = $props();
  const uid = $props.id();
  const width = 960,
    height = 170;
  let hovered = $state<number | null>(null);
  let inspectedIndex = $derived(
    hovered === null ? null : Math.min(hovered, Math.max(0, points.length - 1)),
  );
  let active = $derived(inspectedIndex === null ? points.at(-1) : points[inspectedIndex]);
  let hidden = $state<string[]>([]);
  let menu = $state<HTMLDetailsElement>();
  let visibleSeries = $derived(series.filter((s) => !hidden.includes(s.key)));
  // Visibility controls never change domains, axes, or detail layout.
  let leftSeries = $derived(series.filter((s) => s.axis !== 'right'));
  let rightSeries = $derived(series.filter((s) => s.axis === 'right'));
  let dual = $derived(leftSeries.length > 0 && rightSeries.length > 0);
  let primary = $derived(leftSeries.length ? leftSeries : rightSeries);
  const peak = (key: string) =>
    points.reduce((value, p) => Math.max(value, p.metrics?.[key] ?? 0), 0);
  const maximum = (items: typeof series) => {
    const largest = Math.max(0, ...items.map((s) => peak(s.key)));
    return largest > 0 ? largest * 1.16 : 1;
  };
  let details = $derived(
    linearDetails(
      series.map((s) => ({ ...s, peak: peak(s.key) })),
      logRates,
    ).map((group) => ({
      ...group,
      series: series.filter((s) => group.keys.includes(s.key)),
      visible: visibleSeries.filter((s) => group.keys.includes(s.key)),
    })),
  );
  const zeroInRange = (key: string) =>
    points.some((p) => p.metrics?.[key] !== undefined) && peak(key) === 0;
  let max = $derived(maximum(primary));
  let rightMax = $derived(maximum(rightSeries));
  let leftLog = $derived(logRates && primary[0]?.axis === 'right');
  let rightLog = $derived(logRates && dual);
  let leftTicks = $derived(scaleTicks(max, leftLog));
  let rightTicks = $derived(scaleTicks(rightMax, rightLog));
  const units = (s: (typeof series)[number]) => s.unit ?? unit;
  const detailKind = (items: typeof series) =>
    byteScale ? 'bandwidth' : /\/\s*s/.test(units(items[0])) ? 'rates' : 'counts';
  function toggle(key: string) {
    hidden = hidden.includes(key) ? hidden.filter((k) => k !== key) : [...hidden, key];
  }
  function only(key: string) {
    hidden = series.filter((s) => s.key !== key).map((s) => s.key);
    closeMenu();
  }
  function closeMenu() {
    if (menu?.open) {
      menu.open = false;
      menu.querySelector('summary')?.focus();
    }
  }
  onMount(() => {
    const outside = (e: PointerEvent) => {
      if (menu?.open && !menu.contains(e.target as Node)) menu.open = false;
    };
    const escape = (e: KeyboardEvent) => {
      if (e.key === 'Escape') closeMenu();
    };
    document.addEventListener('pointerdown', outside);
    document.addEventListener('keydown', escape);
    return () => {
      document.removeEventListener('pointerdown', outside);
      document.removeEventListener('keydown', escape);
    };
  });
  let start = $derived(points[0]?.time ?? 0),
    end = $derived(points.at(-1)?.time ?? 1);
  let hasValues = $derived(
    points.some((p) => visibleSeries.some((s) => p.metrics?.[s.key] !== undefined)),
  );
  const x = (time: number) => ((time - start) / Math.max(1, end - start)) * width;
  const y = (value: number, axis?: 'right') => {
    const right = dual && axis === 'right';
    return height * (1 - scalePosition(value, right ? rightMax : max, right ? rightLog : leftLog));
  };
  const time = (t: number) =>
    new Date(t).toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' });
  const format = (v: number | undefined) =>
    v === undefined
      ? '—'
      : byteScale
        ? bytes(v)
        : v > 0 && v < 0.1
          ? new Intl.NumberFormat('en-US', { maximumSignificantDigits: 2 }).format(v)
          : compact(v);
  function segments(key: string, axis?: 'right', detailMax?: number) {
    const groups: { x: number; y: number }[][] = [];
    let group: { x: number; y: number }[] = [];
    points.forEach((p, i) => {
      const value = p.metrics?.[key];
      if (value === undefined || (i > 0 && p.time - points[i - 1].time > 12000)) {
        if (group.length) groups.push(group);
        group = [];
      }
      if (value !== undefined)
        group.push({ x: x(p.time), y: detailMax ? 68 * (1 - value / detailMax) : y(value, axis) });
    });
    if (group.length) groups.push(group);
    return groups.map((g) => ({
      points: g,
      line: smoothPath(g),
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

<div class="diagnostic-chart" class:has-details={details.length > 0}>
  <div class="chart-toolbar">
    <div class="series-readout" aria-label="Chart legend">
      {#each series as s}<button
          class:series-hidden={hidden.includes(s.key)}
          aria-label={`Toggle ${s.label}`}
          aria-pressed={!hidden.includes(s.key)}
          onclick={() => toggle(s.key)}
          title={`${zeroInRange(s.key) ? 'All reported samples are zero. ' : ''}Click to ${hidden.includes(s.key) ? 'show' : 'hide'} ${s.label}`}
        >
          <span class="series-label"><i style:background={s.color}></i>{s.label}</span><strong
            style:color={hidden.includes(s.key) ? 'var(--faint)' : s.color}
          >
            <span class:invisible={hidden.includes(s.key)}>
              {format(active?.metrics?.[s.key])}<small>{units(s)}</small>
            </span>
            {#if hidden.includes(s.key)}<small class="hidden-label">Hidden</small>{/if}
          </strong>
          {#if zeroInRange(s.key)}<small class="zero-note" class:invisible={hidden.includes(s.key)}>
              Zero reported
            </small>{/if}
        </button>{/each}
    </div>
    <details class="series-menu" bind:this={menu}>
      <summary aria-label={`Choose series for ${label}`}>
        <Icon name="settings" size={14} />Series
        <span>{visibleSeries.length}/{series.length}</span><Icon name="down" size={12} />
      </summary>
      <div class="series-popover">
        <div class="series-menu-actions">
          <button onclick={() => (hidden = [])}>Show all</button><button
            onclick={() => (hidden = series.map((s) => s.key))}
          >
            Hide all
          </button>
        </div>
        {#each series as s}<div class="series-option">
            <label>
              <input
                type="checkbox"
                checked={!hidden.includes(s.key)}
                onchange={() => toggle(s.key)}
              /><i style:background={s.color}></i><span>{s.label}<small>{units(s)}</small></span>
            </label><button aria-label={`Show only ${s.label}`} onclick={() => only(s.key)}>
              Only
            </button>
          </div>{/each}
      </div>
    </details>
  </div>
  <div class="axis-labels">
    <span>{primary.length ? units(primary[0]) : ''}{leftLog ? ' · log' : ''}</span>{#if dual}<span>
        {units(rightSeries[0])}{rightLog ? ' · log' : ''}
      </span>{/if}
  </div>
  <div class="plot-layout">
    <div class="plot-y">
      {#each leftTicks as value}<span
          style:top={`${(1 - scalePosition(value, max, leftLog)) * 100}%`}
        >
          {format(value)}
        </span>{/each}
    </div>
    <div class="plot-main">
      <svg
        viewBox={`0 0 ${width} ${height}`}
        preserveAspectRatio="none"
        role="img"
        aria-label={label}
      >
        <defs>
          {#each visibleSeries as s, i}<linearGradient
              id={`${uid}-fill-${i}`}
              x1="0"
              y1="0"
              x2="0"
              y2="1"
            >
              <stop offset="0%" stop-color={s.color} stop-opacity=".16" /><stop
                offset="85%"
                stop-color={s.color}
                stop-opacity=".035"
              /><stop offset="100%" stop-color={s.color} stop-opacity="0" />
            </linearGradient>{/each}
        </defs>
        {#each leftTicks as value}<line
            x1="0"
            x2={width}
            y1={y(value)}
            y2={y(value)}
            class="plot-grid"
          />{/each}
        {#each visibleSeries as s, i (s.key)}{#each segments(s.key, s.axis) as group}<path
              d={`${group.line} L ${group.points.at(-1)!.x} ${height} L ${group.points[0].x} ${height} Z`}
              fill={`url(#${uid}-fill-${i})`}
            />{/each}{/each}
        {#each visibleSeries as s (s.key)}{#each segments(s.key, s.axis) as group}<path
              d={group.line}
              data-series={s.key}
              fill="none"
              stroke={s.color}
              stroke-width="2.1"
              stroke-dasharray={s.dash}
              stroke-linecap="round"
              vector-effect="non-scaling-stroke"
            />
            {#if group.points.length === 1}<circle
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
          />{#each visibleSeries as s}{#if active.metrics?.[s.key] !== undefined}<circle
                cx={x(active.time)}
                cy={y(active.metrics[s.key], s.axis)}
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
        aria-valuenow={inspectedIndex ?? Math.max(0, points.length - 1)}
        aria-valuetext={active
          ? `${time(active.time)}: ${visibleSeries.map((s) => `${s.label} ${format(active.metrics?.[s.key])} ${units(s)}`).join(', ')}`
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
                    ? Math.max(0, points.length - 1)
                    : Math.max(
                        0,
                        Math.min(
                          points.length - 1,
                          (inspectedIndex ?? points.length - 1) + (e.key === 'ArrowLeft' ? -1 : 1),
                        ),
                      );
          }
        }}
      ></div>
      {#if !hasValues}<div class="plot-empty">
          {visibleSeries.length ? 'Collecting history…' : 'Choose a series to display'}
        </div>{/if}
      {#if hovered !== null && active}<div
          class="plot-tooltip"
          style:left={`${Math.min(80, Math.max(4, (x(active.time) / width) * 100))}%`}
        >
          <b>{new Date(active.time).toLocaleTimeString()}</b>{#each visibleSeries as s}<span>
              <i style:background={s.color}></i>{s.label}<strong>
                {active.metrics?.[s.key] === undefined
                  ? '—'
                  : byteScale
                    ? bytes(active.metrics[s.key])
                    : new Intl.NumberFormat('en-US', { maximumSignificantDigits: 6 }).format(
                        active.metrics[s.key],
                      )}
                {units(s)}
              </strong>
            </span>{/each}
        </div>{/if}
      <div class="plot-x">
        {#each [0, 0.25, 0.5, 0.75, 1] as f}<span>
            {points.length ? time(start + (end - start) * f) : '—'}
          </span>{/each}
      </div>
    </div>
    {#if dual}<div class="plot-y plot-y-right" aria-label="Rate axis">
        {#each rightTicks as value}<span
            style:top={`${(1 - scalePosition(value, rightMax, rightLog)) * 100}%`}
          >
            {format(value)}
          </span>{/each}
      </div>{/if}
  </div>
  {#each details as detail, index}
    <div class="linear-detail" data-axis={detail.axis} data-maximum={detail.maximum}>
      <div class="detail-heading">
        <span>Magnified {detailKind(detail.series)} · linear</span><span>
          {detail.series.map((s) => s.label).join(' · ')}
        </span>
      </div>
      <div class="plot-layout">
        <div class="detail-y">
          {#each [1, 0.5, 0] as fraction}<span style:top={`${(1 - fraction) * 100}%`}>
              {format(detail.maximum * fraction)}
            </span>{/each}
        </div>
        <div class="detail-main">
          <svg
            viewBox={`0 0 ${width} 68`}
            preserveAspectRatio="none"
            role="img"
            aria-label={`Magnified ${detailKind(detail.series)}: ${detail.series.map((s) => s.label).join(', ')}`}
            onpointermove={pointer}
            onpointerleave={() => (hovered = null)}
          >
            {#each [0, 0.5, 1] as fraction}<line
                x1="0"
                x2={width}
                y1={fraction * 68}
                y2={fraction * 68}
                class="plot-grid"
              />{/each}
            {#each detail.visible as s (s.key)}{#each segments(s.key, s.axis, detail.maximum) as group}<path
                  d={group.line}
                  data-detail-series={s.key}
                  fill="none"
                  stroke={s.color}
                  stroke-width="2"
                  stroke-dasharray={s.dash}
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
                y2="68"
                class="plot-crosshair"
              />{/if}
          </svg>
          {#if !detail.visible.length}<span class="detail-empty">
              No selected series in this view
            </span>{/if}
          {#if index === details.length - 1}<div class="plot-x">
              {#each [0, 0.25, 0.5, 0.75, 1] as f}<span>
                  {points.length ? time(start + (end - start) * f) : '—'}
                </span>{/each}
            </div>{/if}
        </div>
        {#if dual}<span class="detail-unit">
            {units(detail.series[0])}
          </span>{/if}
      </div>
    </div>
  {/each}
</div>

<style>
  .series-readout .zero-note {
    display: block;
    margin: 3px 0 0;
    font-size: 9px;
    color: var(--faint);
  }
  .has-details .plot-main svg,
  .has-details .plot-y {
    height: 130px;
  }
  .has-details .plot-main .plot-x {
    display: none;
  }
  .has-details .plot-input {
    inset: 0;
  }
  .linear-detail {
    margin-top: 22px;
  }
  .detail-heading {
    display: flex;
    justify-content: space-between;
    gap: 12px;
    flex-wrap: wrap;
    font-size: 10px;
    color: var(--muted);
    margin: 0 0 12px;
  }
  .detail-heading > span:first-child {
    color: var(--text);
  }
  .detail-y {
    position: relative;
    width: 48px;
    height: 68px;
    flex-shrink: 0;
    color: var(--faint);
    font-size: 10px;
    text-align: right;
  }
  .detail-y span {
    position: absolute;
    left: 0;
    right: 0;
    transform: translateY(-50%);
  }
  .invisible {
    visibility: hidden;
  }
  .detail-empty {
    position: absolute;
    top: 24px;
    left: 0;
    right: 0;
    text-align: center;
    font-size: 11px;
    color: var(--muted);
  }
  .detail-main {
    position: relative;
    flex: 1;
    min-width: 0;
  }
  .detail-main svg {
    height: 68px;
    width: 100%;
    display: block;
    overflow: visible;
    cursor: crosshair;
  }
  .detail-unit {
    width: 48px;
    flex-shrink: 0;
    font-size: 10px;
    color: var(--faint);
  }
  @media (max-width: 600px) {
    .detail-y,
    .detail-unit {
      width: 30px;
    }
    .detail-heading {
      font-size: 9px;
    }
    .has-details .plot-main svg,
    .has-details .plot-y {
      height: 115px;
    }
  }

  .diagnostic-chart {
    padding: 0 22px 10px;
  }
  .chart-toolbar {
    display: flex;
    align-items: flex-start;
    justify-content: space-between;
    gap: 16px;
    margin: 0 0 16px;
  }
  .series-readout {
    display: flex;
    gap: 12px 16px;
    flex-wrap: wrap;
    flex: 1;
    min-width: 0;
  }
  .series-readout > button {
    display: flex;
    flex-direction: column;
    align-items: flex-start;
    justify-content: flex-start;
    text-align: left;
    padding: 0;
    min-width: 90px;
    border-radius: 4px;
  }
  .series-label {
    display: flex;
    align-items: center;
    gap: 6px;
    font-size: 11px;
    color: var(--muted);
    margin-bottom: 5px;
  }
  .series-label i {
    width: 6px;
    height: 6px;
    border-radius: 2px;
    flex-shrink: 0;
  }
  .series-readout strong {
    position: relative;
    font-size: 18px;
    letter-spacing: -0.4px;
    font-weight: 400;
    font-family: 'IBM Plex Mono', monospace;
  }
  .series-readout small {
    font-size: 10px;
    letter-spacing: 0;
    color: var(--muted);
    margin-left: 5px;
    font-family: 'IBM Plex Sans', sans-serif;
  }
  .series-readout .hidden-label {
    position: absolute;
    left: 0;
    bottom: 0;
    margin-left: 0;
  }
  .series-hidden {
    opacity: 0.45;
  }
  .series-hidden .series-label {
    text-decoration: line-through;
  }
  .series-menu {
    position: relative;
    flex-shrink: 0;
    font-size: 11px;
    z-index: 5;
  }
  .series-menu summary {
    display: flex;
    align-items: center;
    gap: 7px;
    padding: 8px 10px;
    border: 1px solid var(--border);
    border-radius: 6px;
    cursor: pointer;
    background: var(--panel);
  }
  .series-menu summary::-webkit-details-marker {
    display: none;
  }
  .series-menu summary > span {
    color: var(--muted);
  }
  .series-menu[open] summary {
    border-color: var(--accent);
  }
  .series-popover {
    position: absolute;
    top: calc(100% + 6px);
    right: 0;
    width: 285px;
    max-width: calc(100vw - 40px);
    max-height: 360px;
    overflow: auto;
    padding: 8px;
    background: var(--panel);
    border: 1px solid var(--border);
    border-radius: 9px;
    box-shadow: 0 8px 28px #0002;
  }
  .series-menu-actions {
    display: flex;
    justify-content: space-between;
    padding: 4px 4px 10px;
    border-bottom: 1px solid var(--subtle-border);
    margin-bottom: 4px;
  }
  .series-menu-actions button {
    color: var(--accent);
    font-size: 11px;
  }
  .series-option {
    display: flex;
    align-items: center;
    gap: 8px;
    padding: 8px 4px;
    border-radius: 5px;
  }
  .series-option:hover {
    background: var(--panel-raised);
  }
  .series-option label {
    display: flex;
    align-items: center;
    gap: 8px;
    flex: 1;
    min-width: 0;
    cursor: pointer;
  }
  .series-option input {
    accent-color: var(--accent);
    margin: 0;
  }
  .series-option i {
    width: 6px;
    height: 6px;
    border-radius: 2px;
    flex-shrink: 0;
  }
  .series-option span {
    font-size: 12px;
  }
  .series-option small {
    display: block;
    font-size: 10px;
    color: var(--muted);
    margin-top: 2px;
  }
  .series-option > button {
    color: var(--accent);
    font-size: 10px;
    padding: 4px;
  }
  .axis-labels {
    display: flex;
    justify-content: space-between;
    font-size: 10px;
    color: var(--faint);
    margin: 0 0 12px;
    padding: 0 4px;
  }
  .plot-y-right {
    text-align: left !important;
  }
  .plot-layout {
    display: flex;
    gap: 14px;
  }
  .plot-y {
    width: 48px;
    flex-shrink: 0;
    height: 170px;
    position: relative;
    text-align: right;
    color: var(--faint);
    font-size: 11px;
  }
  .plot-y span {
    position: absolute;
    left: 0;
    right: 0;
    transform: translateY(-50%);
  }
  .plot-main {
    position: relative;
    flex: 1;
    min-width: 0;
  }
  .plot-main svg {
    width: 100%;
    height: 170px;
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
  @media (max-width: 600px) {
    .diagnostic-chart {
      padding: 0 14px 8px;
    }
    .chart-toolbar {
      gap: 10px;
    }
    .series-readout {
      gap: 10px 14px;
    }
    .series-readout > button {
      display: flex;
      flex-direction: column;
      align-items: flex-start;
      justify-content: flex-start;
      min-width: 90px;
    }
    .series-readout strong {
      font-size: 16px;
    }
    .series-menu summary {
      padding: 7px;
      gap: 5px;
    }
    .series-menu summary > span {
      display: none;
    }
    .plot-main svg,
    .plot-y {
      height: 145px;
    }
    .plot-y {
      width: 30px;
      font-size: 10px;
    }
    .plot-x span:nth-child(even) {
      display: none;
    }
    .plot-layout {
      gap: 8px;
    }
    .plot-tooltip {
      min-width: 140px;
      font-size: 10px;
    }
    .series-label {
      font-size: 10px;
    }
  }
</style>
