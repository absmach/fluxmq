<script lang="ts">
  import { onMount } from 'svelte';
  import { compact, type BrokerNode } from './types';
  let { nodes, oninspect }: { nodes: BrokerNode[]; oninspect: (id: string) => void } = $props();
  let canvas: HTMLCanvasElement;
  let host: HTMLDivElement;
  let selected = $state('');
  let query = $state('');
  let zoom = $state(1);
  let x = 0,
    y = 0,
    width = 1,
    height = 1;
  let frame = 0,
    mounted = false,
    fitted = false;
  const cardW = 224,
    cardH = 114,
    gap = 24;
  let ordered = $derived([...nodes].sort((a, b) => a.id.localeCompare(b.id)));
  let columns = $derived(Math.max(1, Math.ceil(Math.sqrt(nodes.length * 1.5))));
  let active = $derived(nodes.find((n) => n.id === selected));
  let matches = $derived(
    query.trim()
      ? ordered.filter((n) => n.id.toLowerCase().includes(query.trim().toLowerCase())).slice(0, 8)
      : [],
  );
  const position = (index: number) => ({
    x: (index % columns) * (cardW + gap),
    y: Math.floor(index / columns) * (cardH + gap),
  });

  function redraw() {
    if (mounted && !frame)
      frame = requestAnimationFrame(() => {
        frame = 0;
        draw();
      });
  }
  function fit() {
    const cols = Math.min(columns, nodes.length);
    const w = Math.max(cardW, cols * (cardW + gap) - gap);
    const h = Math.max(cardH, Math.ceil(nodes.length / columns) * (cardH + gap) - gap);
    zoom = Math.max(0.02, Math.min(1, (width - 48) / w, (height - 48) / h));
    x = (width - w * zoom) / 2;
    y = (height - h * zoom) / 2;
    redraw();
  }
  function scale(factor: number, cx = width / 2, cy = height / 2) {
    const next = Math.min(2.5, Math.max(0.02, zoom * factor));
    x = cx - ((cx - x) * next) / zoom;
    y = cy - ((cy - y) * next) / zoom;
    zoom = next;
    redraw();
  }
  function focusNode(id: string) {
    selected = id;
    query = '';
    const p = position(ordered.findIndex((n) => n.id === id));
    zoom = Math.min(1, (width - 32) / cardW);
    x = width / 2 - (p.x + cardW / 2) * zoom;
    y = height / 2 - (p.y + cardH / 2) * zoom;
    redraw();
  }
  function draw() {
    const ctx = canvas.getContext('2d');
    if (!ctx) return;
    const dpr = window.devicePixelRatio || 1;
    ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
    ctx.clearRect(0, 0, width, height);
    const styles = getComputedStyle(document.documentElement);
    const color = (key: string) => styles.getPropertyValue(key).trim();
    ctx.save();
    ctx.translate(x, y);
    ctx.scale(zoom, zoom);
    ordered.forEach((n, i) => {
      const p = position(i);
      if (
        p.x * zoom + x > width ||
        (p.x + cardW) * zoom + x < 0 ||
        p.y * zoom + y > height ||
        (p.y + cardH) * zoom + y < 0
      )
        return;
      ctx.fillStyle = color('--panel');
      ctx.strokeStyle = color(n.id === selected ? '--accent' : '--border');
      ctx.lineWidth = n.id === selected ? 2 / zoom : 1 / zoom;
      ctx.beginPath();
      ctx.roundRect(p.x, p.y, cardW, cardH, 8);
      ctx.fill();
      ctx.stroke();
      ctx.fillStyle = n.healthy ? color('--green') || '#62cba8' : '#d99356';
      ctx.beginPath();
      ctx.arc(p.x + 19, p.y + 23, 3.5, 0, Math.PI * 2);
      ctx.fill();
      if (zoom < 0.28) return;
      ctx.save();
      ctx.beginPath();
      ctx.rect(p.x + 12, p.y + 8, cardW - 24, cardH - 16);
      ctx.clip();
      ctx.fillStyle = color('--muted');
      ctx.font = '12px "IBM Plex Sans", sans-serif';
      ctx.fillText(n.healthy ? 'Responding' : 'Unavailable', p.x + 30, p.y + 27);
      if (n.leader) {
        ctx.textAlign = 'right';
        ctx.fillText('Leader', p.x + cardW - 16, p.y + 27);
        ctx.textAlign = 'left';
      }
      ctx.fillStyle = color('--text');
      ctx.font = '500 16px "IBM Plex Sans", sans-serif';
      let label = n.id;
      while (label.length > 1 && ctx.measureText(label).width > cardW - 32)
        label = label.slice(0, -1);
      if (label !== n.id) label = label.slice(0, -1) + '…';
      ctx.fillText(label, p.x + 16, p.y + 59);
      ctx.fillStyle = color('--muted');
      ctx.font = '12px "IBM Plex Mono", monospace';
      ctx.fillText(
        `${compact(n.connections)} clients · ${compact(n.incoming)} msg/s`,
        p.x + 16,
        p.y + 91,
      );
      ctx.restore();
    });
    ctx.restore();
  }
  const pointers = new Map<number, { x: number; y: number }>();
  let start = { x: 0, y: 0 },
    dragged = false;
  function point(e: PointerEvent | WheelEvent) {
    const rect = canvas.getBoundingClientRect();
    return { x: e.clientX - rect.left, y: e.clientY - rect.top };
  }
  function down(e: PointerEvent) {
    if (e.button !== 0) return;
    canvas.focus();
    canvas.setPointerCapture(e.pointerId);
    start = point(e);
    pointers.set(e.pointerId, start);
    dragged = pointers.size > 1;
  }
  function move(e: PointerEvent) {
    const previous = pointers.get(e.pointerId);
    if (!previous) return;
    const next = point(e);
    if (pointers.size === 2) {
      const other = [...pointers.entries()].find(([id]) => id !== e.pointerId)![1];
      const oldDistance = Math.hypot(previous.x - other.x, previous.y - other.y);
      const distance = Math.hypot(next.x - other.x, next.y - other.y);
      if (oldDistance > 0)
        scale(distance / oldDistance, (previous.x + other.x) / 2, (previous.y + other.y) / 2);
      x += (next.x - previous.x) / 2;
      y += (next.y - previous.y) / 2;
      dragged = true;
    } else {
      if (Math.hypot(next.x - start.x, next.y - start.y) > 4) dragged = true;
      x += next.x - previous.x;
      y += next.y - previous.y;
    }
    pointers.set(e.pointerId, next);
    redraw();
  }
  function up(e: PointerEvent) {
    if (!pointers.has(e.pointerId)) return;
    if (!dragged && e.type === 'pointerup') {
      const p = point(e),
        wx = (p.x - x) / zoom,
        wy = (p.y - y) / zoom;
      const i = ordered.findIndex((_, index) => {
        const pos = position(index);
        return wx >= pos.x && wx <= pos.x + cardW && wy >= pos.y && wy <= pos.y + cardH;
      });
      selected = ordered[i]?.id ?? '';
      redraw();
    }
    pointers.delete(e.pointerId);
  }
  function key(e: KeyboardEvent) {
    if (e.key === '+' || e.key === '=') scale(1.25);
    else if (e.key === '-') scale(0.8);
    else if (e.key === 'Home') fit();
    else if (e.key === 'Escape') selected = '';
    else if (e.key === 'Enter' && active) oninspect(active.id);
    else if (e.key.startsWith('Arrow')) {
      x += e.key === 'ArrowLeft' ? 48 : e.key === 'ArrowRight' ? -48 : 0;
      y += e.key === 'ArrowUp' ? 48 : e.key === 'ArrowDown' ? -48 : 0;
    } else return;
    e.preventDefault();
    redraw();
  }
  $effect(() => {
    nodes;
    selected;
    if (mounted) {
      if (!fitted && nodes.length) {
        fitted = true;
        fit();
      }
      redraw();
    }
  });
  onMount(() => {
    mounted = true;
    const resize = new ResizeObserver(() => {
      const oldWidth = width,
        oldHeight = height;
      width = host.clientWidth;
      height = host.clientHeight;
      const dpr = window.devicePixelRatio || 1;
      canvas.width = Math.round(width * dpr);
      canvas.height = Math.round(height * dpr);
      if (!fitted && nodes.length) {
        fitted = true;
        fit();
      } else {
        x += (width - oldWidth) / 2;
        y += (height - oldHeight) / 2;
        redraw();
      }
    });
    resize.observe(host);
    const theme = new MutationObserver(redraw);
    theme.observe(document.documentElement, { attributes: true, attributeFilter: ['data-theme'] });
    const wheel = (e: WheelEvent) => {
      e.preventDefault();
      const p = point(e);
      scale(Math.exp(-Math.max(-100, Math.min(100, e.deltaY)) * 0.004), p.x, p.y);
    };
    canvas.addEventListener('wheel', wheel, { passive: false });
    document.fonts.ready.then(redraw);
    return () => {
      mounted = false;
      resize.disconnect();
      theme.disconnect();
      cancelAnimationFrame(frame);
      canvas.removeEventListener('wheel', wheel);
    };
  });
</script>

<section class="panel node-map">
  <div class="map-toolbar">
    <div>
      <h2>Node map</h2>
      <span class="muted">{nodes.filter((n) => n.healthy).length} / {nodes.length} responding</span>
    </div>
    <div class="map-controls">
      <div class="node-search">
        <input
          aria-label="Find node"
          placeholder="Find node…"
          bind:value={query}
          onkeydown={(e) => {
            if (e.key === 'Enter' && matches[0]) focusNode(matches[0].id);
            if (e.key === 'Escape') query = '';
          }}
        />
        {#if query.trim()}<div class="node-results">
            {#each matches as n}<button onclick={() => focusNode(n.id)}>{n.id}</button>{:else}<span>
                No matching nodes
              </span>{/each}
          </div>{/if}
      </div>
      <div class="zoom-controls">
        <button aria-label="Zoom out" title="Zoom out" onclick={() => scale(0.8)}>−</button><output
          aria-label="Zoom level"
        >
          {Math.round(zoom * 100)}%
        </output><button aria-label="Zoom in" title="Zoom in" onclick={() => scale(1.25)}>+</button>
      </div>
      <button class="button" onclick={fit}>Fit view</button>
    </div>
  </div>
  <div class="map-viewport" bind:this={host}>
    <canvas
      bind:this={canvas}
      tabindex="0"
      aria-label="Node map. Drag or use arrow keys to pan. Plus and minus zoom, Home fits all nodes. Use Find node or the inventory below to select a broker."
      onpointerdown={down}
      onpointermove={move}
      onpointerup={up}
      onpointercancel={up}
      onlostpointercapture={up}
      onkeydown={key}
    ></canvas>
    {#if !nodes.length}<div class="map-empty">No configured nodes</div>{/if}
  </div>
  <div class="map-footer">
    {#if active}<span class="selection" aria-live="polite">
        <strong>{active.id}</strong> · {active.healthy ? 'Responding' : 'Unavailable'}
      </span><button class="button" onclick={() => oninspect(active.id)}>Inspect node →</button>
    {:else}<span>Drag to pan · Scroll or pinch to zoom</span><span>
        Select a node to inspect
      </span>{/if}
  </div>
</section>

<style>
  .node-map {
    overflow: hidden;
    margin-bottom: 24px;
  }
  .map-toolbar {
    display: flex;
    justify-content: space-between;
    align-items: center;
    gap: 16px;
    padding: 20px 24px;
    flex-wrap: wrap;
  }
  h2 {
    font-size: 1.1rem;
    margin: 0 0 3px;
  }
  .muted {
    font-size: 12px;
  }
  .map-controls,
  .zoom-controls {
    display: flex;
    align-items: center;
    gap: 8px;
  }
  .map-controls {
    flex-wrap: wrap;
  }
  .zoom-controls {
    border: 1px solid var(--border);
    border-radius: 5px;
    gap: 0;
    height: 36px;
  }
  .zoom-controls button {
    width: 32px;
    height: 34px;
    font-size: 20px;
  }
  output {
    font: 11px var(--mono);
    min-width: 42px;
    text-align: center;
    color: var(--muted);
  }
  .node-search {
    position: relative;
  }
  input {
    width: 170px;
    height: 36px;
    border: 1px solid var(--border);
    border-radius: 5px;
    background: var(--bg);
    color: var(--text);
    padding: 0 10px;
    font: inherit;
    font-size: 13px;
  }
  .node-results {
    position: absolute;
    z-index: 2;
    top: 42px;
    left: 0;
    width: 240px;
    max-width: 80vw;
    padding: 5px;
    background: var(--panel);
    border: 1px solid var(--border);
    border-radius: 5px;
    box-shadow: 0 8px 24px #0003;
  }
  .node-results button,
  .node-results span {
    display: block;
    padding: 10px;
    text-align: left;
    width: 100%;
    overflow-wrap: anywhere;
    font-size: 13px;
  }
  button:hover {
    background: var(--accent-bg);
    color: var(--accent);
  }
  .map-viewport {
    height: 400px;
    position: relative;
    background-color: var(--bg);
    background-image: radial-gradient(var(--border) 1px, transparent 1px);
    background-size: 24px 24px;
    border-block: 1px solid var(--border);
  }
  canvas {
    display: block;
    width: 100%;
    height: 100%;
    touch-action: none;
    cursor: grab;
  }
  canvas:active {
    cursor: grabbing;
  }
  canvas:focus-visible {
    outline: 2px solid var(--accent);
    outline-offset: -3px;
  }
  .map-empty {
    position: absolute;
    inset: 0;
    display: grid;
    place-items: center;
    pointer-events: none;
    color: var(--muted);
  }
  .map-footer {
    min-height: 62px;
    padding: 12px 24px;
    display: flex;
    justify-content: space-between;
    align-items: center;
    gap: 12px;
    color: var(--muted);
    font-size: 12px;
  }
  .selection {
    min-width: 0;
    overflow-wrap: anywhere;
  }
  .selection strong {
    color: var(--text);
    font-weight: 500;
  }
  .map-footer button {
    flex-shrink: 0;
  }
  @media (max-width: 600px) {
    .map-toolbar {
      padding: 16px;
    }
    .map-viewport {
      height: 340px;
    }
    .map-footer {
      padding: 12px 16px;
      flex-wrap: wrap;
    }
    .map-controls {
      gap: 8px;
    }
    input {
      width: 100%;
    }
    .node-search {
      width: 100%;
    }
  }
</style>
