<script lang="ts">
  // A small, deterministic pen-plotter drawing. Decorative, not telemetry.
  const ribbons = Array.from({ length: 22 }, (_, row) => {
    const points = Array.from({ length: 100 }, (_, i) => {
      const t = i / 99;
      const x = 12 + t * 272;
      const wave = Math.sin(t * Math.PI * 2 - 1.2 + row * 0.075);
      const envelope = Math.sin(t * Math.PI);
      const y = 66 + wave * envelope * 43 + (row - 10.5) * (1.6 + envelope * 0.7);
      return `${i === 0 ? 'M' : 'L'}${x.toFixed(2)},${y.toFixed(2)}`;
    });
    return points.join(' ');
  });
</script>

<div class="signal-art" aria-hidden="true">
  <svg viewBox="0 0 300 148" fill="none">
    <path class="art-registration" d="M4 18V6h12M284 6h12v12M4 130v12h12m268 0h12v-12" />
    <path class="art-axis" d="M0 74h300M150 0v148" />
    {#each ribbons as d, i}<path {d} class="art-ribbon" style:opacity={0.28 + i / 40} />{/each}
    <circle cx="12" cy="49" r="3" class="art-terminal" />
    <circle cx="284" cy="83" r="3" class="art-terminal" />
    <text x="8" y="137">TX</text><text x="274" y="18">RX</text>
  </svg>
  <span class="art-caption">a little order in the chaos.</span>
</div>
