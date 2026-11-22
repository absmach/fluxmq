<script lang="ts">
  let {
    name,
    size = 20,
    stroke = 1.7,
  }: { name: string; size?: number; stroke?: number } = $props();
  const paths: Record<string, string[]> = {
    overview: ['M3 3h7v7H3zM14 3h7v7h-7zM3 14h7v7H3zM14 14h7v7h-7z'],
    sessions: ['M8 7V3m8 4V3M6 7h12v5a6 6 0 0 1-12 0V7Zm6 11v4'],
    subscriptions: ['M4 5h16v14H4zM4 9h16M8 13h4m-4 3h7'],
    nodes: ['M3 3h7v6H3zM14 15h7v6h-7zM3 15h7v6H3zM6.5 9v6M6.5 12h11v3'],
    health: ['M3 12h4l3-8 4 16 3-8h4'],
    chevron: ['m9 5 7 7-7 7'],
    down: ['m6 9 6 6 6-6'],
    search: ['M21 21l-5-5', 'M18 10a8 8 0 1 1-16 0 8 8 0 0 1 16 0'],
    sun: [
      'M12 2v2m0 16v2M2 12h2m16 0h2M5 5l1.5 1.5m11 11L19 19M5 19l1.5-1.5m11-11L19 5',
      'M16 12a4 4 0 1 1-8 0 4 4 0 0 1 8 0',
    ],
    moon: ['M21 13A9 9 0 0 1 11 3a9 9 0 1 0 10 10Z'],
    book: ['M12 5v16M3 3h5a4 4 0 0 1 4 2 4 4 0 0 1 4-2h5v16h-5a4 4 0 0 0-4 2 4 4 0 0 0-4-2H3V3Z'],
    download: ['M12 3v12m-5-5 5 5 5-5M4 16v5h16v-5'],
    pause: ['M8 5v14M16 5v14'],
    play: ['m7 4 14 8-14 8V4Z'],
    check: ['m5 12 4 4L19 6'],
    close: ['m6 6 12 12M6 18 18 6'],
    menu: ['M4 6h16M4 12h16M4 18h16'],
    trend: ['m3 17 6-6 4 4 8-10m-6 0h6v6'],
    bolt: ['m13 2-9 12h7l-1 8L21 9h-8l1-7Z'],
    globe: ['M21 12a9 9 0 1 1-18 0 9 9 0 0 1 18 0M3 12h18M12 3c5 5 5 13 0 18-5-5-5-13 0-18'],
    clock: ['M21 12a9 9 0 1 1-18 0 9 9 0 0 1 18 0M12 7v5l3 2'],
    copy: ['M9 9h12v12H9zM5 15H3V3h12v2'],
    info: ['M12 11v6m0-10v.1M21 12a9 9 0 1 1-18 0 9 9 0 0 1 18 0'],
    shield: ['m12 3 8 3v6c0 5-8 9-8 9s-8-4-8-9V6l8-3Z', 'm8 12 3 3 5-6'],
    signal: ['M4 20v-4m5 4v-8m5 8V8m5 12V4'],
    terminal: ['m4 6 6 6-6 6m9 0h7'],
    settings: ['M4 7h16M4 17h16M8 4v6m8 4v6'],
    refresh: ['M20 7V3m0 4h-4M4 17v4m0-4h4M5 7a8 8 0 0 1 14-1M5 18a8 8 0 0 0 14-1'],
  };
</script>

<svg
  width={size}
  height={size}
  viewBox="0 0 24 24"
  fill="none"
  stroke="currentColor"
  stroke-width={stroke}
  stroke-linecap="round"
  stroke-linejoin="round"
  aria-hidden="true"
>
  {#each paths[name] || paths.overview as path}<path d={path} />{/each}
</svg>
