<script lang="ts">
  let {
    name,
    size = 20,
    stroke = 1.8,
  }: { name: string; size?: number; stroke?: number } = $props();
  const paths: Record<string, string[]> = {
    overview: [
      'M5 3h3a2 2 0 0 1 2 2v3a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2V5a2 2 0 0 1 2-2Zm11 0h3a2 2 0 0 1 2 2v3a2 2 0 0 1-2 2h-3a2 2 0 0 1-2-2V5a2 2 0 0 1 2-2ZM5 14h3a2 2 0 0 1 2 2v3a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2v-3a2 2 0 0 1 2-2Zm11 0h3a2 2 0 0 1 2 2v3a2 2 0 0 1-2 2h-3a2 2 0 0 1-2-2v-3a2 2 0 0 1 2-2Z',
    ],
    sessions: [
      'M15 7a4 4 0 1 1-8 0 4 4 0 0 1 8 0ZM3 21v-2a6 6 0 0 1 6-6h4a6 6 0 0 1 6 6v2M18 4a4 4 0 0 1 0 7m3 10v-2a7 7 0 0 0-2-5',
    ],
    subscriptions: ['m12 3 9 5-9 5-9-5 9-5Zm-9 9 9 5 9-5M3 16l9 5 9-5'],
    nodes: [
      'M5 3h14a2 2 0 0 1 2 2v4a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2V5a2 2 0 0 1 2-2Zm0 10h14a2 2 0 0 1 2 2v4a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2v-4a2 2 0 0 1 2-2ZM7 7h.01M7 17h.01M15 7h2m-2 10h2',
    ],
    hash: ['M5 9h14M4 15h14M11 3 7 21M17 3l-4 18'],
    alert: [
      'm10.3 3.9-8 14A2 2 0 0 0 4 21h16a2 2 0 0 0 1.7-3.1l-8-14a2 2 0 0 0-3.4 0ZM12 9v4m0 4h.01',
    ],
    connected: [
      'M10 13a5 5 0 0 0 7 0l3-3a5 5 0 0 0-7-7l-2 2M14 11a5 5 0 0 0-7 0l-3 3a5 5 0 0 0 7 7l2-2',
    ],
    disconnected: [
      'm3 3 18 18M9 15l-2 2m8-8 2-2M7 11l-3 3a5 5 0 0 0 7 7l2-2M11 5l2-2a5 5 0 0 1 7 7l-1 1',
    ],
    queues: ['M4 6h16M4 12h16M4 18h10m3-3 3 3-3 3'],
    health: ['M3 12h4l3-7 4 14 3-7h4'],
    chevron: ['m9 5 7 7-7 7'],
    chevrons: ['m5 6 6 6-6 6m8-12 6 6-6 6'],
    down: ['m6 9 6 6 6-6'],
    'sidebar-collapse': [
      'M4 3h16a1 1 0 0 1 1 1v16a1 1 0 0 1-1 1H4a1 1 0 0 1-1-1V4a1 1 0 0 1 1-1ZM9 3v18m7-12-3 3 3 3',
    ],
    'sidebar-expand': [
      'M4 3h16a1 1 0 0 1 1 1v16a1 1 0 0 1-1 1H4a1 1 0 0 1-1-1V4a1 1 0 0 1 1-1ZM9 3v18m4-12 3 3-3 3',
    ],
    search: ['M21 21l-5-5', 'M18 10a8 8 0 1 1-16 0 8 8 0 0 1 16 0'],
    sun: [
      'M12 2v2m0 16v2M2 12h2m16 0h2M5 5l1.5 1.5m11 11L19 19M5 19l1.5-1.5m11-11L19 5',
      'M16 12a4 4 0 1 1-8 0 4 4 0 0 1 8 0',
    ],
    moon: ['M21 13A9 9 0 0 1 11 3a9 9 0 1 0 10 10Z'],
    book: ['M12 5v16M3 3h5a4 4 0 0 1 4 2 4 4 0 0 1 4-2h5v16h-5a4 4 0 0 0-4 2 4 4 0 0 0-4-2H3V3Z'],
    receive: ['M12 3v12m-4-4 4 4 4-4M4 15v4a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2v-4'],
    send: ['M12 15V3m-4 4 4-4 4 4M4 15v4a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2v-4'],
    archive: ['M4 3h16v4H4zM5 7v12a2 2 0 0 0 2 2h10a2 2 0 0 0 2-2V7M9 12h6'],
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
  data-icon={name}
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
