<script lang="ts">
  import { onMount } from 'svelte';
  import Icon from './lib/Icon.svelte';
  import TrafficChart from './lib/TrafficChart.svelte';
  import {
    number,
    compact,
    bytes,
    uptime,
    protocolLabel,
    type Page,
    type Point,
    type Snapshot,
    type Session,
  } from './lib/types';

  const navigation: { id: Page; label: string; description: string }[] = [
    { id: 'overview', label: 'Overview', description: 'Connections, traffic, and cluster health.' },
    { id: 'sessions', label: 'Sessions', description: 'Every connection. One clear picture.' },
    {
      id: 'subscriptions',
      label: 'Subscriptions',
      description: 'Follow the conversations across your system.',
    },
    {
      id: 'nodes',
      label: 'Cluster nodes',
      description: 'Distributed by design. Connected by FluxMQ.',
    },
    {
      id: 'health',
      label: 'Broker health',
      description: 'A closer look at the systems beneath the signal.',
    },
  ];
  const pageFromURL = (): Page =>
    navigation.find((n) => n.id === location.pathname.split('/').filter(Boolean).at(-1))?.id ??
    'overview';
  let page = $state<Page>(pageFromURL());
  let viewportWidth = $state(window.innerWidth);
  let snapshot = $state<Snapshot | null>(null);
  let histories = $state<Record<string, Point[]>>({});
  let connection = $state<'connecting' | 'connected' | 'reconnecting'>('connecting');
  let scope = $state('all');
  let range = $state(5);
  let paused = $state(false);
  let mobileOpen = $state(false);
  let theme = $state(document.documentElement.dataset.theme ?? 'dark');
  let search = $state('');
  let filter = $state('all');
  let protocol = $state('all');
  let sort = $state('client');
  let tablePage = $state(1);
  let toast = $state('');
  let toastTimer: ReturnType<typeof setTimeout>;
  let detailDialog: HTMLDialogElement;
  let connectionDialog: HTMLDialogElement;
  let detail = $state<Session | null>(null);
  let detailLoading = $state(false);
  let detailError = $state('');
  let detailController: AbortController | undefined;
  let clock = $state(Date.now());
  let newest: Snapshot | null = null;
  let stream: EventSource | undefined;

  let currentNav = $derived(navigation.find((n) => n.id === page)!);
  let nodes = $derived(snapshot?.nodes.filter((n) => scope === 'all' || n.id === scope) ?? []);
  let sessions = $derived(
    snapshot?.sessions.filter((s) => scope === 'all' || s.node_id === scope) ?? [],
  );
  let currentPoint = $derived(snapshot?.points[scope]);
  let points = $derived(
    (histories[scope] ?? []).filter(
      (p) => p.time >= (snapshot?.timestamp ?? Date.now()) - range * 60000,
    ),
  );
  let totals = $derived({
    connections: nodes.reduce((v, n) => v + n.connections, 0),
    subscriptions: nodes.reduce((v, n) => v + n.subscriptions, 0),
    errors: nodes.reduce((v, n) => v + n.errors, 0),
    received: nodes.reduce((v, n) => v + n.received, 0),
    sent: nodes.reduce((v, n) => v + n.sent, 0),
  });
  let healthy = $derived(nodes.length > 0 && nodes.every((n) => n.healthy));
  let age = $derived(snapshot ? Math.max(0, Math.floor((clock - snapshot.timestamp) / 1000)) : 0);
  let stale = $derived(!paused && !!snapshot && age > 12);
  let filteredSessions = $derived(
    sessions
      .filter(
        (s) =>
          (!search || `${s.client_id} ${s.node_id}`.toLowerCase().includes(search.toLowerCase())) &&
          (filter === 'all' || (filter === 'connected') === s.connected) &&
          (protocol === 'all' || s.protocol === protocol),
      )
      .sort((a, b) =>
        sort === 'queued'
          ? b.offline_queue_depth - a.offline_queue_depth
          : a.client_id.localeCompare(b.client_id),
      ),
  );
  let pagedSessions = $derived(filteredSessions.slice((tablePage - 1) * 10, tablePage * 10));
  let subscriptionRows = $derived.by(() => {
    const grouped = new Map<
      string,
      { filter: string; subscriber_count: number; max_qos: number; nodes: string[] }
    >();
    for (const s of snapshot?.subscriptions ?? []) {
      if (scope !== 'all' && s.node_id !== scope) continue;
      const old = grouped.get(s.filter);
      if (old) {
        old.subscriber_count += s.subscriber_count;
        old.max_qos = Math.max(old.max_qos, s.max_qos);
        old.nodes.push(s.node_id);
      } else grouped.set(s.filter, { ...s, nodes: [s.node_id] });
    }
    return [...grouped.values()]
      .filter((s) => s.filter.toLowerCase().includes(search.toLowerCase()))
      .sort((a, b) => b.subscriber_count - a.subscriber_count);
  });
  let protocols = $derived.by(() => {
    const counts = new Map<string, number>();
    for (const s of sessions) counts.set(s.protocol, (counts.get(s.protocol) ?? 0) + 1);
    return [...counts]
      .sort((a, b) => b[1] - a[1])
      .map(([name, count], i) => ({
        name,
        count,
        percent: (count / Math.max(sessions.length, 1)) * 100,
        color: ['var(--accent)', 'var(--mint)', 'var(--violet)', 'var(--muted)'][i % 4],
      }));
  });
  let donut = $derived.by(() => {
    let offset = 0;
    return `conic-gradient(${
      protocols
        .map((p) => {
          const start = offset;
          offset += p.percent;
          return `${p.color} ${start}% ${offset}%`;
        })
        .join(',') || 'var(--border) 0% 100%'
    })`;
  });
  $effect(() => {
    const max = Math.max(1, Math.ceil(filteredSessions.length / 10));
    if (tablePage > max) tablePage = max;
  });

  function navigate(id: Page) {
    page = id;
    history.pushState(null, '', id === 'overview' ? '/dashboard' : `/dashboard/${id}`);
    search = '';
    filter = 'all';
    protocol = 'all';
    tablePage = 1;
    mobileOpen = false;
    document.querySelector('main')?.scrollTo(0, 0);
  }
  function apply(s: Snapshot) {
    snapshot = s;
    if (s.history) histories = s.history;
    else {
      const updated = { ...histories };
      for (const [id, p] of Object.entries(s.points)) {
        const old = updated[id] ?? [];
        if (old.at(-1)?.time !== p.time) updated[id] = [...old, p].slice(-450);
      }
      histories = updated;
    }
    if (scope !== 'all' && !s.nodes.some((n) => n.id === scope)) scope = 'all';
  }
  async function togglePause() {
    paused = !paused;
    if (!paused) {
      if (newest) apply(newest);
      try {
        const response = await fetch('/console-api/snapshot');
        if (response.ok) apply(await response.json());
      } catch {
        /* The SSE connection handles retries. */
      }
    }
  }
  function notify(message: string) {
    toast = message;
    clearTimeout(toastTimer);
    toastTimer = setTimeout(() => (toast = ''), 3500);
  }
  function toggleTheme() {
    theme = theme === 'dark' ? 'light' : 'dark';
    document.documentElement.dataset.theme = theme;
    try {
      localStorage.setItem('fluxmq-theme', theme);
    } catch {}
  }
  async function showSession(s: Session) {
    detail = s;
    detailError = '';
    detailLoading = true;
    detailController?.abort();
    detailController = new AbortController();
    const controller = detailController;
    detailDialog.showModal();
    try {
      const response = await fetch(
        `/console-api/session?id=${encodeURIComponent(s.client_id)}&node=${encodeURIComponent(s.node_id)}`,
        { signal: controller.signal },
      );
      if (!response.ok) throw new Error('Could not load this session. It may have disconnected.');
      detail = await response.json();
    } catch (error) {
      if (!controller.signal.aborted) detailError = (error as Error).message;
    } finally {
      if (!controller.signal.aborted) detailLoading = false;
    }
  }
  function exportCSV() {
    if (!snapshot) return;
    const rows: (string | number)[][] =
      page === 'sessions'
        ? [
            ['Client', 'Node', 'Protocol', 'Status', 'Subscriptions', 'Queued'],
            ...filteredSessions.map((s) => [
              s.client_id,
              s.node_id,
              s.protocol,
              s.connected ? 'connected' : 'disconnected',
              s.subscription_count,
              s.offline_queue_depth,
            ]),
          ]
        : page === 'subscriptions'
          ? [
              ['Topic filter', 'Subscribers', 'Max QoS', 'Nodes'],
              ...subscriptionRows.map((s) => [
                s.filter,
                s.subscriber_count,
                s.max_qos,
                s.nodes.join(';'),
              ]),
            ]
          : [
              ['Time', 'Messages in/s', 'Messages out/s', 'Bandwidth bytes/s', 'Connections'],
              ...points.map((p) => [
                new Date(p.time).toISOString(),
                p.incoming,
                p.outgoing,
                p.bandwidth,
                p.connections,
              ]),
            ];
    const csv = rows
      .map((r) =>
        r
          .map(
            (v) =>
              `"${String(v)
                .replace(/^[=+@-]/, "'$&")
                .replaceAll('"', '""')}"`,
          )
          .join(','),
      )
      .join('\n');
    const url = URL.createObjectURL(new Blob([csv], { type: 'text/csv' }));
    const a = document.createElement('a');
    a.href = url;
    a.download = `fluxmq-${snapshot.mode}-${page}-${new Date().toISOString().slice(0, 10)}.csv`;
    a.click();
    URL.revokeObjectURL(url);
    notify('Export ready.');
  }
  onMount(() => {
    stream = new EventSource('/console-api/stream');
    stream.addEventListener('snapshot', (event) => {
      try {
        const data = JSON.parse((event as MessageEvent).data) as Snapshot;
        newest = data;
        connection = 'connected';
        if (!paused) apply(data);
      } catch {
        connection = 'reconnecting';
      }
    });
    stream.onerror = () => (connection = 'reconnecting');
    const interval = setInterval(() => (clock = Date.now()), 1000);
    const pop = () => {
      page = pageFromURL();
      search = '';
      tablePage = 1;
    };
    window.addEventListener('popstate', pop);
    return () => {
      stream?.close();
      clearInterval(interval);
      clearTimeout(toastTimer);
      detailController?.abort();
      window.removeEventListener('popstate', pop);
    };
  });
</script>

<svelte:window bind:innerWidth={viewportWidth} />
<svelte:head><title>{currentNav.label} · FluxMQ</title></svelte:head>

<div class="app-shell">
  <a class="skip-link" href="#main">Skip to content</a>
  {#if mobileOpen}<button
      class="mobile-backdrop"
      aria-label="Close navigation"
      onclick={() => (mobileOpen = false)}
    ></button>{/if}
  <aside
    id="console-navigation"
    class:open={mobileOpen}
    class="sidebar"
    inert={viewportWidth <= 800 && !mobileOpen}
  >
    <a
      class="brand"
      href="/dashboard"
      onclick={(e) => {
        e.preventDefault();
        navigate('overview');
      }}
      aria-label="FluxMQ overview"
    >
      <img src="/brand/fluxmq-symbol.svg" alt="" width="36" height="36" /><span>
        Flux<span class="brand-mq">MQ</span>
      </span>
    </a>
    <nav aria-label="Main navigation">
      {#each navigation as item}<a
          href={item.id === 'overview' ? '/dashboard' : `/dashboard/${item.id}`}
          class:active={page === item.id}
          aria-current={page === item.id ? 'page' : undefined}
          onclick={(e) => {
            e.preventDefault();
            navigate(item.id);
          }}
        >
          <Icon name={item.id} size={19} /><span>
            {item.label}
          </span>{#if item.id === 'nodes' && snapshot}<span class="nav-count">
              {snapshot.nodes.length}
            </span>{/if}
        </a>{/each}
    </nav>
    <div class="sidebar-bottom">
      <a
        class="utility-link"
        href="https://www.absmach.eu/docs/fluxmq"
        target="_blank"
        rel="noreferrer"
      >
        <Icon name="book" size={18} /><span>Documentation</span>
      </a>
      <button class="utility-link" onclick={toggleTheme}>
        <Icon name={theme === 'dark' ? 'sun' : 'moon'} size={18} /><span>
          {theme === 'dark' ? 'Light appearance' : 'Dark appearance'}
        </span>
      </button>
      <a class="company" href="https://www.absmach.eu/" target="_blank" rel="noreferrer">
        <span class="company-copy">
          <small>A PRODUCT OF</small><img
            src="/brand/abstract-machines.png"
            alt="Abstract Machines"
            width="158"
            height="19"
          />
        </span>
      </a>
    </div>
  </aside>
  <div class="console">
    {#if viewportWidth <= 800}
      <header class="mobile-header">
        <button
          class="icon-button"
          aria-label="Open navigation"
          aria-expanded={mobileOpen}
          aria-controls="console-navigation"
          onclick={() => (mobileOpen = true)}
        >
          <Icon name="menu" />
        </button>
        <span>FluxMQ</span>
      </header>
    {/if}
    <main id="main">
      <div class:overview-heading={page === 'overview'} class="page-heading">
        <div>
          <h1>
            {page === 'overview' ? 'Broker overview' : currentNav.label}
          </h1>
          <p>{currentNav.description}</p>
        </div>
        <div class="heading-actions">
          <span
            class:warning={connection !== 'connected' || stale || !healthy}
            class="status-label"
          >
            <span class="status-dot"></span>{connection === 'connecting'
              ? 'Connecting'
              : connection === 'reconnecting'
                ? 'Reconnecting'
                : stale
                  ? 'Data is stale'
                  : healthy
                    ? 'All systems operational'
                    : 'Broker needs attention'}
          </span><button class="button" onclick={exportCSV} disabled={!snapshot}>
            <Icon name="download" size={16} />Export
          </button>
        </div>
      </div>
      <div class="console-toolbar">
        <div class="scope-control">
          <Icon name="nodes" size={16} /><select
            aria-label="Cluster scope"
            bind:value={scope}
            onchange={() => (tablePage = 1)}
          >
            <option value="all">All nodes</option>{#each snapshot?.nodes ?? [] as n}<option
                value={n.id}
              >
                {n.id}
              </option>{/each}
          </select><Icon name="down" size={14} /><span class="toolbar-divider"></span><span
            class="scope-caption"
          >
            {nodes.length}
            {nodes.length === 1 ? 'node' : 'nodes'} configured
          </span>
        </div>
        <div class="stream-controls">
          {#if snapshot?.mode === 'demo'}<button
              class="demo-badge"
              aria-label="Broker connection details"
              onclick={() => connectionDialog.showModal()}
            >
              DEMO DATA <Icon name="info" size={12} />
            </button>{/if}<span class="stream-time">
            {paused
              ? 'Updates paused'
              : snapshot
                ? `Updated ${age < 2 ? 'just now' : `${age}s ago`}`
                : 'Opening stream…'}
          </span><button
            class:paused
            class="live-control"
            onclick={togglePause}
            disabled={!snapshot}
            aria-label={paused ? 'Resume updates' : 'Pause updates'}
          >
            <Icon name={paused ? 'play' : 'pause'} size={13} />{paused ? 'Paused' : 'Live'}
          </button>
        </div>
      </div>
      {#if snapshot?.warning}<div class="notice" role="alert">
          <Icon name="info" size={18} />{snapshot.warning}
        </div>{/if}
      {#if connection === 'reconnecting' || stale}<div class="notice" role="status">
          <Icon name="refresh" size={18} />Connection interrupted. Showing the last snapshot while
          we reconnect.
        </div>{/if}

      {#if page === 'overview'}
        <section class="metric-grid" aria-label="Key metrics">
          <article class="metric-card">
            <div class="metric-label">Active connections <Icon name="sessions" size={17} /></div>
            <div class="metric-value">
              {snapshot ? number(totals.connections) : '—'}<span class="metric-unit">clients</span>
            </div>
            <div class="metric-bottom">
              <span>
                <i class="tiny-dot"></i> Across {nodes.length}
                {nodes.length === 1 ? 'node' : 'nodes'}
              </span><span class="metric-mini-bars" aria-hidden="true">
                {#each points.slice(-18) as p}<i
                    style:height={`${5 + (p.connections / Math.max(1, totals.connections)) * 18}px`}
                  ></i>{/each}
              </span>
            </div>
          </article>
          <article class="metric-card">
            <div class="metric-label">Messages in <Icon name="signal" size={17} /></div>
            <div class="metric-value">
              {currentPoint ? compact(currentPoint.incoming) : '—'}<span class="metric-unit">
                msg / s
              </span>
            </div>
            <div class="metric-bottom">
              <span class="accent-text">Incoming throughput</span>
              <div class="spark"><TrafficChart points={points.slice(-40)} small /></div>
            </div>
          </article>
          <article class="metric-card">
            <div class="metric-label">Messages out <Icon name="signal" size={17} /></div>
            <div class="metric-value">
              {currentPoint ? compact(currentPoint.outgoing) : '—'}<span class="metric-unit">
                msg / s
              </span>
            </div>
            <div class="metric-bottom">
              <span class="mint-text">Outgoing throughput</span>
              <div class="spark">
                <TrafficChart
                  points={points.slice(-40).map((p) => ({ ...p, incoming: p.outgoing }))}
                  small
                  color="green"
                />
              </div>
            </div>
          </article>
          <article class="metric-card">
            <div class="metric-label">
              Active subscriptions <Icon name="subscriptions" size={17} />
            </div>
            <div class="metric-value">
              {snapshot ? number(totals.subscriptions) : '—'}<span class="metric-unit">
                filters
              </span>
            </div>
            <div class="metric-bottom">
              <span>Messages finding their way</span><span class="metric-decoration">
                <Icon name="subscriptions" size={25} />
              </span>
            </div>
          </article>
        </section>
        <div class="overview-grid">
          <section class="panel traffic-panel">
            <div class="panel-heading">
              <div>
                <h2>Message traffic <span class="live-dot"></span></h2>
                <p>The pulse of your infrastructure.</p>
              </div>
              <div class="segmented" aria-label="Chart time range">
                {#each [1, 5, 15] as minutes}<button
                    class:chosen={range === minutes}
                    onclick={() => (range = minutes)}
                  >
                    {minutes}m
                  </button>{/each}
              </div>
            </div>
            <div class="chart-legend">
              <span>
                <i class="legend-dot blue"></i>Incoming
                <strong>{currentPoint ? number(currentPoint.incoming) : '—'}</strong><small>
                  msg/s
                </small>
              </span><span>
                <i class="legend-dot green"></i>Outgoing
                <strong>{currentPoint ? number(currentPoint.outgoing) : '—'}</strong><small>
                  msg/s
                </small>
              </span>
            </div>
            <TrafficChart {points} />
            <div class="chart-footer">
              <span>
                <Icon name="clock" size={13} />Last {range}
                {range === 1 ? 'minute' : 'minutes'}
              </span><span>
                {snapshot?.mode === 'demo' ? 'Simulated traffic' : 'Observed traffic'} · 2s collection
                interval
              </span>
            </div>
          </section>
          <section class="panel protocol-panel">
            <div class="panel-heading">
              <div>
                <h2>Session mix</h2>
                <p>Different protocols. One platform.</p>
              </div>
              <Icon name="globe" size={18} />
            </div>
            <div class="donut-container">
              <div class="donut" style:background={donut}>
                <div class="donut-hole">
                  <span>{number(sessions.length)}</span><small>
                    {snapshot?.mode === 'demo' ? 'sample clients' : 'listed clients'}
                  </small>
                </div>
              </div>
              <div class="donut-orbit"></div>
            </div>
            <div class="protocol-list">
              {#each protocols as p}<button
                  onclick={() => {
                    navigate('sessions');
                    protocol = p.name;
                  }}
                >
                  <span>
                    <i class="legend-dot" style:background={p.color}></i>{protocolLabel(p.name)}
                  </span><strong>{p.percent.toFixed(0)}<small>%</small></strong>
                </button>{/each}{#if protocols.length === 0}<p class="empty-inline">
                  No sessions in this scope.
                </p>{/if}
            </div>
          </section>
        </div>
        <section class="panel node-panel">
          <div class="panel-heading">
            <div class="heading-inline">
              <h2>Cluster nodes</h2>
              <span class="count-tag">{nodes.length}</span><span class="panel-caption">
                Built to work together.
              </span>
            </div>
            <button class="text-button" onclick={() => navigate('nodes')}>
              Explore cluster <Icon name="nodes" size={15} />
            </button>
          </div>
          {@render nodeTable()}
        </section>
      {:else if page === 'sessions'}
        <section class="summary-strip">
          <div><span>Listed sessions</span><strong>{number(sessions.length)}</strong></div>
          <div>
            <span>Connected</span><strong class="mint-text">
              {number(sessions.filter((s) => s.connected).length)}
            </strong>
          </div>
          <div>
            <span>Disconnected</span><strong>
              {number(sessions.filter((s) => !s.connected).length)}
            </strong>
          </div>
          <div>
            <span>Queued messages</span><strong>
              {number(sessions.reduce((v, s) => v + s.offline_queue_depth, 0))}
            </strong>
          </div>
        </section>
        <section class="panel">
          <div class="table-toolbar">
            <div class="tabs">
              {#each ['all', 'connected', 'disconnected'] as state}<button
                  class:chosen={filter === state}
                  onclick={() => {
                    filter = state;
                    tablePage = 1;
                  }}
                >
                  {state === 'all' ? 'All sessions' : state[0].toUpperCase() + state.slice(1)}
                </button>{/each}
            </div>
            <label class="search-field">
              <Icon name="search" size={16} /><input
                aria-label="Search sessions"
                placeholder="Search clients…"
                bind:value={search}
                oninput={() => (tablePage = 1)}
              />
            </label>
          </div>
          <div class="filter-row">
            <label>
              Protocol <select bind:value={protocol} onchange={() => (tablePage = 1)}>
                <option value="all">
                  All protocols
                </option>{#each [...new Set(sessions.map((s) => s.protocol))] as p}<option
                    value={p}
                  >
                    {protocolLabel(p)}
                  </option>{/each}
              </select>
            </label><label>
              Sort by <select bind:value={sort}>
                <option value="client">Client name</option><option value="queued">
                  Queued messages
                </option>
              </select>
            </label><span>{filteredSessions.length} results</span>
          </div>
          {#if snapshot?.inventory_limited}<div class="notice">
              Showing up to 200 sessions per node. Narrow the scope to inspect a node.
            </div>{/if}
          <div class="table-scroll">
            <table>
              <thead>
                <tr>
                  <th>Client ID</th><th>Status</th><th>Protocol</th><th>Node</th><th
                    class="numeric"
                  >
                    Subscriptions
                  </th><th class="numeric">Queued</th><th><span class="sr-only">Details</span></th>
                </tr>
              </thead><tbody>
                {#each pagedSessions as s}<tr>
                    <td>
                      <button class="client-link" onclick={() => showSession(s)}>
                        <span class="client-icon">
                          <Icon name="terminal" size={16} />
                        </span>{s.client_id}
                      </button>
                    </td><td>
                      <span class:offline={!s.connected} class="state-tag">
                        <i></i>{s.connected ? 'Connected' : 'Disconnected'}
                      </span>
                    </td><td><span class="protocol-tag">{protocolLabel(s.protocol)}</span></td><td
                      class="muted mono"
                    >
                      {s.node_id}
                    </td><td class="numeric">{s.subscription_count}</td><td class="numeric">
                      {s.offline_queue_depth}
                    </td><td>
                      <button
                        class="icon-button"
                        aria-label={`Details for ${s.client_id}`}
                        onclick={() => showSession(s)}
                      >
                        <Icon name="chevron" size={16} />
                      </button>
                    </td>
                  </tr>{:else}<tr>
                    <td colspan="7">
                      <div class="empty-state">
                        <Icon name="search" size={28} />
                        <h3>No matching sessions</h3>
                        <p>Try another client name or adjust your filters.</p>
                        <button
                          class="button"
                          onclick={() => {
                            search = '';
                            filter = 'all';
                            protocol = 'all';
                          }}
                        >
                          Clear filters
                        </button>
                      </div>
                    </td>
                  </tr>{/each}
              </tbody>
            </table>
          </div>
          <div class="table-footer">
            <span>
              {filteredSessions.length ? (tablePage - 1) * 10 + 1 : 0}–{Math.min(
                tablePage * 10,
                filteredSessions.length,
              )} of {filteredSessions.length} sessions{snapshot?.mode === 'demo'
                ? ' · Sample inventory'
                : ''}
            </span>
            <div class="pagination">
              <button class="button" disabled={tablePage <= 1} onclick={() => tablePage--}>
                Previous
              </button><span>
                {tablePage} / {Math.max(1, Math.ceil(filteredSessions.length / 10))}
              </span><button
                class="button"
                disabled={tablePage * 10 >= filteredSessions.length}
                onclick={() => tablePage++}
              >
                Next
              </button>
            </div>
          </div>
        </section>
      {:else if page === 'subscriptions'}
        <section class="summary-strip">
          <div><span>Listed topic filters</span><strong>{subscriptionRows.length}</strong></div>
          <div>
            <span>Subscription bindings</span><strong>
              {number(subscriptionRows.reduce((v, s) => v + s.subscriber_count, 0))}
            </strong>
          </div>
          <div>
            <span>Wildcard filters</span><strong>
              {subscriptionRows.filter((s) => /[+#]/.test(s.filter)).length}
            </strong>
          </div>
        </section>
        <section class="panel">
          <div class="panel-heading">
            <div>
              <h2>Topic directory</h2>
              <p>Explore the routes that connect your applications.</p>
            </div>
            <label class="search-field">
              <Icon name="search" size={16} /><input
                aria-label="Search topic filters"
                placeholder="Search topic filters…"
                bind:value={search}
              />
            </label>
          </div>
          {#if snapshot?.inventory_limited}<div class="notice">
              Inventory is limited to 200 records per node; some topic filters may not be listed.
            </div>{/if}
          <div class="table-scroll">
            <table>
              <thead>
                <tr>
                  <th>Topic filter</th><th>Subscriber distribution</th><th class="numeric">
                    Subscribers
                  </th><th>Max QoS</th><th>Nodes</th>
                </tr>
              </thead><tbody>
                {#each subscriptionRows as s}<tr>
                    <td>
                      <span class="topic-name">
                        <Icon name="subscriptions" size={17} />{s.filter}
                      </span>
                    </td><td>
                      <div class="distribution-track">
                        <span
                          style:width={`${(s.subscriber_count / Math.max(...subscriptionRows.map((s) => s.subscriber_count), 1)) * 100}%`}
                        ></span>
                      </div>
                    </td><td class="numeric">{s.subscriber_count}</td><td>
                      <span class="protocol-tag">QoS {s.max_qos}</span>
                    </td><td>
                      <div class="node-chips">
                        {#each s.nodes as id}<button onclick={() => (scope = id)}>
                            {id}
                          </button>{/each}
                      </div>
                    </td>
                  </tr>{:else}<tr>
                    <td colspan="5">
                      <div class="empty-state">
                        <Icon name="subscriptions" size={28} />
                        <h3>No subscriptions found</h3>
                        <p>Topic filters appear here when clients subscribe.</p>
                      </div>
                    </td>
                  </tr>{/each}
              </tbody>
            </table>
          </div>
          <div class="table-footer">
            <span>
              {subscriptionRows.length} topic filters{snapshot?.mode === 'demo'
                ? ' · Sample inventory'
                : ''}
            </span><span>
              QoS 0 · At most once &nbsp; / &nbsp; QoS 1 · At least once &nbsp; / &nbsp; QoS 2 ·
              Exactly once
            </span>
          </div>
        </section>
      {:else if page === 'nodes'}
        <section class="cluster-hero panel">
          <div>
            <span class="eyebrow">CONNECTED BY DESIGN</span>
            <h2>One cluster.<br /><span>Many possibilities.</span></h2>
            <p>
              {nodes.filter((n) => n.healthy).length} of {nodes.length} configured nodes are responding.
            </p>
            <span class="state-tag">
              <i></i>{snapshot?.mode === 'demo' ? 'Demo cluster' : 'Configured brokers'}
            </span>
          </div>
          <div class="topology" aria-label="Cluster topology">
            <div class="topology-core">
              <img src="/brand/fluxmq-symbol.svg" width="46" height="46" alt="FluxMQ" /><span>
                FluxMQ
              </span>
            </div>
            <div class="topology-lines"></div>
            <div class="topology-nodes">
              {#each nodes as n}<button
                  class:unhealthy={!n.healthy}
                  onclick={() => {
                    scope = n.id;
                    navigate('health');
                  }}
                >
                  <Icon name="nodes" size={20} /><strong>{n.id}</strong><small>
                    {n.leader ? 'Leader' : 'Follower'}
                  </small>
                </button>{/each}
            </div>
          </div>
        </section>
        <section class="panel">
          <div class="panel-heading">
            <h2>Node inventory</h2>
            <span class="muted">Select a node to inspect its health</span>
          </div>
          {@render nodeTable()}
        </section>
      {:else if page === 'health'}
        <div class="health-banner" class:degraded={!healthy || stale || connection !== 'connected'}>
          <span class="health-emblem"><Icon name="shield" size={32} /></span>
          <div>
            <h2>
              {!snapshot
                ? 'Connecting to the broker'
                : connection !== 'connected' || stale
                  ? 'Waiting for a fresh snapshot'
                  : healthy
                    ? 'Your brokers are in good shape.'
                    : 'A broker needs your attention.'}
            </h2>
            <p>
              {snapshot?.mode === 'demo'
                ? 'You’re viewing a simulated environment. Connect a broker to monitor real health.'
                : 'Health is based on successful Admin API responses from configured nodes.'}
            </p>
          </div>
        </div>
        <section class="summary-strip">
          <div>
            <span>Available nodes</span><strong>
              {nodes.filter((n) => n.healthy).length} <small>/ {nodes.length}</small>
            </strong>
          </div>
          <div>
            <span>Protocol & auth errors</span><strong class:mint-text={totals.errors === 0}>
              {number(totals.errors)}
            </strong>
          </div>
          <div><span>Total messages received</span><strong>{compact(totals.received)}</strong></div>
          <div><span>Total messages sent</span><strong>{compact(totals.sent)}</strong></div>
        </section>
        <div class="health-grid">
          {#each nodes as n}<section class="panel health-card">
              <div class="panel-heading">
                <span class="node-identity">
                  <span class="node-icon"><Icon name="nodes" /></span>
                  <h2>{n.id}</h2>
                </span><span class:offline={!n.healthy} class="state-tag">
                  <i></i>{n.healthy ? 'Healthy' : 'Unavailable'}
                </span>
              </div>
              <dl>
                <div>
                  <dt>Admin endpoint</dt>
                  <dd class="mono">{n.address}</dd>
                </div>
                <div>
                  <dt>Role</dt>
                  <dd>{n.leader ? 'Leader' : 'Follower / standalone'}</dd>
                </div>
                <div>
                  <dt>Uptime</dt>
                  <dd>{uptime(n.uptime)}</dd>
                </div>
                <div>
                  <dt>Active connections</dt>
                  <dd>{number(n.connections)}</dd>
                </div>
                <div>
                  <dt>Received / sent</dt>
                  <dd>{bytes(n.bytes_in)} / {bytes(n.bytes_out)}</dd>
                </div>
                <div>
                  <dt>Protocol & auth errors</dt>
                  <dd class:mint-text={n.errors === 0}>{n.errors}</dd>
                </div>
              </dl>
            </section>{/each}
        </div>
      {/if}
    </main>
  </div>
</div>

{#snippet nodeTable()}
  <div class="table-scroll">
    <table class="nodes-table">
      <thead>
        <tr>
          <th>Node</th><th>Status</th><th class="numeric">Connections</th><th>Throughput</th><th>
            Uptime
          </th><th><span class="sr-only">Inspect</span></th>
        </tr>
      </thead><tbody>
        {#each nodes as n}<tr>
            <td>
              <button
                class="node-identity"
                onclick={() => {
                  scope = n.id;
                  navigate('health');
                }}
              >
                <span class="node-icon"><Icon name="nodes" size={18} /></span><span>
                  <strong>{n.id}</strong><small>{n.address}</small>
                </span>{#if n.leader}<span class="leader-tag">LEADER</span>{/if}
              </button>
            </td><td>
              <span class:offline={!n.healthy} class="state-tag">
                <i></i>{n.healthy ? 'Healthy' : 'Unavailable'}
              </span>
            </td><td class="numeric tabular">{number(n.connections)}</td><td>
              <div class="throughput-cell">
                <span>{compact(n.incoming)} <small>msg/s</small></span>
                <div class="throughput-bars" aria-hidden="true">
                  {#each (histories[n.id] ?? []).slice(-12) as p}<i
                      style:height={`${3 + Math.min(20, (p.incoming / Math.max(n.incoming, 1)) * 12)}px`}
                    ></i>{/each}
                </div>
              </div>
            </td><td class="muted tabular">{uptime(n.uptime)}</td><td>
              <button
                class="icon-button"
                aria-label={`Inspect ${n.id}`}
                onclick={() => {
                  scope = n.id;
                  navigate('health');
                }}
              >
                <Icon name="chevron" size={16} />
              </button>
            </td>
          </tr>{:else}<tr>
            <td colspan="6"><div class="empty-state">Waiting for node information…</div></td>
          </tr>{/each}
      </tbody>
    </table>
  </div>
{/snippet}

<dialog
  bind:this={detailDialog}
  class="detail-dialog"
  aria-labelledby="session-dialog-title"
  onclose={() => {
    detailController?.abort();
    detail = null;
  }}
>
  <div class="session-heading">
    <span class="eyebrow">SESSION INSPECTOR</span><button
      class="icon-button"
      aria-label="Close session details"
      onclick={() => detailDialog.close()}
    >
      <Icon name="close" />
    </button>
  </div>
  {#if detail}<div class="session-title">
      <span class="large-client-icon"><Icon name="terminal" size={28} /></span>
      <h2 id="session-dialog-title">{detail.client_id}</h2>
      <span class:offline={!detail.connected} class="state-tag">
        <i></i>{detail.connected ? 'Connected' : 'Disconnected'}
      </span>
    </div>
    <dl class="detail-list">
      <div>
        <dt>Protocol</dt>
        <dd>{protocolLabel(detail.protocol)}</dd>
      </div>
      <div>
        <dt>Node</dt>
        <dd>{detail.node_id}</dd>
      </div>
      <div>
        <dt>Connected since</dt>
        <dd>
          {detail.connected_at ? new Date(detail.connected_at).toLocaleString() : 'Not reported'}
        </dd>
      </div>
      <div>
        <dt>Inflight messages</dt>
        <dd>{detail.inflight_count}</dd>
      </div>
      <div>
        <dt>Offline queue</dt>
        <dd>{detail.offline_queue_depth}</dd>
      </div>
    </dl>
    <h3 class="session-subtitle">
      Subscriptions <span class="count-tag">{detail.subscription_count}</span>
    </h3>
    {#if detailLoading}<p class="muted">Loading session details…</p>{:else if detailError}<div
        class="notice"
        role="alert"
      >
        {detailError}
      </div>{:else}{#each detail.subscriptions ?? [] as sub}<div class="session-subscription">
          <code>{sub.filter}</code><span class="protocol-tag">QoS {sub.qos}</span>
        </div>{:else}<p class="muted">
          This session has no listed subscriptions.
        </p>{/each}{/if}<button
      class="button session-copy"
      onclick={async () => {
        try {
          await navigator.clipboard.writeText(detail!.client_id);
          notify('Client ID copied.');
        } catch {
          notify('Clipboard unavailable. Select and copy the client ID.');
        }
      }}
    >
      <Icon name="copy" size={16} />Copy client ID
    </button>{/if}
</dialog>

<dialog bind:this={connectionDialog} class="connection-dialog">
  <div class="session-heading">
    <span class="eyebrow">BROKER CONNECTION</span><button
      class="icon-button"
      aria-label="Close broker connection details"
      onclick={() => connectionDialog.close()}
    >
      <Icon name="close" />
    </button>
  </div>
  <img src="/brand/fluxmq-symbol.svg" alt="" width="48" height="48" />
  <h2>
    {snapshot?.mode === 'demo' ? 'Demo brokers. Real console.' : 'Connected to your brokers.'}
  </h2>
  <p>
    {snapshot?.mode === 'demo'
      ? 'No broker is connected in this preview. Traffic, clients, and nodes are simulated so you can try the console.'
      : 'Your console reads metrics from the configured FluxMQ brokers. All views in this preview are read-only.'}
  </p>
  <div class="console-detail">
    <span>Data source</span><strong>
      {snapshot?.mode === 'demo' ? 'Demo simulation' : 'FluxMQ Admin API'}
    </strong><span>Configured nodes</span><strong>{snapshot?.nodes.length ?? 0}</strong><span>
      Update transport
    </span><strong>Server-Sent Events</strong>
  </div>
  <button class="button primary" onclick={() => connectionDialog.close()}>Back to console</button>
</dialog>

{#if toast}<div class="toast" role="status">
    <span><Icon name="check" size={17} /></span>{toast}
  </div>{/if}
