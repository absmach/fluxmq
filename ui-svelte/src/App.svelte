<script lang="ts">
  import { onMount, tick } from 'svelte';
  import Icon from './lib/Icon.svelte';
  import { clientURL, subscriptionURL, followLink } from './lib/routes';
  import Pagination from './lib/Pagination.svelte';
  import CumulativeCounters from './lib/CumulativeCounters.svelte';
  import NodeComparison from './lib/NodeComparison.svelte';
  import NodePicker from './lib/NodePicker.svelte';
  import NodeDetails from './lib/NodeDetails.svelte';
  import SubscriptionDetails from './lib/SubscriptionDetails.svelte';
  import ClientFields from './lib/ClientFields.svelte';
  import TrafficChart from './lib/TrafficChart.svelte';
  import {
    number,
    compact,
    bytes,
    uptime,
    protocolLabel,
    sessionProtocol,
    nodeRole,
    type Page,
    type Point,
    type Snapshot,
    type Session,
    type ClientListState,
  } from './lib/types';

  const navigation: { id: Page; label: string }[] = [
    { id: 'overview', label: 'Overview' },
    { id: 'sessions', label: 'Sessions' },
    { id: 'subscriptions', label: 'Subscriptions' },
    { id: 'nodes', label: 'Cluster nodes' },
    { id: 'health', label: 'Broker health' },
  ];
  const pageFromURL = (): Page => {
    if (location.pathname.startsWith('/dashboard/sessions/')) return 'sessions';
    if (location.pathname === '/dashboard/broker-info') return 'health';
    if (location.pathname.startsWith('/dashboard/subscriptions/')) return 'subscriptions';
    return (
      navigation.find((n) => n.id === location.pathname.split('/').filter(Boolean).at(-1))?.id ??
      'overview'
    );
  };
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
  let pageSize = $state(10);
  let toast = $state('');
  let toastTimer: ReturnType<typeof setTimeout>;
  type ViewState = {
    scope: string;
    search: string;
    filter: string;
    protocol: string;
    sort: string;
    tablePage: number;
    pageSize: number;
    scrollY: number;
    subscription?: ClientListState;
  };
  type ParentRoute = { url: string; label: string };
  let returnRoute = $state<ParentRoute | null>(null);
  let subscriptionInitial = $state<ClientListState | undefined>();
  let subscriptionViewState: ClientListState | undefined;
  const localURL = () => location.pathname + location.search;

  let connectionDialog: HTMLDialogElement;
  let detail = $state<Session | null>(null);
  let detailIdentity = $state<{ client_id: string; node_id: string } | null>(null);
  let nodeDetailID = $state('');
  let selectedNode = $derived(snapshot?.nodes.find((n) => n.id === nodeDetailID));
  let subscriptionDetail = $state<{ filter: string; node: string } | null>(null);
  let routeIsDetail = $derived(!!subscriptionDetail || !!detailIdentity);
  let chartMode = $state<'messages' | 'bandwidth' | 'connections'>('messages');
  let detailLoading = $state(false);
  let detailError = $state('');
  let detailController: AbortController | undefined;
  let clock = $state(Date.now());
  let newest: Snapshot | null = null;
  let stream: EventSource | undefined;

  let currentNav = $derived(navigation.find((n) => n.id === page)!);
  let pageTitle = $derived(
    subscriptionDetail?.filter ??
      detailIdentity?.client_id ??
      (page === 'overview'
        ? scope === 'all'
          ? 'Cluster overview'
          : 'Node overview'
        : page === 'health'
          ? scope === 'all'
            ? 'Cluster health'
            : 'Node health'
          : currentNav.label),
  );
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
  let age = $derived(snapshot ? Math.max(0, Math.floor((clock - snapshot.timestamp) / 1000)) : 0);
  let stale = $derived(!paused && !!snapshot && age > 12);
  let filteredSessions = $derived(
    sessions
      .filter(
        (s) =>
          (!search ||
            `${s.client_id} ${s.connection_name ?? ''} ${s.node_id}`
              .toLowerCase()
              .includes(search.toLowerCase())) &&
          (filter === 'all' || (filter === 'connected') === s.connected) &&
          (protocol === 'all' || s.protocol === protocol),
      )
      .sort((a, b) =>
        sort === 'queued'
          ? b.offline_queue_depth - a.offline_queue_depth
          : a.client_id.localeCompare(b.client_id),
      ),
  );
  let pagedSessions = $derived(
    filteredSessions.slice((tablePage - 1) * pageSize, tablePage * pageSize),
  );
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
    const max = Math.max(
      1,
      Math.ceil(
        (page === 'subscriptions' ? subscriptionRows.length : filteredSessions.length) / pageSize,
      ),
    );
    if (tablePage > max) tablePage = max;
  });

  function rememberView() {
    const view: ViewState = {
      scope,
      search,
      filter,
      protocol,
      sort,
      tablePage,
      pageSize,
      scrollY: window.scrollY,
      subscription: subscriptionViewState,
    };
    history.replaceState({ ...history.state, view: $state.snapshot(view) }, '', localURL());
  }
  function routeLabel() {
    return subscriptionDetail
      ? subscriptionDetail.filter
      : detailIdentity
        ? detailIdentity.client_id
        : currentNav.label;
  }
  function go(url: string, parent: ParentRoute | null = null) {
    rememberView();
    history.pushState({ parent }, '', url);
    readRoute();
  }
  function navigate(id: Page, node = 'all') {
    const url = id === 'overview' ? '/dashboard' : `/dashboard/${id}`;
    go(url + (node !== 'all' && id !== 'nodes' ? `?${new URLSearchParams({ node })}` : ''));
  }
  function setScope(id: string) {
    if (subscriptionDetail) {
      history.replaceState(
        { parent: history.state?.parent ?? null },
        '',
        subscriptionURL(subscriptionDetail.filter, id),
      );
      readRoute();
      return;
    }
    scope = id;
    tablePage = 1;
    const url = new URL(location.href);
    if (id === 'all') url.searchParams.delete('node');
    else url.searchParams.set('node', id);
    history.replaceState(history.state, '', url.pathname + url.search);
    rememberView();
  }
  function backToParent() {
    if (returnRoute) history.back();
    else navigate(page);
  }
  async function readRoute(restoring = false) {
    detailController?.abort();
    nodeDetailID = '';
    page = pageFromURL();
    const query = new URLSearchParams(location.search);
    const view = history.state?.view as ViewState | undefined;
    returnRoute = history.state?.parent ?? null;
    scope = view?.scope ?? query.get('node') ?? 'all';
    search = view?.search ?? '';
    filter = view?.filter ?? 'all';
    protocol = view?.protocol ?? 'all';
    sort = view?.sort ?? 'client';
    tablePage = view?.tablePage ?? 1;
    pageSize = view?.pageSize ?? pageSize;
    mobileOpen = false;
    subscriptionInitial = view?.subscription;
    subscriptionViewState = view?.subscription;
    subscriptionDetail = null;
    detailIdentity = null;
    detail = null;
    if (page === 'subscriptions' && query.has('filter')) {
      subscriptionDetail = { filter: query.get('filter')!, node: query.get('node') || 'all' };
      scope = subscriptionDetail.node;
    } else if (page === 'sessions' && query.get('id') && query.get('node')) {
      detailIdentity = { client_id: query.get('id')!, node_id: query.get('node')! };
      loadSession(detailIdentity);
    }
    await tick();
    if (!restoring) document.querySelector<HTMLElement>('main h1')?.focus({ preventScroll: true });
    window.scrollTo(0, view?.scrollY ?? 0);
  }
  function apply(s: Snapshot) {
    s.sessions = s.sessions.map((client) => ({ ...client, protocol: sessionProtocol(client) }));
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
  function showSubscription(filter: string, node = scope) {
    go(subscriptionURL(filter, node), { url: localURL(), label: routeLabel() });
  }
  function openNodeList(id: string, destination: Page) {
    nodeDetailID = '';
    navigate(destination, id);
  }
  function showSession(s: Pick<Session, 'client_id' | 'node_id'>) {
    go(clientURL(s), { url: localURL(), label: routeLabel() });
  }
  async function loadSession(s: Pick<Session, 'client_id' | 'node_id'>) {
    detail =
      snapshot?.sessions.find(
        (client) => client.client_id === s.client_id && client.node_id === s.node_id,
      ) ?? null;
    detailError = '';
    detailLoading = true;
    detailController?.abort();
    detailController = new AbortController();
    const controller = detailController;
    try {
      const response = await fetch(
        `/console-api/session?id=${encodeURIComponent(s.client_id)}&node=${encodeURIComponent(s.node_id)}`,
        { signal: controller.signal },
      );
      if (!response.ok) throw new Error('Could not load this session. It may have disconnected.');
      const loaded: Session = await response.json();
      if (!controller.signal.aborted) detail = { ...loaded, protocol: sessionProtocol(loaded) };
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
        : page === 'nodes' || page === 'health'
          ? [
              [
                'Node',
                'Admin endpoint',
                'Role',
                'Status',
                'Connections',
                'Subscriptions',
                'Messages received',
                'Messages sent',
                'Bytes received',
                'Bytes sent',
                'Uptime seconds',
              ],
              ...(page === 'nodes' ? snapshot.nodes : nodes).map((n) => [
                n.id,
                n.address,
                nodeRole(n),
                n.healthy ? 'healthy' : 'unavailable',
                n.connections,
                n.subscriptions,
                n.received,
                n.sent,
                n.bytes_in,
                n.bytes_out,
                n.uptime,
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
    history.scrollRestoration = 'manual';
    readRoute(true);
    const interval = setInterval(() => (clock = Date.now()), 1000);
    const pop = () => readRoute(true);
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
<svelte:head>
  <title>{pageTitle} · FluxMQ</title>
</svelte:head>

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
        <Icon name={theme === 'dark' ? 'sun' : 'moon'} size={18} />
        <span>{theme === 'dark' ? 'Light appearance' : 'Dark appearance'}</span>
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
    <header class="console-statusbar" aria-label="Console status">
      <div class="header-context">
        {#if viewportWidth <= 800}<button
            class="icon-button"
            aria-label="Open navigation"
            aria-expanded={mobileOpen}
            aria-controls="console-navigation"
            onclick={() => (mobileOpen = true)}
          >
            <Icon name="menu" />
          </button>{/if}
        {#if page === 'nodes'}<span class="inventory-context">
            <Icon name="nodes" size={18} />Cluster inventory
          </span>{:else if detailIdentity}<span class="inventory-context">
            <Icon name="nodes" size={18} />{detailIdentity.node_id}
          </span>{:else}<NodePicker
            nodes={snapshot?.nodes ?? []}
            value={scope}
            onchange={setScope}
          />{/if}
        <button
          class="cluster-count"
          onclick={() => {
            scope = 'all';
            navigate('nodes');
          }}
          aria-label="View all nodes"
        >
          {snapshot ? snapshot.nodes.length : '—'}
          {snapshot?.nodes.length === 1 ? 'node' : 'nodes'}
        </button>
      </div>
      <div class="header-actions">
        <span
          class="cluster-health"
          class:warning={connection !== 'connected' ||
            stale ||
            paused ||
            !snapshot?.nodes.length ||
            snapshot.nodes.some((n) => !n.healthy)}
          role="status"
          title="Reachability of all configured brokers. Paused or interrupted updates show the last collected data."
        >
          <span class="status-dot"></span>{connection === 'connecting'
            ? 'Connecting'
            : connection === 'reconnecting'
              ? 'Reconnecting'
              : paused
                ? 'Paused'
                : stale
                  ? 'Stale data'
                  : !snapshot?.nodes.length
                    ? 'No nodes'
                    : snapshot.nodes.every((n) => n.healthy)
                      ? 'Healthy'
                      : 'Node unavailable'}
        </span>
        {#if snapshot?.mode === 'demo'}<button
            class="header-demo"
            aria-label="Broker connection details"
            title="Simulated broker data"
            onclick={() => connectionDialog.showModal()}
          >
            Demo
          </button>{/if}
        <button
          class="icon-button stream-toggle"
          onclick={togglePause}
          disabled={!snapshot}
          aria-label={paused ? 'Resume updates' : 'Pause updates'}
          title={paused ? 'Resume live updates' : 'Pause live updates'}
        >
          <Icon name={paused ? 'play' : 'pause'} size={16} />
        </button>
      </div>
    </header>
    <main id="main">
      {#if routeIsDetail}
        <nav class="detail-breadcrumb" aria-label="Detail navigation">
          <a
            href={returnRoute?.url ?? `/dashboard/${page}`}
            onclick={(event) => followLink(event, backToParent)}
          >
            <span aria-hidden="true">←</span>
            {returnRoute ? returnRoute.label : currentNav.label}
          </a>
          <span class="breadcrumb-divider" aria-hidden="true">/</span>
          <span>{subscriptionDetail ? 'Subscription' : 'Client'}</span>
        </nav>
      {/if}
      <div class:overview-heading={page === 'overview'} class="page-heading">
        <div>
          <h1 tabindex="-1" class:entity-title={routeIsDetail}>
            {pageTitle}
          </h1>
          {#if scope !== 'all' && !routeIsDetail}<div class="scope-context">
              <span><Icon name="nodes" size={15} />{scope}</span>
              <button class="text-button" onclick={() => setScope('all')}>
                Clear node filter <Icon name="close" size={13} />
              </button>
            </div>{/if}
          {#if detail && !detailLoading && !detailError}<span
              class:offline={!detail.connected}
              class="state-tag client-page-status"
            >
              <i></i>{detail.connected ? 'Connected' : 'Disconnected'} · {protocolLabel(
                detail.protocol,
              )}
            </span>{/if}
        </div>
        <div class="heading-actions">
          {#if detailIdentity}<button
              class="button"
              disabled={detailLoading}
              onclick={() => detailIdentity && loadSession(detailIdentity)}
            >
              <Icon name="refresh" size={16} />Refresh client
            </button>
          {:else if !subscriptionDetail}<button
              class="button"
              onclick={exportCSV}
              disabled={!snapshot}
            >
              <Icon name="download" size={16} />Export
            </button>{/if}
        </div>
      </div>
      {#if snapshot?.warning}<div class="notice" role="alert">
          <Icon name="info" size={18} />{snapshot.warning}
        </div>{/if}
      {#if connection === 'reconnecting' || stale}<div class="notice" role="status">
          <Icon name="refresh" size={18} />Connection interrupted. Showing the last snapshot while
          we reconnect.
        </div>{/if}

      {#if subscriptionDetail}
        {#key subscriptionDetail}<SubscriptionDetails
            filter={subscriptionDetail.filter}
            node={subscriptionDetail.node}
            initial={subscriptionInitial}
            onstate={(state) => (subscriptionViewState = state)}
            onclient={showSession}
          />{/key}
      {:else if detailIdentity}
        <section class="client-detail-page" aria-label="Client details">
          {#if detailLoading}<div class="panel empty-state" role="status">
              Loading client details…
            </div>
          {:else if detailError}<div class="notice" role="alert">
              {detailError}<button
                class="text-button"
                onclick={() => detailIdentity && loadSession(detailIdentity)}
              >
                Retry
              </button>
            </div>
          {:else if detail}<ClientFields
              client={detail}
              onfilter={showSubscription}
              onnode={(id) => (nodeDetailID = id)}
            />{/if}
        </section>
      {:else if page === 'overview'}
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
          </article>
        </section>
        <div class="overview-grid">
          <section class="panel traffic-panel">
            <div class="panel-heading">
              <div>
                <h2>{scope === 'all' ? 'Cluster traffic' : 'Node traffic'}</h2>
                <span class="chart-scope">{scope === 'all' ? 'All nodes combined' : scope}</span>
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
            <div class="chart-mode tabs" aria-label="History metric">
              {#each [{ id: 'messages', label: 'Messages' }, { id: 'bandwidth', label: 'Bandwidth' }, { id: 'connections', label: 'Connections' }] as metric}<button
                  class:chosen={chartMode === metric.id}
                  onclick={() => (chartMode = metric.id as typeof chartMode)}
                >
                  {metric.label}
                </button>{/each}
            </div>
            <div class="chart-legend">
              {#if chartMode === 'connections'}<span>
                  <i class="legend-dot blue"></i>Active connections
                  <strong>{currentPoint ? number(currentPoint.connections) : '—'}</strong>
                </span>
              {:else}<span>
                  <i class="legend-dot blue"></i>Incoming
                  <strong>
                    {currentPoint
                      ? chartMode === 'bandwidth'
                        ? bytes(currentPoint.bytes_in ?? 0)
                        : number(currentPoint.incoming)
                      : '—'}
                  </strong><small>{chartMode === 'bandwidth' ? '/s' : 'msg/s'}</small>
                </span><span>
                  <i class="legend-dot green"></i>Outgoing
                  <strong>
                    {currentPoint
                      ? chartMode === 'bandwidth'
                        ? bytes(currentPoint.bytes_out ?? 0)
                        : number(currentPoint.outgoing)
                      : '—'}
                  </strong><small>{chartMode === 'bandwidth' ? '/s' : 'msg/s'}</small>
                </span>{/if}
            </div>
            <TrafficChart {points} mode={chartMode} label={`${chartMode} history`} />
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
          {#if scope === 'all'}<NodeComparison
              nodes={snapshot?.nodes ?? []}
              onselect={(id) => setScope(id)}
            />{:else}
            <section class="panel protocol-panel">
              <div class="panel-heading">
                <div>
                  <h2>Session mix</h2>
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
                      navigate('sessions', scope);
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
            </section>{/if}
        </div>
        <CumulativeCounters {nodes} loaded={!!snapshot} />
        <section class="panel node-panel">
          <div class="panel-heading">
            <div class="heading-inline">
              <h2>Cluster nodes</h2>
              <span class="count-tag">{snapshot?.nodes.length ?? 0}</span>
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
        {#if scope === 'all' && (snapshot?.nodes.length ?? 0) > 1}<NodeComparison
            kind="sessions"
            nodes={snapshot!.nodes}
            sessions={snapshot!.sessions}
            onselect={setScope}
          />{/if}
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
              Protocol <select
                aria-label="Protocol"
                bind:value={protocol}
                onchange={() => (tablePage = 1)}
              >
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
                      <a
                        class="client-link"
                        href={clientURL(s)}
                        onclick={(event) => followLink(event, () => showSession(s))}
                      >
                        <span class="client-icon">
                          <Icon name="terminal" size={16} />
                        </span>{s.client_id}
                      </a>
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
              {filteredSessions.length ? (tablePage - 1) * pageSize + 1 : 0}–{Math.min(
                tablePage * pageSize,
                filteredSessions.length,
              )} of {filteredSessions.length} sessions{snapshot?.mode === 'demo'
                ? ' · Sample inventory'
                : ''}
            </span>
            <Pagination
              page={tablePage}
              size={pageSize}
              total={filteredSessions.length}
              onpage={(n) => (tablePage = n)}
              onsize={(n) => {
                pageSize = n;
                tablePage = 1;
              }}
            />
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
        {#if scope === 'all' && (snapshot?.nodes.length ?? 0) > 1}<NodeComparison
            kind="subscriptions"
            nodes={snapshot!.nodes}
            subscriptions={snapshot!.subscriptions}
            onselect={setScope}
          />{/if}
        <section class="panel">
          <div class="panel-heading">
            <div>
              <h2>Topic directory</h2>
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
                {#each subscriptionRows.slice((tablePage - 1) * pageSize, tablePage * pageSize) as s}<tr>
                    <td>
                      <a
                        class="topic-name text-button"
                        aria-label={`View clients for ${s.filter}`}
                        href={subscriptionURL(s.filter, scope)}
                        onclick={(event) => followLink(event, () => showSubscription(s.filter))}
                      >
                        <Icon name="subscriptions" size={17} />{s.filter}
                      </a>
                    </td><td>
                      <div class="distribution-track">
                        <span
                          style:width={`${(s.subscriber_count / Math.max(...subscriptionRows.map((s) => s.subscriber_count), 1)) * 100}%`}
                        ></span>
                      </div>
                    </td><td class="numeric">
                      <button
                        class="text-button"
                        aria-label={`View ${s.subscriber_count} clients for ${s.filter}`}
                        onclick={() => showSubscription(s.filter)}
                      >
                        {s.subscriber_count}<Icon name="chevron" size={14} />
                      </button>
                    </td><td>
                      <span class="protocol-tag">QoS {s.max_qos}</span>
                    </td><td>
                      <div class="node-chips">
                        {#each s.nodes as id}<button onclick={() => (nodeDetailID = id)}>
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
            </span><Pagination
              page={tablePage}
              size={pageSize}
              total={subscriptionRows.length}
              onpage={(n) => (tablePage = n)}
              onsize={(n) => {
                pageSize = n;
                tablePage = 1;
              }}
            />
          </div>
        </section>
      {:else if page === 'nodes'}
        <section class="panel">
          <div class="panel-heading">
            <h2>Node inventory</h2>
            <span class="muted">Select a node for details</span>
          </div>
          {@render nodeTable()}
        </section>
      {:else if page === 'health'}
        <section class="summary-strip">
          <div>
            {#if scope === 'all'}<span>Available cluster nodes</span><strong>
                {nodes.filter((n) => n.healthy).length} <small>/ {nodes.length}</small>
              </strong>{:else}<span>Node uptime</span><strong>
                {nodes[0] ? uptime(nodes[0].uptime) : '—'}
              </strong>{/if}
          </div>
          <div>
            <span>Protocol & auth errors</span><strong class:mint-text={totals.errors === 0}>
              {number(totals.errors)}
            </strong>
          </div>
          <div><span>Total messages received</span><strong>{compact(totals.received)}</strong></div>
          <div><span>Total messages sent</span><strong>{compact(totals.sent)}</strong></div>
        </section>
        <section class="panel error-breakdown" aria-label="Error breakdown">
          {#each [{ key: 'protocol_errors', label: 'Protocol' }, { key: 'auth_errors', label: 'Authentication' }, { key: 'authz_errors', label: 'Authorization' }, { key: 'packet_errors', label: 'Packet' }] as error}<div>
              <span>{error.label}</span><strong>
                {number(
                  nodes.reduce(
                    (total, node) => total + (node[error.key as 'protocol_errors'] ?? 0),
                    0,
                  ),
                )}
              </strong>
            </div>{/each}
        </section>
        <div class="health-grid">
          {#each nodes as n}<section class="panel health-card">
              <div class="panel-heading">
                <span class="node-identity">
                  <span class="node-icon"><Icon name="nodes" /></span>
                  <h2>
                    <button class="text-button" onclick={() => (nodeDetailID = n.id)}>
                      {n.id}<Icon name="chevron" size={15} />
                    </button>
                  </h2>
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
                  <dd>{nodeRole(n)}</dd>
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
          <th>Node</th><th>Status</th><th class="numeric">Connections</th><th>Incoming</th><th>
            Uptime
          </th><th><span class="sr-only">Inspect</span></th>
        </tr>
      </thead><tbody>
        {#each snapshot?.nodes ?? [] as n}<tr>
            <td>
              <button
                class="node-identity"
                onclick={() => {
                  nodeDetailID = n.id;
                }}
              >
                <span class="node-icon"><Icon name="nodes" size={18} /></span><span>
                  <strong>{n.id}</strong><small>{n.address}</small>
                </span>{#if n.leader && n.cluster_mode !== false}<span class="leader-tag">
                    LEADER
                  </span>{/if}
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
                class="text-button node-dashboard-link"
                aria-label={`Dashboard for ${n.id}`}
                onclick={() => navigate('overview', n.id)}
              >
                Dashboard
              </button>
              <button
                class="icon-button"
                aria-label={`Inspect ${n.id}`}
                onclick={() => {
                  nodeDetailID = n.id;
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

{#if selectedNode}<NodeDetails
    node={selectedNode}
    onclose={() => (nodeDetailID = '')}
    onoverview={() => openNodeList(selectedNode!.id, 'overview')}
    onclients={() => openNodeList(selectedNode!.id, 'sessions')}
    onsubscriptions={() => openNodeList(selectedNode!.id, 'subscriptions')}
  />{/if}
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
    {snapshot?.mode === 'demo' ? 'Demo environment' : 'Connected to your brokers.'}
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
