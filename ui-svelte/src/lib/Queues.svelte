<script lang="ts">
  import { onMount, untrack, tick } from 'svelte';
  import Icon from './Icon.svelte';
  import { bytes, type BrokerNode } from './types';
  import { followLink } from './routes';
  type Queue = {
    name: string;
    topics?: string[];
    config?: {
      retention?: { maxAge?: string; maxBytes?: string; minMessages?: string };
      maxMessageSize?: number;
      replication?: { group?: string };
    };
  };
  type Group = {
    groupId: string;
    cursor?: { cursor?: string; committed?: string };
    pendingCount?: string;
    consumers?: { consumerId: string; lastHeartbeat?: string }[];
  };
  type Message = {
    offset?: string;
    timestamp?: string;
    value?: string;
    key?: string;
    headers?: Record<string, string>;
  };
  type Pending = {
    offset?: string;
    consumerId?: string;
    deliveredAt?: string;
    deliveryCount?: number;
  };
  let {
    nodes,
    scope,
    identity,
    paused,
    onopen,
    onback,
  }: {
    nodes: BrokerNode[];
    scope: string;
    identity: { name: string; node: string } | null;
    paused: boolean;
    onopen: (name: string, node: string) => void;
    onback: () => void;
  } = $props();
  let rows = $state<(Queue & { node: string })[]>([]),
    next = $state<Record<string, string>>({}),
    failures = $state<string[]>([]);
  let queue = $state<Queue | null>(null),
    info = $state<{ headOffset?: string; tailOffset?: string; messageCount?: string } | null>(null),
    groups = $state<Group[]>([]);
  let loading = $state(false),
    error = $state(''),
    search = $state('');
  let messages = $state<Message[]>([]),
    messageLoading = $state(false),
    messageError = $state(''),
    offset = $state(''),
    inspected = $state<Message | null>(null),
    hasRead = $state(false);
  let pendingGroup = $state(''),
    pending = $state<Pending[]>([]),
    pendingError = $state(''),
    pendingLoading = $state(false);
  let controller: AbortController,
    messageController: AbortController | undefined,
    pendingController: AbortController | undefined;
  let active = $derived(
    nodes.filter((n) => !n.unmonitored && (scope === 'all' || n.id === scope)).map((n) => n.id),
  );
  let routeKey = $derived(JSON.stringify([active, identity]));
  const int = (v?: string) => {
    try {
      return BigInt(v || '0');
    } catch {
      return 0n;
    }
  };
  const count = (v?: string | bigint) =>
    new Intl.NumberFormat('en-US').format(typeof v === 'bigint' ? v : int(v));
  const date = (v?: string) => (v ? new Date(v).toLocaleString() : 'Not reported');
  let head = $derived(int(info?.headOffset)),
    tail = $derived(int(info?.tailOffset));
  const percent = (v: bigint) =>
    tail > head
      ? Math.max(0, Math.min(100, Number(((v - head) * 10000n) / (tail - head)) / 100))
      : 0;
  const waiting = (g: Group) =>
    tail > int(g.cursor?.cursor)
      ? tail - (int(g.cursor?.cursor) > head ? int(g.cursor?.cursor) : head)
      : 0n;
  let pendingTotal = $derived(groups.reduce((n, g) => n + int(g.pendingCount), 0n));
  let consumerTotal = $derived(groups.reduce((n, g) => n + (g.consumers?.length ?? 0), 0));
  let filtered = $derived(
    rows.filter((r) =>
      `${r.name} ${r.node} ${(r.topics ?? []).join(' ')}`
        .toLowerCase()
        .includes(search.toLowerCase()),
    ),
  );
  const url = (name: string, node: string) =>
    `/dashboard/queues?${new URLSearchParams({ queue: name, node })}`;
  async function api(
    action: string,
    node: string,
    args: Record<string, string> = {},
    signal = controller?.signal,
  ) {
    const response = await fetch(
      `/console-api/queues?${new URLSearchParams({ action, node, ...args })}`,
      { signal },
    );
    if (!response.ok) throw new Error((await response.text()).trim() || 'Queue data unavailable');
    return response.json();
  }
  async function load(more = false) {
    if (loading) return;
    loading = true;
    error = '';
    const signal = controller.signal;
    try {
      if (identity) {
        const target = identity;
        const data = await Promise.all([
          api('config', target.node, { queue: target.name }),
          api('info', target.node, { queue: target.name }),
          api('groups', target.node, { queue: target.name }),
        ]);
        if (signal.aborted) return;
        [queue, info] = data;
        groups = data[2].groups ?? [];
      } else {
        const targets = more ? Object.keys(next) : active;
        const batch = await Promise.all(
          targets.map(async (node) => {
            try {
              return { node, data: await api('list', node, more ? { cursor: next[node] } : {}) };
            } catch (e) {
              return { node, error: e as Error };
            }
          }),
        );
        if (signal.aborted) return;
        const newRows = more ? [...rows] : [],
          newNext: Record<string, string> = {},
          failed: string[] = [];
        for (const item of batch) {
          if (item.error) {
            failed.push(item.node);
            newNext[item.node] = more ? next[item.node] : '';
            continue;
          }
          const data = item.data;
          newRows.push(...(data.queues ?? []).map((q: Queue) => ({ ...q, node: item.node })));
          if (data.nextPageToken) {
            if (more && data.nextPageToken === next[item.node]) {
              failed.push(item.node);
            } else newNext[item.node] = data.nextPageToken;
          }
        }
        rows = [...new Map(newRows.map((q) => [JSON.stringify([q.node, q.name]), q])).values()];
        next = newNext;
        failures = failed;
      }
    } catch (e) {
      if (!signal.aborted) error = (e as Error).message;
    } finally {
      if (!signal.aborted) loading = false;
    }
  }
  $effect(() => {
    routeKey;
    untrack(() => {
      controller?.abort();
      messageController?.abort();
      pendingController?.abort();
      controller = new AbortController();
      loading = false;
      rows = [];
      next = {};
      failures = [];
      queue = null;
      info = null;
      groups = [];
      error = '';
      messages = [];
      inspected = null;
      messageError = '';
      messageLoading = false;
      hasRead = false;
      offset = '';
      pendingGroup = '';
      pending = [];
      pendingLoading = false;
      pendingError = '';
      void load();
    });
    return () => {
      controller?.abort();
      messageController?.abort();
      pendingController?.abort();
    };
  });
  onMount(() => {
    const timer = setInterval(() => {
      if (identity && !paused && !loading) void load();
    }, 10000);
    return () => clearInterval(timer);
  });
  async function read(start: string) {
    if (!identity) return;
    if (!/^\d+$/.test(start)) {
      messageError = 'Enter a non-negative integer offset.';
      return;
    }
    messageController?.abort();
    const request = new AbortController();
    messageController = request;
    messageLoading = true;
    messageError = '';
    inspected = null;
    try {
      const data = await api(
        'messages',
        identity.node,
        { queue: identity.name, offset: start },
        request.signal,
      );
      if (!request.signal.aborted) {
        messages = data.messages ?? [];
        offset = start;
        hasRead = true;
      }
    } catch (e) {
      if (!request.signal.aborted) messageError = (e as Error).message;
    } finally {
      if (!request.signal.aborted) messageLoading = false;
    }
  }
  async function showPending(g: string) {
    if (!identity) return;
    pendingController?.abort();
    const request = new AbortController();
    pendingController = request;
    pendingGroup = g;
    pendingLoading = true;
    pendingError = '';
    pending = [];
    try {
      const data = await api(
        'pending',
        identity.node,
        { queue: identity.name, group: g },
        request.signal,
      );
      if (!request.signal.aborted) pending = data.entries ?? [];
    } catch (e) {
      if (!request.signal.aborted) pendingError = (e as Error).message;
    } finally {
      if (!request.signal.aborted) pendingLoading = false;
    }
  }
  async function inspect(message: Message) {
    inspected = message;
    await tick();
    document
      .querySelector('.payload-detail')
      ?.scrollIntoView({ block: 'nearest', behavior: 'smooth' });
  }
  function decode(value?: string) {
    let size = 0;
    try {
      const raw = atob(value ?? '');
      size = raw.length;
      const data = Uint8Array.from(raw.slice(0, 16384), (c) => c.charCodeAt(0));
      const text = new TextDecoder('utf-8', { fatal: true }).decode(data, {
        stream: raw.length > 16384,
      });
      return {
        text: raw.length > 16384 ? text + '\n… preview truncated at 16 KiB' : text,
        size: raw.length,
        binary: false,
      };
    } catch {
      return {
        text:
          (value ?? '').slice(0, 21848) +
          ((value?.length ?? 0) > 21848 ? '\n… Base64 preview truncated' : ''),
        size,
        binary: true,
      };
    }
  }
  const preview = (value?: string) => {
    const d = decode(value);
    return d.binary ? 'Binary payload' : d.text.slice(0, 120);
  };
  function retention(v?: string) {
    if (!v || v === '0s') return 'Unlimited age';
    const seconds = Number(v.replace(/s$/, ''));
    return Number.isFinite(seconds)
      ? seconds >= 86400
        ? `${seconds / 86400} ${seconds === 86400 ? 'day' : 'days'}`
        : seconds >= 3600
          ? `${seconds / 3600} hours`
          : `${seconds} seconds`
      : v;
  }
</script>

{#if identity}
  <div class="queue-toolbar">
    <a class="text-button" href="/dashboard/queues" onclick={(e) => followLink(e, onback)}>
      ← All queues
    </a><button class="button" disabled={loading} onclick={() => load()}>
      <Icon name="refresh" size={15} />Refresh
    </button>
  </div>
{:else}
  <div class="queue-toolbar">
    <label class="search-field">
      <Icon name="search" size={16} /><input
        aria-label="Find queues"
        placeholder="Find queues or topic routes…"
        bind:value={search}
      />
    </label><button class="button" disabled={loading} onclick={() => load()}>
      <Icon name="refresh" size={15} />Refresh
    </button>
  </div>
{/if}
{#if loading && !info && !rows.length}<div class="panel empty-state" role="status">
    Loading queues…
  </div>{/if}
{#if error}<div class="notice" role="alert">
    {error}<button class="text-button" onclick={() => load()}>Retry</button>
  </div>{/if}
{#if failures.length}<div class="notice" role="alert">
    Queue data unavailable on {failures.join(', ')}. Check that the queue service is enabled.<button
      class="text-button"
      onclick={() => load(true)}
    >
      Retry
    </button>
  </div>{/if}
{#if !identity}
  <section class="panel queue-inventory">
    <div class="panel-heading">
      <h2>Durable queues</h2>
      <span class="muted">
        {rows.length}{Object.keys(next).length ? '+' : ''} node / queue entries
      </span>
    </div>
    <div class="table-scroll">
      <table>
        <thead>
          <tr>
            <th>Queue</th><th>Node</th><th>Topic routes</th><th>Retention</th><th>
              <span class="sr-only">Inspect queue</span>
            </th>
          </tr>
        </thead><tbody>
          {#each filtered as q}<tr>
              <td>
                <a
                  class="client-link"
                  href={url(q.name, q.node)}
                  onclick={(e) => followLink(e, () => onopen(q.name, q.node))}
                >
                  <span class="client-icon"><Icon name="queues" size={16} /></span>{q.name}
                </a>
              </td><td class="mono muted">{q.node}</td><td>
                {#each q.topics ?? [] as topic}<code class="topic-route mono muted">
                    {topic}
                  </code>{:else}<span class="muted"> Direct publishing </span>{/each}
              </td><td>{retention(q.config?.retention?.maxAge)}</td><td>
                <a
                  class="icon-button"
                  aria-label={`Inspect queue ${q.name} on ${q.node}`}
                  href={url(q.name, q.node)}
                  onclick={(e) => followLink(e, () => onopen(q.name, q.node))}
                >
                  <Icon name="chevron" size={16} />
                </a>
              </td>
            </tr>
          {:else}<tr>
              <td colspan="5">
                <div class="empty-state">
                  {loading
                    ? 'Loading…'
                    : search
                      ? 'No matching queues'
                      : failures.length
                        ? 'Queue inventory unavailable'
                        : 'No durable queues on these nodes'}
                </div>
              </td>
            </tr>{/each}
        </tbody>
      </table>
    </div>
    {#if Object.keys(next).length}<div class="queue-toolbar inset">
        <span class="muted">Search covers loaded queues.</span><button
          class="button"
          disabled={loading}
          onclick={() => load(true)}
        >
          Load more
        </button>
      </div>{/if}
  </section>
  <p class="queue-caption">
    Durable queues are shared across protocols. MQTT offline session buffers are shown under
    Sessions.
  </p>
{:else if info && queue}
  <section class="summary-strip queue-metrics" aria-label="Queue metrics">
    <div>
      <span class="summary-label">Stored messages<Icon name="archive" size={18} /></span><strong>
        {count(info.messageCount)}
      </strong>
    </div>
    <div>
      <span class="summary-label">
        Pending acknowledgements<Icon name="clock" size={18} />
      </span><strong class:pending-value={pendingTotal > 0n}>
        {count(pendingTotal)}
      </strong>
    </div>
    <div>
      <span class="summary-label">
        Consumer groups<Icon name="subscriptions" size={18} />
      </span><strong>
        {groups.length}
      </strong>
    </div>
    <div>
      <span class="summary-label">
        Registered consumers<Icon name="sessions" size={18} />
      </span><strong>
        {consumerTotal}
      </strong>
    </div>
  </section>
  <section class="panel delivery-panel" aria-labelledby="delivery-progress">
    <div class="panel-heading">
      <div>
        <h2 id="delivery-progress">Delivery progress</h2>
        <p class="muted">Consumer positions in the retained log</p>
      </div>
      <span class="queue-log-range mono">{count(head)} <span>→</span> {count(tail)}</span>
    </div>
    <div class="progress-legend">
      <span><i class="committed"></i>Committed range</span><span>
        <i class="delivered"></i>Uncommitted range
      </span><span><i class="waiting"></i>Not delivered</span>
    </div>
    <div class="group-lanes">
      {#each groups as g}
        <div class="group-lane">
          <div class="group-label">
            <strong>{g.groupId}</strong><span class="muted">
              {g.consumers?.length ?? 0}
              {g.consumers?.length === 1 ? 'consumer' : 'consumers'}
            </span>
          </div>
          <div class="group-progress">
            <div
              class="progress-track"
              role="img"
              aria-label={`${g.groupId}: committed position ${g.cursor?.committed ?? '0'}, next delivery ${g.cursor?.cursor ?? '0'}, ${count(waiting(g))} retained offsets not delivered, ${count(g.pendingCount)} pending acknowledgements`}
            >
              <span class="committed" style:width={`${percent(int(g.cursor?.committed))}%`}
              ></span><span
                class="delivered"
                style:width={`${Math.max(0, percent(int(g.cursor?.cursor)) - percent(int(g.cursor?.committed)))}%`}
              ></span>
              <span class="cursor-marker" style:left={`${percent(int(g.cursor?.cursor))}%`}></span>
            </div>
            <div class="group-positions">
              <span>Committed <b>{count(g.cursor?.committed)}</b></span><span>
                Next delivery <b>{count(g.cursor?.cursor)}</b>
              </span>
            </div>
          </div>
          <div class="group-counts">
            <span><strong>{count(waiting(g))}</strong> not delivered</span><button
              class="pending-link"
              onclick={() => showPending(g.groupId)}
            >
              <strong>{count(g.pendingCount)}</strong> pending <Icon name="chevron" size={12} />
            </button>
          </div>
        </div>
      {:else}<div class="empty-state">
          No consumer groups. Stored messages have no registered group to deliver to.
        </div>{/each}
    </div>
    <details class="delivery-help">
      <summary>About delivery positions <Icon name="down" size={13} /></summary>
      <p class="queue-caption inset">
        Bars represent offset ranges; an uncommitted range can include acknowledgements received out
        of order. Pending counts are per group. Acknowledgement does not remove a message from the
        retained log.
      </p>
    </details>
  </section>
  {#if pendingGroup}<section class="panel pending-panel" aria-label="Pending deliveries">
      <div class="panel-heading">
        <h2>Pending · {pendingGroup}</h2>
        <button
          class="icon-button"
          aria-label="Close pending deliveries"
          onclick={() => {
            pendingController?.abort();
            pendingGroup = '';
          }}
        >
          <Icon name="close" size={17} />
        </button>
      </div>
      {#if pendingLoading}<p class="inset" role="status">
          Loading pending deliveries…
        </p>{:else if pendingError}<div class="notice" role="alert">
          {pendingError}<button class="text-button" onclick={() => showPending(pendingGroup)}>
            Retry
          </button>
        </div>{:else}
        <div class="table-scroll pending-scroll">
          <table>
            <thead>
              <tr>
                <th>Offset</th><th>Consumer</th><th>Delivered at</th><th class="numeric">
                  Delivery attempts
                </th>
              </tr>
            </thead><tbody>
              {#each pending as p}<tr>
                  <td>
                    <button class="text-button mono" onclick={() => read(p.offset ?? '0')}>
                      {count(p.offset)}<Icon name="chevron" size={12} />
                    </button>
                  </td><td class="mono">{p.consumerId}</td><td>{date(p.deliveredAt)}</td><td
                    class="numeric"
                    class:pending-value={(p.deliveryCount ?? 0) > 1}
                  >
                    {p.deliveryCount ?? 0}{#if (p.deliveryCount ?? 0) > 1}<span class="retry-label">
                        redelivered
                      </span>{/if}
                  </td>
                </tr>{:else}<tr>
                  <td colspan="4"><div class="empty-state">No pending deliveries</div></td>
                </tr>{/each}
            </tbody>
          </table>
        </div>{/if}
    </section>{/if}
  <section class="panel message-panel" aria-labelledby="message-browser">
    <div class="panel-heading">
      <div>
        <h2 id="message-browser">Message browser</h2>
        <p class="muted">Inspect payloads and headers</p>
      </div>
      <span class="read-only" title="Reading does not consume or acknowledge messages">
        <Icon name="book" size={14} />Read only
      </span>
    </div>
    <form
      class="message-controls"
      onsubmit={(e) => {
        e.preventDefault();
        read(offset || head.toString());
      }}
    >
      <label>
        Offset <input
          aria-label="Message offset"
          inputmode="numeric"
          placeholder={head.toString()}
          bind:value={offset}
        />
      </label><button class="button" disabled={messageLoading} type="submit">
        Read messages
      </button><button
        class="button"
        disabled={messageLoading}
        type="button"
        onclick={() => read(head.toString())}
      >
        Oldest
      </button><button
        class="button"
        disabled={messageLoading}
        type="button"
        onclick={() => read((tail - head > 20n ? tail - 20n : head).toString())}
      >
        Latest
      </button>
    </form>
    {#if messageLoading}<p class="inset" role="status">
        Reading messages…
      </p>{:else if messageError}<div class="notice" role="alert">{messageError}</div>{/if}
    {#if hasRead}<div class="table-scroll message-scroll">
        <table>
          <thead>
            <tr>
              <th>Offset</th><th>Appended</th><th>Payload preview</th><th class="numeric">Size</th>
            </tr>
          </thead><tbody>
            {#each messages as m}<tr>
                <td>
                  <button class="text-button mono" onclick={() => inspect(m)}>
                    {count(m.offset)}<Icon name="chevron" size={12} />
                  </button>
                </td><td class="muted">{date(m.timestamp)}</td><td>
                  <button class="payload-preview mono" onclick={() => inspect(m)}>
                    {preview(m.value)}
                  </button>
                </td><td class="numeric">{bytes(decode(m.value).size)}</td>
              </tr>{:else}<tr>
                <td colspan="4">
                  <div class="empty-state">
                    No messages at this offset. Retention may have advanced the log.
                  </div>
                </td>
              </tr>{/each}
          </tbody>
        </table>
      </div>
      {#if messages.length && int(messages.at(-1)?.offset) + 1n < tail}<div
          class="queue-toolbar inset"
        >
          <span class="muted">20 messages per read</span><button
            class="button"
            disabled={messageLoading}
            onclick={() => read((int(messages.at(-1)?.offset) + 1n).toString())}
          >
            Next messages <Icon name="chevron" size={14} />
          </button>
        </div>{/if}
    {:else if !messageLoading}<p class="message-hint">
        Select Latest or enter a message offset.
      </p>{/if}
    {#if inspected}{@const payload = decode(inspected.value)}
      <div class="payload-detail">
        <div class="panel-heading">
          <h3>Message {count(inspected.offset)}</h3>
          <button class="icon-button" aria-label="Close message" onclick={() => (inspected = null)}>
            <Icon name="close" size={16} />
          </button>
        </div>
        <span class="muted">
          {payload.binary ? 'Binary · Base64' : 'UTF-8'} · {bytes(payload.size)}
        </span>
        <pre>{payload.text || '(empty payload)'}</pre>
        {#if inspected.key}<h4>Key</h4>
          <pre>{decode(inspected.key).text}</pre>{/if}
        <h4>Headers / properties</h4>
        <dl class="message-headers">
          {#each Object.entries(inspected.headers ?? {}) as [key, value]}<div>
              <dt>{key}</dt>
              <dd>{decode(value).text}</dd>
            </div>{:else}<span class="muted">No headers reported</span>{/each}
        </dl>
      </div>{/if}
  </section>
  <div class="queue-details-grid">
    <section class="panel queue-config">
      <div class="panel-heading"><h2>Routing & retention</h2></div>
      <dl class="inspector-fields">
        <div>
          <dt>Topic routes</dt>
          <dd>{(queue.topics ?? []).join(', ') || 'Direct publishing'}</dd>
        </div>
        <div>
          <dt>Maximum age</dt>
          <dd>{retention(queue.config?.retention?.maxAge)}</dd>
        </div>
        <div>
          <dt>Retention size</dt>
          <dd>
            {int(queue.config?.retention?.maxBytes) > 0n
              ? bytes(Number(queue.config?.retention?.maxBytes))
              : 'Unlimited'}
          </dd>
        </div>
        <div>
          <dt>Minimum retained messages</dt>
          <dd>{count(queue.config?.retention?.minMessages)}</dd>
        </div>
        <div>
          <dt>Maximum message size</dt>
          <dd>
            {queue.config?.maxMessageSize
              ? bytes(queue.config.maxMessageSize)
              : 'No queue-specific limit'}
          </dd>
        </div>
        <div>
          <dt>Replication group</dt>
          <dd>{queue.config?.replication?.group || 'Local queue'}</dd>
        </div>
      </dl>
    </section>
    <section class="panel queue-config">
      <div class="panel-heading"><h2>Consumers</h2></div>
      <div class="table-scroll consumer-list">
        <table>
          <thead><tr><th>Consumer</th><th>Group</th><th>Last heartbeat</th></tr></thead><tbody>
            {#each groups as g}{#each g.consumers ?? [] as c}<tr>
                  <td>
                    <span class="consumer-identity">
                      <Icon name="sessions" size={16} />{c.consumerId}
                    </span>
                  </td><td class="muted">{g.groupId}</td><td class="muted">
                    {date(c.lastHeartbeat)}
                  </td>
                </tr>{/each}{/each}
            {#if !consumerTotal}<tr>
                <td colspan="3"><div class="empty-state">No registered consumers</div></td>
              </tr>{/if}
          </tbody>
        </table>
      </div>
    </section>
  </div>
{/if}

<style>
  .queue-toolbar {
    display: flex;
    justify-content: space-between;
    gap: 16px;
    align-items: center;
    margin-bottom: 20px;
  }
  .inset {
    padding: 16px 24px;
  }
  .queue-toolbar.inset {
    margin: 0;
  }
  .queue-caption {
    font-size: 12px;
    line-height: 1.6;
    color: var(--muted);
    margin: 12px 0 0;
  }
  .topic-route {
    display: block;
    line-height: 1.5;
  }
  .pending-value {
    color: var(--violet);
  }
  .delivery-panel,
  .message-panel,
  .pending-panel {
    margin-bottom: 24px;
  }
  .panel-heading p {
    margin: 6px 0 0;
    font-size: 13px;
  }
  .queue-log-range {
    font-size: 14px;
    white-space: nowrap;
  }
  .queue-log-range span {
    color: var(--muted);
    padding: 0 12px;
  }
  .progress-legend {
    display: flex;
    gap: 22px;
    padding: 0 24px 24px;
    font-size: 12px;
    color: var(--muted);
  }
  .progress-legend span {
    display: flex;
    align-items: center;
    gap: 7px;
  }
  .progress-legend i {
    height: 7px;
    width: 16px;
    border-radius: 2px;
  }
  .committed {
    background: var(--mint);
  }
  .delivered {
    background: var(--accent);
  }
  .waiting {
    background: var(--border);
  }
  .group-lanes {
    padding: 0 24px;
  }
  .group-lane {
    display: grid;
    grid-template-columns: 180px minmax(120px, 1fr) 150px;
    gap: 24px;
    align-items: center;
    padding: 16px 0;
    border-top: 1px solid var(--subtle-border);
  }
  .group-label {
    display: flex;
    flex-direction: column;
    gap: 7px;
    font-size: 13px;
    overflow-wrap: anywhere;
  }
  .group-label strong {
    font-weight: 500;
  }
  .group-label span {
    font-size: 12px;
  }
  .progress-track {
    height: 16px;
    border-radius: 4px;
    background: var(--panel-raised);
    position: relative;
    display: flex;
    border: 1px solid var(--border);
  }
  .progress-track > span:not(.cursor-marker) {
    height: 100%;
    transition: width 0.3s;
  }
  .progress-track > span:first-child {
    border-radius: 3px 0 0 3px;
  }
  .cursor-marker {
    width: 2px;
    background: var(--text);
    position: absolute;
    top: -5px;
    bottom: -5px;
    transform: translateX(-1px);
  }
  .group-positions {
    display: flex;
    justify-content: space-between;
    gap: 12px;
    font-size: 11px;
    color: var(--muted);
    margin-top: 14px;
  }
  .group-positions b {
    font-family: 'IBM Plex Mono', monospace;
    font-weight: 400;
    color: var(--text);
    margin-left: 5px;
  }
  .group-counts {
    font-size: 12px;
    display: flex;
    align-items: flex-end;
    flex-direction: column;
    gap: 10px;
    color: var(--muted);
  }
  .group-counts strong {
    font-family: 'IBM Plex Mono', monospace;
    font-size: 17px;
    font-weight: 500;
    color: var(--text);
    margin-right: 4px;
  }
  .pending-link {
    display: flex;
    align-items: center;
    gap: 4px;
    color: var(--violet);
  }
  .pending-link strong {
    color: inherit;
  }
  .pending-link:hover {
    text-decoration: underline;
  }
  .read-only {
    font-size: 12px;
    color: var(--muted);
    display: flex;
    gap: 6px;
    align-items: center;
  }
  .message-controls {
    display: flex;
    align-items: center;
    gap: 16px;
    padding: 0 22px 16px;
    flex-wrap: wrap;
  }
  .message-controls label {
    font-size: 12px;
    color: var(--muted);
    display: flex;
    align-items: center;
    gap: 12px;
  }
  .message-controls input {
    border: 1px solid var(--border);
    background: var(--panel-raised);
    border-radius: 6px;
    padding: 9px 12px;
    max-width: 180px;
    color: var(--text);
    font-family: 'IBM Plex Mono', monospace;
  }
  .payload-preview {
    max-width: 400px;
    display: block;
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
    font-size: 12px;
    color: var(--muted);
    text-align: left;
  }
  .payload-preview:hover {
    color: var(--accent);
  }
  .payload-detail {
    padding: 24px;
    background: var(--panel-raised);
    border-top: 1px solid var(--border);
  }
  .payload-detail .panel-heading {
    padding: 0 0 12px;
  }
  .payload-detail pre {
    white-space: pre-wrap;
    overflow-wrap: anywhere;
    font-family: 'IBM Plex Mono', monospace;
    font-size: 13px;
    max-height: 400px;
    overflow: auto;
    line-height: 1.7;
    padding: 16px;
    border: 1px solid var(--border);
    border-radius: 6px;
    background: var(--panel);
  }
  .payload-detail h4 {
    font-size: 13px;
  }
  .message-headers > div {
    display: grid;
    grid-template-columns: 160px 1fr;
    gap: 16px;
    font-size: 12px;
    margin: 12px 0;
    overflow-wrap: anywhere;
  }
  .message-headers dd {
    margin: 0;
    font-family: 'IBM Plex Mono', monospace;
  }
  .queue-details-grid {
    display: grid;
    grid-template-columns: 1fr 1fr;
    gap: 24px;
  }
  .queue-config {
    min-width: 0;
  }
  .queue-config .inspector-fields {
    margin: 0;
    padding: 0 22px 16px;
  }
  .message-hint {
    font-size: 12px;
    color: var(--muted);
    padding: 12px 22px 16px;
    margin: 0;
    border-top: 1px solid var(--subtle-border);
  }
  .delivery-help {
    border-top: 1px solid var(--subtle-border);
  }
  .delivery-help summary {
    display: flex;
    justify-content: space-between;
    align-items: center;
    padding: 14px 24px;
    font-size: 12px;
    color: var(--muted);
    cursor: pointer;
  }
  .delivery-help summary::-webkit-details-marker {
    display: none;
  }
  .delivery-help .queue-caption {
    margin: 0;
    padding-top: 0;
  }
  .queue-config dd {
    overflow-wrap: anywhere;
  }
  .consumer-list {
    max-height: 400px;
    overflow: auto;
  }
  .consumer-identity {
    display: flex;
    align-items: center;
    gap: 10px;
    font-size: 13px;
    font-weight: 400;
  }
  .consumer-identity :global(svg) {
    color: var(--faint);
    flex-shrink: 0;
  }
  .message-scroll {
    max-height: 420px;
    overflow: auto;
  }
  .pending-scroll {
    max-height: 300px;
    overflow: auto;
  }
  .retry-label {
    margin-left: 8px;
    font-size: 11px;
  }
  .search-field {
    display: flex;
    gap: 10px;
    align-items: center;
    border: 1px solid var(--border);
    border-radius: 7px;
    padding: 10px 12px;
    min-width: 280px;
    background: var(--panel);
  }
  .search-field input {
    border: 0;
    background: none;
    color: var(--text);
    outline: none;
    width: 100%;
  }
  @media (max-width: 1050px) {
    .group-lane {
      grid-template-columns: 140px 1fr;
    }
    .group-counts {
      grid-column: 2;
      flex-direction: row;
      justify-content: space-between;
    }
    .queue-details-grid {
      grid-template-columns: 1fr;
    }
  }
  @media (max-width: 600px) {
    .group-lane {
      grid-template-columns: 1fr;
      gap: 18px;
    }
    .group-counts {
      grid-column: auto;
    }
    .progress-legend {
      flex-wrap: wrap;
      gap: 12px;
    }
    .queue-toolbar {
      flex-wrap: wrap;
    }
    .queue-log-range {
      display: none;
    }
    .queue-metrics {
      grid-template-columns: 1fr 1fr;
    }
    .message-controls {
      gap: 12px;
    }
  }
</style>
