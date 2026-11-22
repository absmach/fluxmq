<script lang="ts">
  import { onMount, untrack } from 'svelte';
  import Pagination from './Pagination.svelte';
  import Icon from './Icon.svelte';
  import { clientURL, followLink } from './routes';
  import type { ClientListState, SubscriptionClient as Client } from './types';
  let {
    filter,
    node,
    initial,
    onstate,
    onclient,
  }: {
    filter: string;
    node: string;
    initial?: ClientListState;
    onstate: (state: ClientListState) => void;
    onclient: (client: { client_id: string; node_id: string }) => void;
  } = $props();
  const saved = untrack(() => initial);
  let clients = $state<Client[]>(saved?.clients ?? []),
    next = $state<Record<string, string>>(saved?.next ?? {}),
    failed = $state<string[]>(saved?.failed ?? []);
  let loading = $state(false),
    error = $state(''),
    search = $state(saved?.search ?? ''),
    page = $state(saved?.page ?? 1);
  let loaded = $state(saved?.loaded ?? false);
  let controller: AbortController;
  let pageSize = $state(saved?.pageSize ?? 10);
  $effect(() => onstate({ clients, next, failed, search, page, pageSize, loaded }));
  let filtered = $derived(
    clients.filter((c) =>
      `${c.client_id} ${c.node_id}`.toLowerCase().includes(search.toLowerCase()),
    ),
  );

  async function load(more = false) {
    controller?.abort();
    controller = new AbortController();
    const request = controller;
    loading = true;
    error = '';
    const query = new URLSearchParams({ filter });
    if (node !== 'all') query.set('node', node);
    if (more) query.set('cursor', JSON.stringify(next));
    try {
      const response = await fetch(`/console-api/subscription-clients?${query}`, {
        signal: request.signal,
      });
      if (!response.ok) throw new Error('Could not load subscription clients. Try again.');
      const data = await response.json();
      const unique = new Map<string, Client>();
      for (const c of [...(more ? clients : []), ...data.clients] as Client[])
        unique.set(JSON.stringify([c.node_id, c.client_id]), c);
      clients = [...unique.values()].sort(
        (a, b) => a.client_id.localeCompare(b.client_id) || a.node_id.localeCompare(b.node_id),
      );
      loaded = true;
      next = data.next;
      failed = data.failed_nodes;
      if (!more) page = 1;
    } catch (e) {
      if (!request.signal.aborted) error = (e as Error).message;
    } finally {
      if (!request.signal.aborted) loading = false;
    }
  }
  onMount(() => {
    if (!loaded) load();
    return () => controller?.abort();
  });
</script>

<section class="panel subscription-detail-page" aria-label="Subscription clients">
  <div class="panel-heading">
    <h2>Connected clients</h2>
    <span class="muted">{node === 'all' ? 'All nodes' : node}</span>
  </div>
  <div class="inspector-actions">
    <label class="search-field">
      <Icon name="search" size={16} /><input
        aria-label="Search subscription clients"
        placeholder="Search clients…"
        bind:value={search}
        oninput={() => (page = 1)}
      />
    </label><button
      class="icon-button"
      aria-label="Refresh subscription clients"
      disabled={loading}
      onclick={() => load()}
    >
      <Icon name="refresh" />
    </button>
  </div>
  {#if error}<div class="notice" role="alert">
      {error}<button class="text-button" onclick={() => load()}>Retry</button>
    </div>{/if}
  {#if failed.length}<div class="notice" role="alert">
      Incomplete results. Could not reach {failed.join(', ')}. Use “Load more / retry” below.
    </div>{/if}
  <div class="table-scroll">
    <table class="subscriber-table">
      <thead><tr><th>Client</th><th>Node</th><th>QoS</th></tr></thead><tbody>
        {#each filtered.slice((page - 1) * pageSize, page * pageSize) as client}<tr>
            <td>
              <a
                class="client-link"
                href={clientURL(client)}
                onclick={(event) => followLink(event, () => onclient(client))}
              >
                {client.client_id}<Icon name="chevron" size={14} />
              </a>
            </td><td>{client.node_id}</td><td>{client.qos}</td>
          </tr>{/each}
      </tbody>
    </table>
  </div>
  {#if loading}<p class="inspector-context" role="status">
      Loading clients…
    </p>{:else if !filtered.length && !error && !failed.length}<p class="inspector-context">
      {search ? 'No matching clients.' : 'No connected clients for this filter.'}
    </p>{/if}
  <div class="table-footer">
    <span>{filtered.length} loaded clients</span>
    <Pagination
      {page}
      size={pageSize}
      total={filtered.length}
      onpage={(n) => (page = n)}
      onsize={(n) => {
        pageSize = n;
        page = 1;
      }}
    />
  </div>
  {#if Object.keys(next).length}<button
      class="button"
      disabled={loading}
      onclick={() => load(true)}
    >
      Load more / retry
    </button>{/if}
</section>
