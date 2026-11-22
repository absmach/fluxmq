import type { Session, Subscription } from './types';
export type InventoryQuery = {
  kind: 'sessions' | 'subscriptions';
  node: string;
  q: string;
  state: string;
  protocol: string;
  sort: string;
};
type Saved = {
  sessions: Session[];
  subscriptions: Subscription[];
  next: Record<string, string>;
  failed: string[];
  ready: boolean;
  updated: number;
};

export class Inventory {
  sessions = $state<Session[]>([]);
  subscriptions = $state<Subscription[]>([]);
  next = $state<Record<string, string>>({});
  failed = $state<string[]>([]);
  loading = $state(false);
  ready = $state(false);
  error = $state('');
  key = $state('');
  updated = 0;
  query: InventoryQuery | null = null;
  private controller?: AbortController;
  private timer?: ReturnType<typeof setTimeout>;
  private cache = new Map<string, Saved>();
  get hasMore() {
    return Object.keys(this.next).length > 0;
  }
  private save() {
    if (!this.ready || !this.key) return;
    this.cache.set(
      this.key,
      $state.snapshot({
        sessions: this.sessions,
        subscriptions: this.subscriptions,
        next: this.next,
        failed: this.failed,
        ready: this.ready,
        updated: this.updated,
      }),
    );
    if (this.cache.size > 6) this.cache.delete(this.cache.keys().next().value!);
  }
  stop() {
    this.save();
    this.controller?.abort();
    clearTimeout(this.timer);
    this.loading = false;
  }
  select(query: InventoryQuery) {
    this.stop();
    this.query = query;
    this.key = JSON.stringify(query);
    this.error = '';
    const saved = this.cache.get(this.key);
    this.sessions = saved?.sessions ?? [];
    this.subscriptions = saved?.subscriptions ?? [];
    this.next = saved?.next ?? {};
    this.failed = saved?.failed ?? [];
    this.ready = saved?.ready ?? false;
    this.updated = saved?.updated ?? 0;
    if (!saved) {
      this.loading = true;
      this.timer = setTimeout(
        () => {
          this.loading = false;
          void this.load(true);
        },
        query.q ? 250 : 0,
      );
    }
  }
  async load(reset = false, all = false, minimum = 0) {
    if (!this.query || this.loading) return;
    const query = this.query;
    const controller = new AbortController();
    this.controller?.abort();
    this.controller = controller;
    this.loading = true;
    this.error = '';
    let sessions = reset ? [] : [...this.sessions];
    let subscriptions = reset ? [] : [...this.subscriptions];
    let cursor: Record<string, string> | undefined = reset ? undefined : this.next;
    if (cursor && !Object.keys(cursor).length) {
      this.loading = false;
      return;
    }
    const seen = new Set<string>();
    try {
      do {
        const cursorKey = JSON.stringify(cursor ?? {});
        if (seen.has(cursorKey))
          throw new Error('The broker repeated a page token. Refresh to retry.');
        seen.add(cursorKey);
        const params = new URLSearchParams({
          kind: query.kind,
          q: query.q,
          state: query.state,
          protocol: query.protocol,
        });
        if (query.node !== 'all') params.set('node', query.node);
        if (cursor) params.set('cursor', JSON.stringify(cursor));
        const response = await fetch(`/console-api/inventory?${params}`, {
          signal: controller.signal,
        });
        if (!response.ok) throw new Error('Could not load inventory. Retry to continue.');
        const data = await response.json();
        if (controller.signal.aborted) return;
        const sessionMap = new Map(
          sessions.map((s) => [JSON.stringify([s.node_id, s.client_id]), s]),
        );
        for (const s of data.sessions as Session[])
          sessionMap.set(JSON.stringify([s.node_id, s.client_id]), s);
        sessions = [...sessionMap.values()];
        const subMap = new Map(
          subscriptions.map((s) => [JSON.stringify([s.node_id, s.filter]), s]),
        );
        for (const s of data.subscriptions as Subscription[])
          subMap.set(JSON.stringify([s.node_id, s.filter]), s);
        subscriptions = [...subMap.values()];
        this.sessions = sessions;
        this.subscriptions = subscriptions;
        this.next = data.next;
        this.failed = data.failed_nodes;
        this.ready = true;
        this.updated = Date.now();
        cursor = data.next;
        this.save();
        if (this.failed.length) break;
      } while (
        Object.keys(cursor ?? {}).length &&
        (all ||
          !!query.q ||
          query.protocol !== 'all' ||
          query.sort === 'queued' ||
          query.kind === 'subscriptions' ||
          sessions.length < minimum)
      );
    } catch (e) {
      if (!controller.signal.aborted) this.error = (e as Error).message;
    } finally {
      if (!controller.signal.aborted) {
        this.loading = false;
        this.save();
      }
    }
  }
}
