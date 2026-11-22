<script lang="ts">
  import Icon from './Icon.svelte';
  let {
    page,
    size,
    total,
    onpage,
    onsize,
    more = false,
    busy = false,
    onmore,
    onlast,
  }: {
    page: number;
    size: number;
    total: number;
    onpage: (page: number) => void;
    onsize: (size: number) => void;
    more?: boolean;
    busy?: boolean;
    onmore?: () => void;
    onlast?: () => void;
  } = $props();
  let pages = $derived(Math.max(1, Math.ceil(total / size)));
</script>

<div class="pagination">
  <label class="page-size">
    Rows <select
      aria-label="Rows per page"
      value={size}
      onchange={(e) => onsize(Number(e.currentTarget.value))}
    >
      {#each [5, 10, 15, 25] as n}<option value={n}>{n}</option>{/each}
    </select>
  </label>
  <button
    class="icon-button first-page"
    aria-label="First page"
    title="First page"
    disabled={page <= 1}
    onclick={() => onpage(1)}
  >
    <Icon name="chevron" size={14} /><Icon name="chevron" size={14} />
  </button>
  <button class="button" disabled={page <= 1} onclick={() => onpage(page - 1)}>Previous</button>
  <span>{page} / {pages}{more ? '+' : ''}</span>
  <button
    class="button"
    disabled={busy || (page >= pages && !more)}
    onclick={() => (page >= pages && more ? onmore?.() : onpage(page + 1))}
  >
    Next
  </button>
  <button
    class="icon-button last-page"
    aria-label="Last page"
    title="Last page"
    disabled={busy || (page >= pages && !more)}
    onclick={() => (more ? onlast?.() : onpage(pages))}
  >
    <Icon name="chevron" size={14} /><Icon name="chevron" size={14} />
  </button>
</div>

<style>
  .pagination {
    flex-wrap: wrap;
    gap: 8px;
  }
  .page-size {
    display: flex;
    gap: 8px;
    align-items: center;
    font-size: 12px;
    color: var(--muted);
  }
  select {
    color: var(--text);
    background: var(--panel);
    border: 1px solid var(--border);
    border-radius: 4px;
    padding: 4px;
    font: inherit;
  }
  .first-page,
  .last-page {
    display: flex;
    align-items: center;
    gap: 0;
  }
  .first-page :global(svg) {
    transform: rotate(180deg);
  }
  .first-page :global(svg + svg),
  .last-page :global(svg + svg) {
    margin-left: -8px;
  }
</style>
