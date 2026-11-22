<script lang="ts">
  import { onMount, tick } from 'svelte';
  import Icon from './Icon.svelte';
  import type { BrokerNode } from './types';
  let {
    nodes,
    value,
    onchange,
  }: { nodes: BrokerNode[]; value: string; onchange: (id: string) => void } = $props();
  let open = $state(false),
    query = $state('');
  let root: HTMLDivElement, trigger: HTMLButtonElement;
  let panel = $state<HTMLDivElement>();
  let input = $state<HTMLInputElement>();
  let matches = $derived(nodes.filter((n) => n.id.toLowerCase().includes(query.toLowerCase())));
  async function show() {
    open = !open;
    query = '';
    if (open) {
      await tick();
      input?.focus();
    }
  }
  function choose(id: string) {
    onchange(id);
    open = false;
    trigger.focus();
  }
  function key(e: KeyboardEvent) {
    if (e.key === 'Escape') {
      e.preventDefault();
      e.stopPropagation();
      open = false;
      trigger.focus();
    }
    if (['ArrowDown', 'ArrowUp', 'Home', 'End'].includes(e.key)) {
      e.preventDefault();
      const options = [...panel!.querySelectorAll<HTMLButtonElement>('[role=option]')];
      const i = options.indexOf(document.activeElement as HTMLButtonElement);
      const next =
        e.key === 'Home'
          ? 0
          : e.key === 'End'
            ? options.length - 1
            : e.key === 'ArrowDown'
              ? (i + 1) % options.length
              : i <= 0
                ? options.length - 1
                : i - 1;
      options[next]?.focus();
    }
  }
  onMount(() => {
    const outside = (event: PointerEvent) => {
      if (!root.contains(event.target as Node)) open = false;
    };
    document.addEventListener('pointerdown', outside);
    return () => document.removeEventListener('pointerdown', outside);
  });
</script>

<div
  class="scope-picker"
  bind:this={root}
  onfocusout={(e) => {
    if (e.relatedTarget && !root.contains(e.relatedTarget as Node)) open = false;
  }}
>
  <button
    class="scope-trigger"
    bind:this={trigger}
    onclick={show}
    aria-label="Select node scope"
    aria-haspopup="listbox"
    aria-expanded={open}
    aria-controls="node-scope-options"
  >
    <Icon name="nodes" size={18} /><span>{value === 'all' ? 'All nodes' : value}</span><Icon
      name="down"
      size={14}
    />
  </button>
  {#if open}<div
      class="scope-popover"
      bind:this={panel}
      role="listbox"
      tabindex="-1"
      aria-label="Node scope"
      id="node-scope-options"
      onkeydown={key}
    >
      <label class="scope-search">
        <Icon name="search" size={16} /><input
          bind:this={input}
          bind:value={query}
          aria-label="Find a node"
          placeholder="Find a node…"
        />
      </label>
      <div class="scope-options">
        <button
          role="option"
          aria-label="All nodes"
          aria-selected={value === 'all'}
          onclick={() => choose('all')}
        >
          <Icon name="nodes" size={18} /><span>
            All nodes<small>{nodes.length} configured</small>
          </span>{#if value === 'all'}<Icon name="check" size={16} />{/if}
        </button>
        {#each matches as n}<button
            role="option"
            aria-label={n.id}
            aria-selected={value === n.id}
            onclick={() => choose(n.id)}
          >
            <i class:unavailable={!n.healthy}></i><span>
              {n.id}<small>{n.leader && n.cluster_mode !== false ? 'Leader' : n.address}</small>
            </span>{#if value === n.id}<Icon name="check" size={16} />{/if}
          </button>{/each}
        {#if !matches.length}<p>No matching nodes</p>{/if}
      </div>
    </div>{/if}
</div>

<style>
  .scope-picker {
    position: relative;
    min-width: 0;
  }
  .scope-trigger {
    display: flex;
    align-items: center;
    gap: 10px;
    height: 36px;
    padding: 0 10px;
    background: var(--panel-raised);
    border: 1px solid transparent;
    border-radius: 7px;
    font-size: 13px;
    font-weight: 500;
  }
  .scope-trigger:hover,
  .scope-trigger[aria-expanded='true'] {
    border-color: var(--border);
  }
  .scope-trigger span {
    max-width: 180px;
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
  }
  .scope-trigger :global(svg) {
    color: var(--muted);
    flex-shrink: 0;
  }
  .scope-popover {
    position: absolute;
    top: calc(100% + 8px);
    left: 0;
    width: 300px;
    max-width: calc(100vw - 64px);
    padding: 6px;
    border: 1px solid var(--border);
    border-radius: 10px;
    background: var(--panel);
    box-shadow: 0 12px 32px #0002;
    z-index: 30;
  }
  .scope-search {
    display: flex;
    align-items: center;
    gap: 8px;
    padding: 8px;
    border-bottom: 1px solid var(--subtle-border);
    color: var(--muted);
  }
  .scope-search input {
    width: 100%;
    min-width: 0;
    background: none;
    color: var(--text);
    border: 0;
    font: inherit;
    font-size: 13px;
  }
  .scope-search:focus-within {
    box-shadow: inset 0 -2px 0 var(--accent);
  }
  .scope-search input:focus-visible {
    outline: none;
  }
  .scope-options {
    max-height: 280px;
    overflow-y: auto;
    padding-top: 5px;
  }
  .scope-options button {
    display: flex;
    align-items: center;
    gap: 12px;
    width: 100%;
    text-align: left;
    padding: 10px;
    border-radius: 6px;
    font-size: 13px;
  }
  .scope-options button:hover,
  .scope-options button[aria-selected='true'] {
    background: var(--accent-bg);
  }
  .scope-options button > span {
    flex: 1;
    overflow-wrap: anywhere;
  }
  .scope-options small {
    display: block;
    margin-top: 3px;
    font-size: 11px;
    color: var(--muted);
  }
  .scope-options i {
    width: 6px;
    height: 6px;
    margin: 0 6px;
    border-radius: 50%;
    background: var(--mint);
  }
  .scope-options i.unavailable {
    background: var(--warning);
  }
  .scope-options p {
    padding: 12px;
    font-size: 12px;
    color: var(--muted);
  }
</style>
