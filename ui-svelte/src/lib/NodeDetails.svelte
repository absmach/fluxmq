<script lang="ts">
  import { onMount } from 'svelte';
  import Icon from './Icon.svelte';
  import { number, bytes, uptime, nodeRole, type BrokerNode } from './types';
  let {
    node,
    onclose,
    onoverview,
    onclients,
    onsubscriptions,
  }: {
    node: BrokerNode;
    onclose: () => void;
    onoverview: () => void;
    onclients: () => void;
    onsubscriptions: () => void;
  } = $props();
  let dialog: HTMLDialogElement;
  onMount(() => dialog.showModal());
</script>

<dialog
  bind:this={dialog}
  class="detail-dialog node-detail-dialog"
  aria-labelledby="node-detail-title"
  {onclose}
>
  <div class="session-heading">
    <span class="eyebrow">NODE DETAILS</span><button
      class="icon-button"
      aria-label="Close node details"
      onclick={() => dialog.close()}
    >
      <Icon name="close" />
    </button>
  </div>
  <div class="session-title">
    <span class="large-client-icon"><Icon name="nodes" size={26} /></span>
    <h2 id="node-detail-title">{node.id}</h2>
    <span class="state-tag" class:offline={!node.healthy}>
      <i></i>{node.healthy ? 'Healthy' : 'Unavailable'}
    </span>
  </div>
  {#if !node.healthy}<p class="notice">
      The broker is unreachable. These are its last known values.
    </p>{/if}
  <div class="inspector-actions">
    <button class="button primary" onclick={onoverview}>
      <Icon name="overview" size={15} />View dashboard
    </button>
  </div>
  <dl class="inspector-fields">
    <div>
      <dt>Admin endpoint</dt>
      <dd>{node.address}</dd>
    </div>
    <div>
      <dt>Cluster address</dt>
      <dd>{node.transport_address || 'Not reported'}</dd>
    </div>
    <div>
      <dt>Role</dt>
      <dd>{nodeRole(node)}</dd>
    </div>
    <div>
      <dt>Version</dt>
      <dd>{node.version || 'Not reported'}</dd>
    </div>
    <div>
      <dt>Uptime</dt>
      <dd>{uptime(node.uptime)}</dd>
    </div>
  </dl>
  <h3 class="session-subtitle">Connections and traffic</h3>
  <dl class="inspector-fields">
    <div>
      <dt>Active connections</dt>
      <dd>{number(node.connections)}</dd>
    </div>
    <div>
      <dt>Subscriptions / consumers</dt>
      <dd>{number(node.subscriptions)}</dd>
    </div>
    <div>
      <dt>Incoming</dt>
      <dd>{number(node.incoming)} msg/s</dd>
    </div>
    <div>
      <dt>Outgoing</dt>
      <dd>{number(node.outgoing)} msg/s</dd>
    </div>
    <div>
      <dt>Messages received</dt>
      <dd>{number(node.received)}</dd>
    </div>
    <div>
      <dt>Messages sent</dt>
      <dd>{number(node.sent)}</dd>
    </div>
    <div>
      <dt>Bytes received</dt>
      <dd>{bytes(node.bytes_in)}</dd>
    </div>
    <div>
      <dt>Bytes sent</dt>
      <dd>{bytes(node.bytes_out)}</dd>
    </div>
  </dl>
  <dl class="inspector-fields">
    <div>
      <dt>Retained messages</dt>
      <dd>{node.retained_messages ?? 'Not reported'}</dd>
    </div>
  </dl>
  <h3 class="session-subtitle">Errors</h3>
  <dl class="inspector-fields">
    <div>
      <dt>Protocol</dt>
      <dd>{node.protocol_errors ?? 'Not reported'}</dd>
    </div>
    <div>
      <dt>Authentication</dt>
      <dd>{node.auth_errors ?? 'Not reported'}</dd>
    </div>
    <div>
      <dt>Authorization</dt>
      <dd>{node.authz_errors ?? 'Not reported'}</dd>
    </div>
    <div>
      <dt>Packet</dt>
      <dd>{node.packet_errors ?? 'Not reported'}</dd>
    </div>
  </dl>
  <div class="inspector-actions">
    <button class="button" onclick={onclients}>
      View clients <Icon name="chevron" size={15} />
    </button><button class="button" onclick={onsubscriptions}>
      View subscriptions <Icon name="chevron" size={15} />
    </button>
  </div>
</dialog>
