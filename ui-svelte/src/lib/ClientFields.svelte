<script lang="ts">
  import Icon from './Icon.svelte';
  import { subscriptionURL, followLink } from './routes';
  import { bytes, protocolLabel, sessionProtocol, type Session } from './types';
  let {
    client,
    onfilter,
    onnode,
  }: {
    client: Session;
    onfilter: (filter: string, node: string) => void;
    onnode: (node: string) => void;
  } = $props();
  let protocol = $derived(sessionProtocol(client));
  let mqtt = $derived(protocol.startsWith('mqtt'));
  let copyStatus = $state('');
  let hasConsumerGroups = $derived(client.subscriptions?.some((sub) => !!sub.consumer_group));
  async function copyID() {
    try {
      await navigator.clipboard.writeText(client.client_id);
      copyStatus = 'Copied';
    } catch {
      copyStatus = 'Copy unavailable — select the client ID to copy it.';
    }
  }
  const flag = (value: boolean | undefined) =>
    value === undefined ? 'Not reported' : value ? 'Yes' : 'No';
  const date = (value?: string) => (value ? new Date(value).toLocaleString() : 'Not reported');
</script>

<section class="panel client-info-panel">
  <h2>Connection</h2>
  <dl class="inspector-fields">
    <div>
      <dt>Client ID</dt>
      <dd class="client-id-value">
        <span>{client.client_id}</span><button
          class="icon-button"
          aria-label="Copy client ID"
          onclick={copyID}
        >
          <Icon name={copyStatus === 'Copied' ? 'check' : 'copy'} size={14} />
        </button>
      </dd>
    </div>
    {#if client.connection_name}<div>
        <dt>Connection name</dt>
        <dd>{client.connection_name}</dd>
      </div>{/if}
    <div>
      <dt>Protocol</dt>
      <dd>{protocolLabel(protocol)}</dd>
    </div>
    <div>
      <dt>Node</dt>
      <dd>
        <button class="text-button" onclick={() => onnode(client.node_id)}>{client.node_id}</button>
      </dd>
    </div>
    <div>
      <dt>Connected at</dt>
      <dd>{date(client.connected_at)}</dd>
    </div>
    {#if client.disconnected_at || !client.connected}<div>
        <dt>Disconnected at</dt>
        <dd>{date(client.disconnected_at)}</dd>
      </div>{/if}
  </dl>
  {#if copyStatus}<p class="inspector-context" role="status">{copyStatus}</p>{/if}
  {#if mqtt}
    <h3 class="session-subtitle">Delivery</h3>
    <dl class="inspector-fields">
      <div>
        <dt>Inflight messages</dt>
        <dd>{client.inflight_count}</dd>
      </div>
      <div>
        <dt>Offline queue</dt>
        <dd>{client.offline_queue_depth}</dd>
      </div>
    </dl>
    <p class="inspector-context">
      The session API reports buffer depth and inflight counts; it does not expose message payloads
      or acknowledgement history.
    </p>
  {:else}<p class="inspector-context">
      Inspect pending deliveries and acknowledgements in Queues.
    </p>{/if}
</section>
{#if mqtt}<section class="panel client-info-panel">
    <h2>Session settings</h2>
    <dl class="inspector-fields">
      <div>
        <dt>Clean start</dt>
        <dd>{flag(client.clean_start)}</dd>
      </div>
      <div>
        <dt>Expiry interval</dt>
        <dd>
          {client.expiry_interval === undefined ? 'Not reported' : `${client.expiry_interval} s`}
        </dd>
      </div>
      <div>
        <dt>Receive maximum</dt>
        <dd>{client.receive_maximum ?? 'Not reported'}</dd>
      </div>
      <div>
        <dt>Maximum packet size</dt>
        <dd>
          {client.max_packet_size === undefined ? 'Not reported' : bytes(client.max_packet_size)}
        </dd>
      </div>
      <div>
        <dt>Topic alias maximum</dt>
        <dd>{client.topic_alias_max ?? 'Not reported'}</dd>
      </div>
      <div>
        <dt>Will configured</dt>
        <dd>{flag(client.has_will)}</dd>
      </div>
      <div>
        <dt>Request response information</dt>
        <dd>{flag(client.request_response)}</dd>
      </div>
      <div>
        <dt>Request problem information</dt>
        <dd>{flag(client.request_problem)}</dd>
      </div>
    </dl>
  </section>{/if}
<section class="panel client-subscriptions-panel" class:amqp-consumers={!mqtt}>
  <h2>
    {mqtt ? 'Subscriptions' : 'Consumes from'}
    {#if mqtt}<span class="count-tag">{client.subscription_count}</span>{/if}
  </h2>
  {#if !mqtt}
    <div class="table-scroll">
      <table class="amqp-consumer-table">
        <thead>
          <tr>
            <th>Queue / topic filter</th>{#if hasConsumerGroups}<th>Consumer group</th>{/if}
          </tr>
        </thead>
        <tbody>
          {#each client.subscriptions ?? [] as sub}<tr>
              <td class="mono">{sub.filter}</td>
              {#if hasConsumerGroups}<td>{sub.consumer_group || '—'}</td>{/if}
            </tr>{:else}<tr>
              <td colspan={hasConsumerGroups ? 2 : 1}>
                <div class="empty-state">This connection has no active subscriptions.</div>
              </td>
            </tr>{/each}
        </tbody>
      </table>
    </div>
  {:else}
    {#each client.subscriptions ?? [] as sub}
      <div class="session-subscription subscription-info">
        <div>
          <a
            class="text-button"
            href={subscriptionURL(sub.filter, client.node_id)}
            onclick={(event) => followLink(event, () => onfilter(sub.filter, client.node_id))}
          >
            <code>{sub.filter}</code>
          </a><span class="protocol-tag">QoS {sub.qos}</span>
        </div>
        {#if mqtt}<dl class="inspector-fields compact-fields">
            <div>
              <dt>No local</dt>
              <dd>{flag(sub.no_local)}</dd>
            </div>
            <div>
              <dt>Retain as published</dt>
              <dd>{flag(sub.retain_as_published)}</dd>
            </div>
            <div>
              <dt>Retain handling</dt>
              <dd>{sub.retain_handling ?? 'Not reported'}</dd>
            </div>
            {#if sub.subscription_id !== undefined}<div>
                <dt>Subscription ID</dt>
                <dd>{sub.subscription_id}</dd>
              </div>{/if}
            {#if sub.consumer_group}<div>
                <dt>Consumer group</dt>
                <dd>{sub.consumer_group}</dd>
              </div>{/if}
          </dl>{/if}
      </div>
    {:else}<p class="inspector-context">No listed {mqtt ? 'subscriptions' : 'consumers'}.</p>{/each}
  {/if}
</section>
