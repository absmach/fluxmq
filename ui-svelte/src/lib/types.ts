export type Page = 'overview' | 'sessions' | 'subscriptions' | 'nodes' | 'health' | 'queues';
export interface Point {
  metrics?: Record<string, number>;
  time: number;
  incoming: number;
  outgoing: number;
  bandwidth: number;
  bytes_in?: number;
  bytes_out?: number;
  connections: number;
}
export interface QueueMetrics {
  capture_failures?: number;
  capture_dropped?: number;
  claims?: { attempts?: number; successes?: number; failures?: number };
  steals?: { attempts?: number; successes?: number; failures?: number };
  acknowledgments?: {
    ack?: number;
    nack?: number;
    reject?: number;
    dlq?: number;
    dlq_transfer_failures?: number;
    poison_without_dlq?: number;
    poison_pending?: number;
    poison_pending_no_destination?: number;
  };
  pending?: { current?: number; high_water?: number };
}
export interface ProtocolMetrics {
  connections?: { current?: number; total?: number; disconnections?: number };
  messages?: { received?: number; sent?: number; publish_received?: number; publish_sent?: number };
  bytes?: { received?: number; sent?: number };
  channels?: number;
  consumers?: number;
  errors?: Record<string, number>;
  local_principals?: {
    active_connections?: number;
    publish_timeouts?: number;
    publish_rejections?: number;
    authentication?: { success?: number; failure?: number };
    authorization?: {
      publish_denied?: number;
      subscribe_denied?: number;
      operation_denied?: number;
    };
    reloads?: { success?: number; failure?: number; forced_disconnects?: number };
  };
}
export interface BrokerNode {
  ingress_metrics?: Record<string, { publishes: number; payload_bytes: number }>;
  queue_metrics?: QueueMetrics;
  protocol_metrics?: Record<string, ProtocolMetrics>;
  unmonitored?: boolean;
  reported_healthy?: boolean;
  retained_messages?: number;
  cluster_mode?: boolean;
  version?: string;
  transport_address?: string;
  protocol_errors?: number;
  auth_errors?: number;
  authz_errors?: number;
  packet_errors?: number;
  id: string;
  address: string;
  leader: boolean;
  healthy: boolean;
  uptime: number;
  connections: number;
  subscriptions: number;
  received: number;
  sent: number;
  bytes_in: number;
  bytes_out: number;
  errors: number;
  incoming: number;
  outgoing: number;
}
export interface Session {
  consumer_count?: number;
  connection_name?: string;
  state?: string;
  version?: number;
  clean_start?: boolean;
  expiry_interval?: number;
  disconnected_at?: string;
  receive_maximum?: number;
  max_packet_size?: number;
  topic_alias_max?: number;
  request_response?: boolean;
  request_problem?: boolean;
  has_will?: boolean;
  client_id: string;
  node_id: string;
  protocol: string;
  connected: boolean;
  subscription_count: number;
  inflight_count: number;
  offline_queue_depth: number;
  connected_at?: string;
  subscriptions?: {
    consumer_count?: number;
    filter: string;
    qos: number;
    no_local?: boolean;
    retain_as_published?: boolean;
    retain_handling?: number;
    consumer_group?: string;
    subscription_id?: number;
  }[];
}
export interface Subscription {
  filter: string;
  subscriber_count: number;
  max_qos: number;
  node_id: string;
}
export interface Snapshot {
  mode: 'demo' | 'live';
  timestamp: number;
  nodes: BrokerNode[];
  sessions: Session[];
  subscriptions: Subscription[];
  points: Record<string, Point>;
  history?: Record<string, Point[]>;
  warning: string;
  inventory_limited: boolean;
}
export const number = (n: number) =>
  new Intl.NumberFormat('en-US', { maximumFractionDigits: 0 }).format(n);
export const compact = (n: number) =>
  new Intl.NumberFormat('en-US', { notation: 'compact', maximumFractionDigits: 1 }).format(n);
export function bytes(n: number) {
  if (n >= 1e9) return `${(n / 1e9).toFixed(1)} GB`;
  if (n >= 1e6) return `${(n / 1e6).toFixed(1)} MB`;
  if (n >= 1e3) return `${(n / 1e3).toFixed(1)} KB`;
  return `${Math.round(n)} B`;
}
export function uptime(n: number) {
  if (n > 86400) return `${Math.floor(n / 86400)}d ${Math.floor((n % 86400) / 3600)}h`;
  if (n > 3600) return `${Math.floor(n / 3600)}h ${Math.floor((n % 3600) / 60)}m`;
  return `${Math.floor(n / 60)}m ${Math.floor(n % 60)}s`;
}
export function protocolLabel(p: string) {
  return (
    (
      {
        mqtt5: 'MQTT 5.0',
        'mqtt3.1.1': 'MQTT 3.1.1',
        'mqtt3.1': 'MQTT 3.1',
        'amqp0.9.1': 'AMQP 0.9.1',
        'amqp1.0': 'AMQP 1.0',
      } as Record<string, string>
    )[p] || p
  );
}

export function nodeRole(n: BrokerNode) {
  if (n.cluster_mode === false) return 'Standalone';
  if (n.leader) return 'Leader';
  return n.cluster_mode === true ? 'Follower' : 'Not reported';
}
export function sessionProtocol(s: Pick<Session, 'protocol' | 'version' | 'client_id'>) {
  const raw = (s.protocol || '').trim().toLowerCase();
  if (['mqtt5', 'mqtt 5', 'mqtt 5.0'].includes(raw) || s.version === 5) return 'mqtt5';
  if (['mqtt3.1.1', 'mqtt 3.1.1', 'mqtt311'].includes(raw) || s.version === 4) return 'mqtt3.1.1';
  if (['mqtt3.1', 'mqtt 3.1', 'mqtt31'].includes(raw) || s.version === 3) return 'mqtt3.1';
  if (
    ['amqp', 'amqp0.9.1', 'amqp 0.9.1', 'amqp091', 'amqp_0_9_1'].includes(raw) ||
    s.client_id.startsWith('amqp091:')
  )
    return 'amqp0.9.1';
  if (
    ['amqp1', 'amqp1.0', 'amqp 1.0', 'amqp10', 'amqp_1_0'].includes(raw) ||
    s.client_id.startsWith('amqp:')
  )
    return 'amqp1.0';
  return raw || 'unknown';
}

export interface SubscriptionClient {
  client_id: string;
  node_id: string;
  qos: number;
}
export interface ClientListState {
  clients: SubscriptionClient[];
  next: Record<string, string>;
  failed: string[];
  search: string;
  page: number;
  pageSize: number;
  loaded: boolean;
}

export function nodeStatus(n: BrokerNode) {
  return n.unmonitored ? 'Not monitored' : n.healthy ? 'Healthy' : 'Unavailable';
}
