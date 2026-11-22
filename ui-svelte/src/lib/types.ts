export type Page = 'overview' | 'sessions' | 'subscriptions' | 'nodes' | 'health';
export interface Point {
  time: number;
  incoming: number;
  outgoing: number;
  bandwidth: number;
  connections: number;
}
export interface BrokerNode {
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
  client_id: string;
  node_id: string;
  protocol: string;
  connected: boolean;
  subscription_count: number;
  inflight_count: number;
  offline_queue_depth: number;
  connected_at?: string;
  subscriptions?: { filter: string; qos: number }[];
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
