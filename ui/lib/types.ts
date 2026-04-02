export interface BrokerStatus {
	node_id: string;
	is_leader: boolean;
	cluster_mode: boolean;
	node_count?: number;
	sessions: number;
	sessions_total: number;
	connections_total: number;
	connections_disconnections: number;
	messages_received: number;
	messages_sent: number;
	publish_received: number;
	publish_sent: number;
	bytes_received: number;
	bytes_sent: number;
	subscriptions: number;
	retained_messages: number;
	uptime_seconds: number;
	auth_errors: number;
	authz_errors: number;
	protocol_errors: number;
	packet_errors: number;
}

export interface NodeInfo {
	node_id: string;
	is_leader: boolean;
	healthy?: boolean;
	addr: string;
	uptime_seconds: number;
	sessions?: number;
	subscriptions?: number;
	messages_received?: number;
	messages_sent?: number;
	bytes_received?: number;
	bytes_sent?: number;
}

export interface SessionSubscription {
	filter: string;
	qos: number;
	no_local: boolean;
	retain_as_published: boolean;
	retain_handling: number;
	consumer_group?: string;
	subscription_id?: number;
}

export interface SessionInfo {
	client_id: string;
	node_id?: string;
	connection_name?: string;
	state: string;
	connected: boolean;
	protocol: string;
	version: number;
	clean_start: boolean;
	expiry_interval: number;
	connected_at?: string;
	disconnected_at?: string;
	receive_maximum: number;
	max_packet_size: number;
	topic_alias_max: number;
	request_response: boolean;
	request_problem: boolean;
	has_will: boolean;
	subscription_count: number;
	inflight_count: number;
	offline_queue_depth: number;
	subscriptions?: SessionSubscription[];
}

export interface MockHistoryPoint {
	time: string;
	sessions: number;
	msgsIn: number;
	msgsOut: number;
	bytesIn: number;
	bytesOut: number;
}
