import type { SessionsParams } from "@/lib/services/sessions";

export interface SubscriptionClient {
	client_id: string;
	node_id?: string;
	qos: number;
}

export interface AggregatedSubscription {
	filter: string;
	subscriber_count: number;
	max_qos: number;
	clients?: SubscriptionClient[];
}

export interface AggregatedSubscriptionsParams {
	scope?: SessionsParams["scope"];
	nodeId?: SessionsParams["nodeId"];
	state?: "all" | "connected" | "disconnected";
	prefix?: string;
}

export async function getAggregatedSubscriptions(
	params: AggregatedSubscriptionsParams = {},
): Promise<AggregatedSubscription[]> {
	const qs = new URLSearchParams();
	if (params.scope) qs.set("scope", params.scope);
	if (params.nodeId) qs.set("node_id", params.nodeId);
	if (params.state) qs.set("state", params.state);
	if (params.prefix) qs.set("prefix", params.prefix);

	const url = `/api/subscriptions${qs.toString() ? `?${qs}` : ""}`;
	const res = await fetch(url, { cache: "no-store" });
	if (!res.ok) {
		throw new Error(`Failed to fetch subscriptions: ${res.status}`);
	}
	const data = await res.json();
	const subscriptions = (data.subscriptions ?? []) as AggregatedSubscription[];
	return [...subscriptions].sort((a, b) =>
		a.subscriber_count === b.subscriber_count
			? a.filter.localeCompare(b.filter)
			: b.subscriber_count - a.subscriber_count,
	);
}

export interface SubscriptionClientsParams {
	scope?: SessionsParams["scope"];
	nodeId?: SessionsParams["nodeId"];
	state?: "all" | "connected" | "disconnected";
	prefix?: string;
}

export async function getSubscriptionClients(
	filter: string,
	params: SubscriptionClientsParams = {},
): Promise<SubscriptionClient[]> {
	const qs = new URLSearchParams();
	if (params.scope) qs.set("scope", params.scope);
	if (params.nodeId) qs.set("node_id", params.nodeId);
	if (params.state) qs.set("state", params.state);
	if (params.prefix) qs.set("prefix", params.prefix);

	const url = `/api/subscriptions/${encodeURIComponent(filter)}/clients${
		qs.toString() ? `?${qs}` : ""
	}`;
	const res = await fetch(url, { cache: "no-store" });
	if (!res.ok) {
		throw new Error(
			`Failed to fetch subscription clients for "${filter}": ${res.status}`,
		);
	}
	const data = await res.json();
	return (data.clients ?? []) as SubscriptionClient[];
}
