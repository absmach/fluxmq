import type { SessionInfo } from "@/lib/api";
import {
	getSession,
	getSessions,
	type SessionsParams,
} from "@/lib/services/sessions";

export interface SubscriptionClient {
	client_id: string;
	node_id?: string;
	qos: number;
}

export interface AggregatedSubscription {
	filter: string;
	subscriber_count: number;
	max_qos: number;
	clients: SubscriptionClient[];
}

export interface AggregatedSubscriptionsParams {
	scope?: SessionsParams["scope"];
	nodeId?: SessionsParams["nodeId"];
}

export async function getAggregatedSubscriptions(
	params: AggregatedSubscriptionsParams = {},
): Promise<AggregatedSubscription[]> {
	const { sessions } = await getSessions({
		state: "connected",
		scope: params.scope,
		nodeId: params.nodeId,
	});

	const details = await Promise.all(
		sessions.map(async (session) => {
			try {
				const detail = await getSession(session.client_id, session.node_id);
				return {
					...detail,
					node_id: detail.node_id ?? session.node_id,
				} as SessionInfo;
			} catch {
				return null;
			}
		}),
	);

	const map = new Map<string, AggregatedSubscription>();
	for (const detail of details) {
		if (!detail?.subscriptions) continue;
		for (const sub of detail.subscriptions) {
			const existing = map.get(sub.filter);
			const nextClient: SubscriptionClient = {
				client_id: detail.client_id,
				node_id: detail.node_id,
				qos: sub.qos,
			};

			if (existing) {
				existing.max_qos = Math.max(existing.max_qos, sub.qos);
				const duplicate = existing.clients.some(
					(client) =>
						client.client_id === nextClient.client_id &&
						(client.node_id ?? "") === (nextClient.node_id ?? ""),
				);
				if (!duplicate) {
					existing.clients.push(nextClient);
				}
				existing.subscriber_count = existing.clients.length;
				continue;
			}

			map.set(sub.filter, {
				filter: sub.filter,
				subscriber_count: 1,
				max_qos: sub.qos,
				clients: [nextClient],
			});
		}
	}

	return Array.from(map.values())
		.map((sub) => ({
			...sub,
			clients: [...sub.clients].sort((a, b) => {
				if (a.client_id === b.client_id) {
					return (a.node_id ?? "").localeCompare(b.node_id ?? "");
				}
				return a.client_id.localeCompare(b.client_id);
			}),
		}))
		.sort((a, b) => b.subscriber_count - a.subscriber_count);
}
