import { NextResponse } from "next/server";
import {
	type BrokerStatus,
	MOCK_NODES,
	MOCK_STATUS,
	type NodeInfo,
} from "@/lib/api";

const API_URL = process.env.FLUXMQ_API_URL || "";
const NODE_URLS = (process.env.FLUXMQ_NODE_URLS || API_URL)
	.split(",")
	.map((u) => u.trim())
	.filter(Boolean);

export interface OverviewResponse {
	status: BrokerStatus;
	nodes: NodeInfo[];
}

interface NodeStats {
	uptime_seconds: number;
	connections: { current: number };
	messages: { received: number; sent: number };
	bytes: { received: number; sent: number };
	by_protocol?: {
		mqtt?: { subscriptions?: { active: number } };
	};
}

async function fetchNodeStats(
	nodeUrl: string,
	nodeId: string,
): Promise<Partial<NodeInfo>> {
	try {
		const res = await fetch(`${nodeUrl}/api/v1/stats`, {
			cache: "no-store",
			signal: AbortSignal.timeout(3000),
		});
		if (!res.ok) return {};
		const s: NodeStats = await res.json();
		return {
			sessions: s.connections?.current ?? 0,
			subscriptions: s.by_protocol?.mqtt?.subscriptions?.active ?? 0,
			messages_received: s.messages?.received ?? 0,
			messages_sent: s.messages?.sent ?? 0,
			bytes_received: s.bytes?.received ?? 0,
			bytes_sent: s.bytes?.sent ?? 0,
			uptime_seconds: s.uptime_seconds ?? 0,
		};
	} catch {
		console.warn(`Could not fetch stats for ${nodeId} at ${nodeUrl}`);
		return {};
	}
}

export async function GET() {
	if (API_URL) {
		try {
			const res = await fetch(`${API_URL}/api/v1/overview`, {
				cache: "no-store",
			});
			if (!res.ok) {
				return NextResponse.json(
					{ error: `Backend returned ${res.status}` },
					{ status: res.status },
				);
			}
			const data = await res.json();

			const mqtt = data.stats?.by_protocol?.mqtt;
			const status: BrokerStatus = {
				node_id: data.node_id,
				is_leader: data.is_leader,
				cluster_mode: data.cluster_mode,
				node_count: data.cluster?.nodes?.length ?? 0,
				sessions:
					data.sessions?.connected ?? data.stats?.connections?.current ?? 0,
				sessions_total: data.sessions?.total ?? 0,
				connections_total: data.stats?.connections?.total ?? 0,
				connections_disconnections:
					data.stats?.connections?.disconnections ?? 0,
				messages_received: data.stats?.messages?.received ?? 0,
				messages_sent: data.stats?.messages?.sent ?? 0,
				publish_received: mqtt?.messages?.publish_received ?? 0,
				publish_sent: mqtt?.messages?.publish_sent ?? 0,
				bytes_received: data.stats?.bytes?.received ?? 0,
				bytes_sent: data.stats?.bytes?.sent ?? 0,
				subscriptions: mqtt?.subscriptions?.active ?? 0,
				retained_messages: mqtt?.subscriptions?.retained_messages ?? 0,
				uptime_seconds: data.uptime_seconds ?? 0,
				auth_errors: mqtt?.errors?.auth ?? 0,
				authz_errors: mqtt?.errors?.authz ?? 0,
				protocol_errors: data.stats?.errors?.protocol ?? 0,
				packet_errors: mqtt?.errors?.packet ?? 0,
			};

			// Build a map of nodeId → admin API URL from FLUXMQ_NODE_URLS
			// Fetch stats from each node in parallel
			const clusterNodes: {
				id: string;
				address: string;
				leader: boolean;
				healthy: boolean;
				uptime_seconds: number;
			}[] = data.cluster?.nodes ?? [];

			const nodeStatsResults = await Promise.all(
				NODE_URLS.map(async (url) => {
					try {
						const idRes = await fetch(`${url}/api/v1/overview`, {
							cache: "no-store",
							signal: AbortSignal.timeout(3000),
						});
						const nodeId = idRes.ok
							? ((await idRes.json()).node_id as string)
							: url;
						const stats = await fetchNodeStats(url, nodeId);
						return { nodeId, stats };
					} catch {
						return { nodeId: url, stats: {} };
					}
				}),
			);

			const statsById: Record<string, Partial<NodeInfo>> = {};
			for (const { nodeId, stats } of nodeStatsResults) {
				statsById[nodeId] = stats;
			}

			const nodes: NodeInfo[] = clusterNodes.map((n) => ({
				node_id: n.id,
				is_leader: n.leader,
				healthy: n.healthy,
				addr: n.address,
				uptime_seconds:
					statsById[n.id]?.uptime_seconds ?? n.uptime_seconds ?? 0,
				sessions: statsById[n.id]?.sessions,
				subscriptions: statsById[n.id]?.subscriptions,
				messages_received: statsById[n.id]?.messages_received,
				messages_sent: statsById[n.id]?.messages_sent,
				bytes_received: statsById[n.id]?.bytes_received,
				bytes_sent: statsById[n.id]?.bytes_sent,
			}));

			return NextResponse.json({ status, nodes } satisfies OverviewResponse);
		} catch (err) {
			console.error("Failed to fetch broker overview:", err);
			return NextResponse.json(
				{ error: "Could not reach FluxMQ broker" },
				{ status: 503 },
			);
		}
	}

	// Mock fallback
	return NextResponse.json({
		status: MOCK_STATUS,
		nodes: MOCK_NODES,
	} satisfies OverviewResponse);
}
