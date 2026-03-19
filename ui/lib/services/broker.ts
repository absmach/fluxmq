import type { BrokerStatus, NodeInfo } from "@/lib/api";

export interface BrokerOverview {
	status: BrokerStatus;
	nodes: NodeInfo[];
}

export async function getBrokerOverview(): Promise<BrokerOverview> {
	const res = await fetch("/api/broker/overview", { cache: "no-store" });
	if (!res.ok)
		throw new Error(`Failed to fetch broker overview: ${res.status}`);
	return res.json();
}
