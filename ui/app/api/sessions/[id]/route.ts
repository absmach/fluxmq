import { NextResponse } from "next/server";
import { MOCK_SESSIONS, type SessionInfo } from "@/lib/api";

const API_URL = process.env.FLUXMQ_API_URL || "";
const NODE_URLS = (process.env.FLUXMQ_NODE_URLS || API_URL)
	.split(",")
	.map((u) => u.trim())
	.filter(Boolean);

const REQUEST_TIMEOUT_MS = 5000;

interface NodeTarget {
	nodeId: string;
	nodeUrl: string;
}

async function fetchNodeID(nodeUrl: string): Promise<string> {
	try {
		const res = await fetch(`${nodeUrl}/api/v1/overview`, {
			cache: "no-store",
			signal: AbortSignal.timeout(REQUEST_TIMEOUT_MS),
		});
		if (!res.ok) return nodeUrl;
		const data = await res.json();
		return String(data.node_id || nodeUrl);
	} catch {
		return nodeUrl;
	}
}

async function resolveNodeTargets(): Promise<NodeTarget[]> {
	const seen = new Set<string>();
	const uniqueUrls = NODE_URLS.filter((url) => {
		if (seen.has(url)) return false;
		seen.add(url);
		return true;
	});

	const resolved = await Promise.all(
		uniqueUrls.map(async (nodeUrl) => ({
			nodeUrl,
			nodeId: await fetchNodeID(nodeUrl),
		})),
	);

	const byNodeID = new Map<string, NodeTarget>();
	for (const target of resolved) {
		if (!byNodeID.has(target.nodeId)) {
			byNodeID.set(target.nodeId, target);
		}
	}
	return [...byNodeID.values()];
}

async function fetchSessionFromNode(
	nodeUrl: string,
	id: string,
): Promise<Response> {
	return fetch(`${nodeUrl}/api/v1/sessions/${encodeURIComponent(id)}`, {
		cache: "no-store",
		signal: AbortSignal.timeout(REQUEST_TIMEOUT_MS),
	});
}

export async function GET(
	req: Request,
	{ params }: { params: Promise<{ id: string }> },
) {
	const { id } = await params;
	const { searchParams } = new URL(req.url);
	const requestedNodeID = searchParams.get("node_id");

	if (API_URL) {
		try {
			const targets = await resolveNodeTargets();
			const defaultTarget = targets[0] ?? {
				nodeId: "single-node",
				nodeUrl: API_URL,
			};

			const targetOrder =
				requestedNodeID === null
					? [
							defaultTarget,
							...targets.filter((t) => t.nodeId !== defaultTarget.nodeId),
						]
					: (() => {
							const requestedTarget = targets.find(
								(t) => t.nodeId === requestedNodeID,
							);
							return requestedTarget ? [requestedTarget] : [defaultTarget];
						})();

			let hadNetworkError = false;
			for (const target of targetOrder) {
				try {
					const res = await fetchSessionFromNode(target.nodeUrl, id);
					if (res.status === 404) {
						continue;
					}
					if (!res.ok) {
						return NextResponse.json(
							{ error: `Backend returned ${res.status}` },
							{ status: res.status },
						);
					}
					const session = (await res.json()) as SessionInfo;
					return NextResponse.json({
						...session,
						node_id: session.node_id ?? target.nodeId,
					});
				} catch {
					hadNetworkError = true;
				}
			}

			if (hadNetworkError) {
				return NextResponse.json(
					{ error: "Could not reach FluxMQ broker" },
					{ status: 503 },
				);
			}
			return NextResponse.json({ error: "Session not found" }, { status: 404 });
		} catch (err) {
			console.error("Failed to fetch session:", err);
			return NextResponse.json(
				{ error: "Could not reach FluxMQ broker" },
				{ status: 503 },
			);
		}
	}

	const session = MOCK_SESSIONS.find((s) => s.client_id === id);
	if (!session) {
		return NextResponse.json({ error: "Session not found" }, { status: 404 });
	}
	if (requestedNodeID) {
		return NextResponse.json({ ...session, node_id: requestedNodeID });
	}
	return NextResponse.json(session);
}
