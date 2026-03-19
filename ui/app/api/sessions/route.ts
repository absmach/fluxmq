import { type NextRequest, NextResponse } from "next/server";
import { MOCK_SESSIONS, type SessionInfo } from "@/lib/api";

const API_URL = process.env.FLUXMQ_API_URL || "";
const NODE_URLS = (process.env.FLUXMQ_NODE_URLS || API_URL)
	.split(",")
	.map((u) => u.trim())
	.filter(Boolean);

const REQUEST_TIMEOUT_MS = 5000;

interface SessionsAPIResponse {
	sessions?: SessionInfo[];
	next_page_token?: string | null;
}

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

function buildSessionQuery(
	state: string | null,
	prefix: string | null,
	limit: string | null,
	pageToken: string | null,
): string {
	const params = new URLSearchParams();
	if (state && state !== "all") params.set("state", state);
	if (prefix) params.set("prefix", prefix);
	if (limit) {
		params.set("limit", limit);
	} else {
		// Ask for the complete set when the caller does not request pagination.
		params.set("limit", "0");
	}
	if (pageToken) params.set("page_token", pageToken);
	return params.toString();
}

async function fetchSessionsPage(
	nodeUrl: string,
	state: string | null,
	prefix: string | null,
	limit: string | null,
	pageToken: string | null,
): Promise<SessionsAPIResponse> {
	const qs = buildSessionQuery(state, prefix, limit, pageToken);
	const res = await fetch(`${nodeUrl}/api/v1/sessions${qs ? `?${qs}` : ""}`, {
		cache: "no-store",
		signal: AbortSignal.timeout(REQUEST_TIMEOUT_MS),
	});
	if (!res.ok) throw new Error(`Backend returned ${res.status}`);
	return (await res.json()) as SessionsAPIResponse;
}

async function fetchAllNodeSessions(
	target: NodeTarget,
	state: string | null,
	prefix: string | null,
): Promise<SessionInfo[]> {
	let pageToken: string | null = null;
	const sessions: SessionInfo[] = [];

	for (;;) {
		const page = await fetchSessionsPage(
			target.nodeUrl,
			state,
			prefix,
			null,
			pageToken,
		);
		const pageSessions = page.sessions ?? [];
		for (const session of pageSessions) {
			sessions.push({ ...session, node_id: target.nodeId });
		}
		pageToken = page.next_page_token ?? null;
		if (!pageToken) break;
	}

	return sessions;
}

export async function GET(req: NextRequest) {
	const { searchParams } = req.nextUrl;

	if (API_URL) {
		try {
			const state = searchParams.get("state");
			const prefix = searchParams.get("prefix");
			const limit = searchParams.get("limit");
			const pageToken = searchParams.get("page_token");
			const scope = searchParams.get("scope") === "node" ? "node" : "cluster";
			const nodeID = searchParams.get("node_id");

			const targets = await resolveNodeTargets();
			const defaultTarget = targets[0] ?? {
				nodeId: "single-node",
				nodeUrl: API_URL,
			};
			const requestedTarget = nodeID
				? targets.find((t) => t.nodeId === nodeID)
				: null;
			const selectedTarget = requestedTarget ?? defaultTarget;

			// Preserve pass-through behaviour when caller requests explicit pagination.
			if (limit !== null || pageToken !== null) {
				const data = await fetchSessionsPage(
					selectedTarget.nodeUrl,
					state,
					prefix,
					limit,
					pageToken,
				);
				const sessions = (data.sessions ?? []).map((session) => ({
					...session,
					node_id: selectedTarget.nodeId,
				}));
				return NextResponse.json({
					sessions,
					next_page_token: data.next_page_token ?? null,
				});
			}

			const selectedTargets =
				scope === "node"
					? [selectedTarget]
					: targets.length > 0
						? targets
						: [defaultTarget];
			const results = await Promise.all(
				selectedTargets.map(async (target) => {
					try {
						return {
							target,
							sessions: await fetchAllNodeSessions(target, state, prefix),
							error: null as Error | null,
						};
					} catch (err) {
						console.warn(
							`Could not fetch sessions for ${target.nodeId} at ${target.nodeUrl}`,
							err,
						);
						return {
							target,
							sessions: [] as SessionInfo[],
							error: err as Error,
						};
					}
				}),
			);

			const failedCount = results.filter((r) => r.error !== null).length;
			if (scope === "node" && failedCount > 0) {
				return NextResponse.json(
					{ error: "Could not reach FluxMQ broker node" },
					{ status: 503 },
				);
			}
			if (failedCount === results.length && results.length > 0) {
				return NextResponse.json(
					{ error: "Could not reach FluxMQ broker" },
					{ status: 503 },
				);
			}

			const merged = results
				.flatMap((r) => r.sessions)
				.sort((a, b) =>
					a.client_id === b.client_id
						? (a.node_id ?? "").localeCompare(b.node_id ?? "")
						: a.client_id.localeCompare(b.client_id),
				);

			return NextResponse.json({ sessions: merged, next_page_token: null });
		} catch (err) {
			console.error("Failed to fetch sessions:", err);
			return NextResponse.json(
				{ error: "Could not reach FluxMQ broker" },
				{ status: 503 },
			);
		}
	}

	// Mock fallback — apply client-side filtering to mirror server behaviour
	const state = searchParams.get("state");
	const prefix = searchParams.get("prefix");
	let sessions = [...MOCK_SESSIONS];
	if (state && state !== "all") {
		sessions = sessions.filter((s) => s.state === state);
	}
	if (prefix) {
		sessions = sessions.filter((s) =>
			s.client_id.toLowerCase().startsWith(prefix.toLowerCase()),
		);
	}
	return NextResponse.json({ sessions, next_page_token: null });
}
