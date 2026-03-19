import { type NextRequest, NextResponse } from "next/server";

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

interface BackendSubscription {
	filter: string;
	subscriber_count: number;
	max_qos: number;
}

interface BackendSubscriptionsResponse {
	subscriptions?: BackendSubscription[];
	next_page_token?: string | null;
}

function parseLimit(raw: string | null): number | null {
	if (raw === null || raw.trim() === "") return null;
	const parsed = Number.parseInt(raw, 10);
	if (!Number.isFinite(parsed) || parsed < 0) return Number.NaN;
	return parsed;
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

function buildSubscriptionsQuery(
	state: string | null,
	prefix: string | null,
	limit: string | null,
	pageToken: string | null,
): string {
	const params = new URLSearchParams();
	if (state) params.set("state", state);
	if (prefix) params.set("prefix", prefix);
	if (limit) {
		params.set("limit", limit);
	} else {
		// Ask backend for all rows when route-level pagination is used.
		params.set("limit", "0");
	}
	if (pageToken) params.set("page_token", pageToken);
	return params.toString();
}

async function fetchSubscriptionsPage(
	nodeUrl: string,
	state: string | null,
	prefix: string | null,
	limit: string | null,
	pageToken: string | null,
): Promise<BackendSubscriptionsResponse> {
	const qs = buildSubscriptionsQuery(state, prefix, limit, pageToken);
	const res = await fetch(
		`${nodeUrl}/api/v1/subscriptions${qs ? `?${qs}` : ""}`,
		{
			cache: "no-store",
			signal: AbortSignal.timeout(REQUEST_TIMEOUT_MS),
		},
	);
	if (!res.ok) throw new Error(`Backend returned ${res.status}`);
	return (await res.json()) as BackendSubscriptionsResponse;
}

async function fetchAllNodeSubscriptions(
	target: NodeTarget,
	state: string | null,
	prefix: string | null,
): Promise<BackendSubscription[]> {
	const subscriptions: BackendSubscription[] = [];
	let pageToken: string | null = null;

	for (;;) {
		const page = await fetchSubscriptionsPage(
			target.nodeUrl,
			state,
			prefix,
			null,
			pageToken,
		);
		subscriptions.push(...(page.subscriptions ?? []));
		pageToken = page.next_page_token ?? null;
		if (!pageToken) break;
	}

	return subscriptions;
}

export async function GET(req: NextRequest) {
	const { searchParams } = req.nextUrl;

	if (!API_URL) {
		return NextResponse.json({ subscriptions: [], next_page_token: null });
	}

	const state = searchParams.get("state");
	const prefix = searchParams.get("prefix");
	const scope = searchParams.get("scope") === "node" ? "node" : "cluster";
	const nodeID = searchParams.get("node_id");
	const limit = parseLimit(searchParams.get("limit"));
	const pageToken = searchParams.get("page_token");

	if (Number.isNaN(limit)) {
		return NextResponse.json(
			{ error: "limit must be a non-negative integer" },
			{ status: 400 },
		);
	}

	try {
		const targets = await resolveNodeTargets();
		const defaultTarget = targets[0] ?? {
			nodeId: "single-node",
			nodeUrl: API_URL,
		};
		const requestedTarget = nodeID
			? targets.find((t) => t.nodeId === nodeID)
			: null;
		const selectedTarget = requestedTarget ?? defaultTarget;

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
						subscriptions: await fetchAllNodeSubscriptions(
							target,
							state,
							prefix,
						),
						error: null as Error | null,
					};
				} catch (err) {
					console.warn(
						`Could not fetch subscriptions for ${target.nodeId} at ${target.nodeUrl}`,
						err,
					);
					return {
						subscriptions: [] as BackendSubscription[],
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

		const mergedByFilter = new Map<string, BackendSubscription>();
		for (const result of results) {
			for (const item of result.subscriptions) {
				const existing = mergedByFilter.get(item.filter);
				if (!existing) {
					mergedByFilter.set(item.filter, { ...item });
					continue;
				}
				existing.subscriber_count += item.subscriber_count;
				existing.max_qos = Math.max(existing.max_qos, item.max_qos);
			}
		}

		const merged = [...mergedByFilter.values()].sort((a, b) =>
			a.filter.localeCompare(b.filter),
		);

		let start = 0;
		if (pageToken) {
			start = merged.findIndex((item) => item.filter > pageToken);
			if (start < 0) start = merged.length;
		}

		let end = merged.length;
		if (limit !== null && limit > 0) {
			end = Math.min(end, start + limit);
		}
		const page = merged.slice(start, end);
		const nextPageToken =
			end < merged.length && page.length > 0
				? page[page.length - 1].filter
				: null;

		return NextResponse.json({
			subscriptions: page,
			next_page_token: nextPageToken,
		});
	} catch (err) {
		console.error("Failed to fetch subscriptions:", err);
		return NextResponse.json(
			{ error: "Could not reach FluxMQ broker" },
			{ status: 503 },
		);
	}
}
