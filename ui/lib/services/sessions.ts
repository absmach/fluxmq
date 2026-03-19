import type { SessionInfo } from "@/lib/api";

export interface SessionsParams {
	state?: "all" | "connected" | "disconnected";
	prefix?: string;
	limit?: number;
	pageToken?: string;
}

export interface SessionsResult {
	sessions: SessionInfo[];
	nextPageToken: string | null;
}

export async function getSessions(
	params: SessionsParams = {},
): Promise<SessionsResult> {
	const qs = new URLSearchParams();
	if (params.state && params.state !== "all") qs.set("state", params.state);
	if (params.prefix) qs.set("prefix", params.prefix);
	if (params.limit) qs.set("limit", String(params.limit));
	if (params.pageToken) qs.set("page_token", params.pageToken);

	const url = `/api/sessions${qs.toString() ? `?${qs}` : ""}`;
	const res = await fetch(url, { cache: "no-store" });
	if (!res.ok) throw new Error(`Failed to fetch sessions: ${res.status}`);
	const data = await res.json();
	return {
		sessions: data.sessions ?? [],
		nextPageToken: data.next_page_token ?? null,
	};
}

export async function getSession(id: string): Promise<SessionInfo> {
	const res = await fetch(`/api/sessions/${encodeURIComponent(id)}`, {
		cache: "no-store",
	});
	if (!res.ok)
		throw new Error(`Failed to fetch session "${id}": ${res.status}`);
	return res.json();
}
