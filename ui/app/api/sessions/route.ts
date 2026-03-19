import { type NextRequest, NextResponse } from "next/server";
import { MOCK_SESSIONS } from "@/lib/api";

const API_URL = process.env.FLUXMQ_API_URL || "";

export async function GET(req: NextRequest) {
	const { searchParams } = req.nextUrl;

	if (API_URL) {
		try {
			const params = new URLSearchParams();
			const state = searchParams.get("state");
			const prefix = searchParams.get("prefix");
			const limit = searchParams.get("limit");
			const pageToken = searchParams.get("page_token");

			if (state && state !== "all") params.set("state", state);
			if (prefix) params.set("prefix", prefix);
			if (limit) params.set("limit", limit);
			if (pageToken) params.set("page_token", pageToken);

			const qs = params.toString();
			const res = await fetch(
				`${API_URL}/api/v1/sessions${qs ? `?${qs}` : ""}`,
				{ cache: "no-store" },
			);
			if (!res.ok) {
				return NextResponse.json(
					{ error: `Backend returned ${res.status}` },
					{ status: res.status },
				);
			}
			const data = await res.json();
			return NextResponse.json({
				sessions: data.sessions ?? [],
				next_page_token: data.next_page_token ?? null,
			});
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
