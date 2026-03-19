import { NextResponse } from "next/server";
import { MOCK_SESSIONS } from "@/lib/api";

const API_URL = process.env.FLUXMQ_API_URL || "";

export async function GET(
	_req: Request,
	{ params }: { params: Promise<{ id: string }> },
) {
	const { id } = await params;

	if (API_URL) {
		try {
			const res = await fetch(
				`${API_URL}/api/v1/sessions/${encodeURIComponent(id)}`,
				{ cache: "no-store" },
			);
			if (res.status === 404) {
				return NextResponse.json(
					{ error: "Session not found" },
					{ status: 404 },
				);
			}
			if (!res.ok) {
				return NextResponse.json(
					{ error: `Backend returned ${res.status}` },
					{ status: res.status },
				);
			}
			return NextResponse.json(await res.json());
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
	return NextResponse.json(session);
}
