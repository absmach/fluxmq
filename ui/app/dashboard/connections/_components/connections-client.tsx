"use client";

import { Search, Wifi } from "lucide-react";
import { useEffect, useState } from "react";
import { Badge } from "@/components/ui/badge";
import { Card, CardContent } from "@/components/ui/card";
import { Input } from "@/components/ui/input";
import {
	Table,
	TableBody,
	TableCell,
	TableHead,
	TableHeader,
	TableRow,
} from "@/components/ui/table";
import { TablePagination } from "@/components/ui/table-pagination";
import type { SessionInfo } from "@/lib/api";
import { getSessions } from "@/lib/services/sessions";

function formatProtocol(protocol: string, version: number): string {
	if (protocol === "mqtt5" || version === 5) return "MQTT 5.0";
	if (protocol === "mqtt3.1.1" || version === 4) return "MQTT 3.1.1";
	if (protocol === "mqtt3.1" || version === 3) return "MQTT 3.1";
	return protocol;
}

const PROTOCOL_COLORS: Record<string, string> = {
	mqtt5: "bg-flux-blue/10 text-flux-blue border-flux-blue/20",
	"mqtt3.1.1": "bg-flux-teal/10 text-flux-teal border-flux-teal/20",
	"mqtt3.1": "bg-flux-teal/10 text-flux-teal border-flux-teal/20",
};

const ConnectionsClient = () => {
	const [sessions, setSessions] = useState<SessionInfo[]>([]);
	const [search, setSearch] = useState("");
	const [page, setPage] = useState(1);
	const [limit, setLimit] = useState(10);

	useEffect(() => {
		getSessions({ state: "connected" })
			.then(({ sessions }) => setSessions(sessions))
			.catch(console.error);
	}, []);

	const filtered = search
		? sessions.filter((s) =>
				s.client_id.toLowerCase().includes(search.toLowerCase()),
			)
		: sessions;

	useEffect(() => {
		setPage(1);
	}, [search]);

	const totalPages = Math.max(1, Math.ceil(filtered.length / limit));
	const paginated = filtered.slice((page - 1) * limit, page * limit);

	return (
		<div className="p-8 space-y-6">
			<div className="flex items-start justify-between gap-4">
				<div>
					<h1 className="text-3xl font-bold text-flux-text mb-1">
						Connections
					</h1>
					<p className="text-flux-text-muted">
						Active client connections on the broker
					</p>
				</div>
				<Badge
					variant="outline"
					className="flex items-center gap-1.5 text-sm px-3 py-1.5 bg-flux-green/10 text-flux-green border-flux-green/30"
				>
					<Wifi className="w-3.5 h-3.5" />
					{sessions.length} connected
				</Badge>
			</div>

			<Card className="border-flux-card-border bg-flux-card">
				<CardContent className="p-6">
					<div className="flex items-center justify-between mb-6">
						<div className="relative flex-1 max-w-xs">
							<Search
								className="absolute left-3 top-1/2 -translate-y-1/2 text-flux-text-muted"
								size={16}
							/>
							<Input
								type="text"
								placeholder="Search by client ID..."
								value={search}
								onChange={(e) => setSearch(e.target.value)}
								className="pl-9 bg-flux-bg border-flux-card-border text-flux-text placeholder:text-flux-text-muted focus-visible:ring-flux-blue"
							/>
						</div>
					</div>

					<Table>
						<TableHeader>
							<TableRow className="border-flux-card-border hover:bg-transparent">
								<TableHead>Client ID</TableHead>
								<TableHead>Protocol</TableHead>
								<TableHead className="text-right">Subscriptions</TableHead>
								<TableHead className="text-right">Inflight</TableHead>
								<TableHead>Clean Start</TableHead>
								<TableHead>Has Will</TableHead>
								<TableHead>Connected At</TableHead>
							</TableRow>
						</TableHeader>
						<TableBody>
							{paginated.map((s) => (
								<TableRow
									key={s.client_id}
									className="border-flux-card-border hover:bg-flux-hover"
								>
									<TableCell className="font-mono text-sm text-flux-text font-medium py-4">
										{s.client_id}
									</TableCell>

									<TableCell>
										<Badge
											variant="outline"
											className={`text-xs ${PROTOCOL_COLORS[s.protocol] ?? "bg-flux-blue/10 text-flux-blue border-flux-blue/20"}`}
										>
											{formatProtocol(s.protocol, s.version)}
										</Badge>
									</TableCell>

									<TableCell className="text-right text-sm text-flux-text tabular-nums py-4">
										{s.subscription_count}
									</TableCell>

									<TableCell className="text-right text-sm tabular-nums py-4">
										{s.inflight_count > 0 ? (
											<span className="text-flux-orange font-medium">
												{s.inflight_count}
											</span>
										) : (
											<span className="text-flux-text-muted">0</span>
										)}
									</TableCell>

									<TableCell>
										<Badge
											variant="outline"
											className={
												s.clean_start
													? "bg-flux-green/10 text-flux-green border-flux-green/20"
													: "bg-flux-text-muted/10 text-flux-text-muted border-flux-card-border"
											}
										>
											{s.clean_start ? "Yes" : "No"}
										</Badge>
									</TableCell>

									<TableCell>
										<Badge
											variant="outline"
											className={
												s.has_will
													? "bg-flux-orange/10 text-flux-orange border-flux-orange/20"
													: "bg-flux-text-muted/10 text-flux-text-muted border-flux-card-border"
											}
										>
											{s.has_will ? "Yes" : "No"}
										</Badge>
									</TableCell>

									<TableCell className="text-flux-text-muted text-sm py-4">
										{s.connected_at
											? new Date(s.connected_at).toLocaleString()
											: "—"}
									</TableCell>
								</TableRow>
							))}

							{paginated.length === 0 && (
								<TableRow className="hover:bg-transparent">
									<TableCell
										colSpan={7}
										className="text-center text-flux-text-muted py-12"
									>
										No active connections.
									</TableCell>
								</TableRow>
							)}
						</TableBody>
					</Table>

					<TablePagination
						page={page}
						limit={limit}
						totalPages={totalPages}
						totalItems={filtered.length}
						setPage={setPage}
						setLimit={setLimit}
						itemLabel="connections"
					/>
				</CardContent>
			</Card>
		</div>
	);
};

export default ConnectionsClient;
