"use client";

import { Hourglass, Search, Trash2, Users, Wifi, WifiOff } from "lucide-react";
import { useCallback, useEffect, useId, useState } from "react";
import { SessionDetailDialog } from "@/components/session-detail-dialog";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
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
import type { NodeInfo, SessionInfo } from "@/lib/api";
import { getBrokerOverview } from "@/lib/services/broker";
import {
	getSession,
	getSessions,
	type SessionsParams,
} from "@/lib/services/sessions";
import {
	formatProtocolLabel,
	PROTOCOL_BADGE_CLASSES,
	resolveSessionProtocol,
} from "@/lib/session-protocol";

type Filter = "all" | "connected" | "disconnected";
type Scope = "cluster" | "node";

function StatCard({
	label,
	value,
	icon: Icon,
	color,
}: {
	label: string;
	value: number;
	icon: React.ElementType;
	color: string;
}) {
	return (
		<Card className="border-flux-card-border bg-flux-card">
			<CardContent className="p-5 flex items-center gap-4">
				<div className={`p-3 rounded-lg ${color}`}>
					<Icon size={20} />
				</div>
				<div>
					<p className="text-2xl font-bold text-flux-text">{value}</p>
					<p className="text-sm text-flux-text-muted">{label}</p>
				</div>
			</CardContent>
		</Card>
	);
}

const SessionsClient = () => {
	const [sessions, setSessions] = useState<SessionInfo[]>([]);
	const [nodes, setNodes] = useState<NodeInfo[]>([]);
	const [filter, setFilter] = useState<Filter>("all");
	const [scope, setScope] = useState<Scope>("cluster");
	const [nodeId, setNodeId] = useState<string>("");
	const [search, setSearch] = useState("");
	const [selected, setSelected] = useState<Set<string>>(new Set());
	const [detailSession, setDetailSession] = useState<SessionInfo | null>(null);
	const [page, setPage] = useState(1);
	const [limit, setLimit] = useState(10);
	const searchInputId = useId();
	const nodeSelectId = useId();

	useEffect(() => {
		getBrokerOverview()
			.then(({ status, nodes: fetchedNodes }) => {
				setNodes(fetchedNodes);
				setNodeId(
					(prev) => prev || fetchedNodes[0]?.node_id || status.node_id || "",
				);
			})
			.catch(console.error);
	}, []);

	useEffect(() => {
		const params: SessionsParams = {};
		if (filter !== "all") params.state = filter;
		if (search) params.prefix = search;
		params.scope = scope;
		if (scope === "node" && nodeId) params.nodeId = nodeId;
		getSessions(params)
			.then(({ sessions }) => setSessions(sessions))
			.catch(console.error);
	}, [filter, search, scope, nodeId]);

	const totalQueued = sessions.reduce(
		(s, sess) => s + sess.offline_queue_depth,
		0,
	);
	const connectedCount = sessions.filter((s) => s.connected).length;
	const disconnectedCount = sessions.length - connectedCount;

	// Filtering is handled server-side
	const filtered = sessions;

	const totalPages = Math.max(1, Math.ceil(filtered.length / limit));
	const paginated = filtered.slice((page - 1) * limit, page * limit);

	const toggleSelect = useCallback((id: string) => {
		setSelected((prev) => {
			const next = new Set(prev);
			next.has(id) ? next.delete(id) : next.add(id);
			return next;
		});
	}, []);

	const openSessionDetail = useCallback(async (session: SessionInfo) => {
		try {
			const detail = await getSession(session.client_id, session.node_id);
			setDetailSession({
				...detail,
				node_id: detail.node_id ?? session.node_id,
			});
		} catch {
			// Fallback to list row when detail endpoint is temporarily unavailable.
			setDetailSession(session);
		}
	}, []);

	const toggleAll = () => {
		if (selected.size === filtered.length) {
			setSelected(new Set());
		} else {
			setSelected(new Set(filtered.map((s) => s.client_id)));
		}
	};

	const filterButtons: { label: string; value: Filter }[] = [
		{ label: "All Sessions", value: "all" },
		{ label: "Connected Only", value: "connected" },
		{ label: "Disconnected Only", value: "disconnected" },
	];

	return (
		<div className="p-4 sm:p-6 lg:p-8 space-y-6">
			{/* Header */}
			<div>
				<h1 className="text-3xl font-bold text-flux-text mb-1">Sessions</h1>
				<p className="text-flux-text-muted">
					Monitor and manage active client sessions across the broker
				</p>
			</div>

			{/* Stat Cards */}
			<div className="grid grid-cols-2 lg:grid-cols-4 gap-4">
				<StatCard
					label="Total Sessions"
					value={sessions.length}
					icon={Users}
					color="bg-flux-blue/10 text-flux-blue"
				/>
				<StatCard
					label="Connected"
					value={connectedCount}
					icon={Wifi}
					color="bg-flux-green/10 text-flux-green"
				/>
				<StatCard
					label="Disconnected"
					value={disconnectedCount}
					icon={WifiOff}
					color="bg-flux-red/10 text-flux-red"
				/>
				<StatCard
					label="Queued Messages"
					value={totalQueued}
					icon={Hourglass}
					color="bg-flux-orange/10 text-flux-orange"
				/>
			</div>

			{/* Table Card */}
			<Card className="border-flux-card-border bg-flux-card">
				<CardContent className="p-6">
					{/* Toolbar */}
					<div className="flex flex-wrap items-center gap-3 mb-5">
						<div className="flex items-center gap-1 p-1 rounded-lg bg-flux-bg border border-flux-card-border">
							{filterButtons.map(({ label, value }) => (
								<button
									type="button"
									key={value}
									aria-pressed={filter === value}
									onClick={() => {
										setFilter(value);
										setPage(1);
									}}
									className={`px-3 py-2 min-h-11 rounded-md text-sm font-medium transition-colors ${
										filter === value
											? "bg-flux-blue text-white"
											: "text-flux-text-muted hover:text-flux-text hover:bg-flux-hover"
									}`}
								>
									{label}
								</button>
							))}
						</div>

						<div className="flex items-center gap-1 p-1 rounded-lg bg-flux-bg border border-flux-card-border">
							<button
								type="button"
								aria-pressed={scope === "cluster"}
								onClick={() => {
									setScope("cluster");
									setPage(1);
								}}
								className={`px-3 py-2 min-h-11 rounded-md text-sm font-medium transition-colors ${
									scope === "cluster"
										? "bg-flux-blue text-white"
										: "text-flux-text-muted hover:text-flux-text hover:bg-flux-hover"
								}`}
							>
								Cluster-wide
							</button>
							<button
								type="button"
								aria-pressed={scope === "node"}
								onClick={() => {
									setScope("node");
									setPage(1);
								}}
								className={`px-3 py-2 min-h-11 rounded-md text-sm font-medium transition-colors ${
									scope === "node"
										? "bg-flux-blue text-white"
										: "text-flux-text-muted hover:text-flux-text hover:bg-flux-hover"
								}`}
							>
								Per Node
							</button>
						</div>

						{scope === "node" && (
							<div className="flex items-center gap-2 min-w-[180px]">
								<label
									htmlFor={nodeSelectId}
									className="text-xs font-medium uppercase tracking-wide text-flux-text-muted"
								>
									Node
								</label>
								<select
									id={nodeSelectId}
									aria-label="Select node scope"
									value={nodeId}
									onChange={(e) => {
										setNodeId(e.target.value);
										setPage(1);
									}}
									className="h-11 rounded-md border border-flux-card-border bg-flux-bg px-3 text-sm text-flux-text focus:outline-none focus:ring-2 focus:ring-flux-blue/40"
								>
									{nodeId === "" && <option value="">Current node</option>}
									{nodes.map((node) => (
										<option key={node.node_id} value={node.node_id}>
											{node.node_id}
										</option>
									))}
								</select>
							</div>
						)}

						<div className="relative flex-1 w-full sm:w-auto min-w-[180px] max-w-xs sm:ml-auto">
							<label htmlFor={searchInputId} className="sr-only">
								Search sessions by client ID
							</label>
							<Search
								className="absolute left-3 top-1/2 -translate-y-1/2 text-flux-text-muted"
								size={16}
							/>
							<Input
								id={searchInputId}
								type="text"
								placeholder="Search by client ID..."
								value={search}
								onChange={(e) => {
									setSearch(e.target.value);
									setPage(1);
								}}
								className="pl-9 bg-flux-bg border-flux-card-border text-flux-text placeholder:text-flux-text-muted focus-visible:ring-flux-blue"
							/>
						</div>

						<Button
							variant="outline"
							size="sm"
							disabled={selected.size === 0}
							className="border-flux-card-border text-flux-text-muted hover:text-flux-red hover:border-flux-red/40 disabled:opacity-40"
						>
							<Trash2 size={14} className="mr-1.5" />
							Remove Selected ({selected.size})
						</Button>
					</div>

					{/* Table */}
					<div className="overflow-x-auto">
						<Table>
							<TableHeader>
								<TableRow className="border-flux-card-border hover:bg-transparent">
									<TableHead className="w-8">
										<input
											type="checkbox"
											aria-label="Select all sessions"
											checked={
												filtered.length > 0 && selected.size === filtered.length
											}
											onChange={toggleAll}
											className="accent-flux-blue cursor-pointer"
										/>
									</TableHead>
									<TableHead>Client ID</TableHead>
									<TableHead className="hidden md:table-cell">Node</TableHead>
									<TableHead className="hidden md:table-cell">
										Protocol
									</TableHead>
									<TableHead>Status</TableHead>
									<TableHead className="text-right hidden md:table-cell">
										Subscriptions
									</TableHead>
									<TableHead className="text-right hidden md:table-cell">
										Inflight
									</TableHead>
									<TableHead className="text-right">Queued</TableHead>
									<TableHead className="hidden md:table-cell">
										Clean Start
									</TableHead>
									<TableHead className="text-right">Actions</TableHead>
								</TableRow>
							</TableHeader>
							<TableBody>
								{paginated.map((session: SessionInfo) => {
									const protocol = resolveSessionProtocol(session);
									return (
										<TableRow
											key={session.client_id}
											className={`border-flux-card-border hover:bg-flux-hover ${
												selected.has(session.client_id) ? "bg-flux-blue/5" : ""
											}`}
										>
											<TableCell>
												<input
													type="checkbox"
													aria-label={`Select session ${session.client_id}`}
													checked={selected.has(session.client_id)}
													onChange={() => toggleSelect(session.client_id)}
													className="accent-flux-blue cursor-pointer"
												/>
											</TableCell>

											<TableCell className="font-medium text-sm text-flux-text font-mono py-4">
												<div>{session.client_id}</div>
												<div className="md:hidden mt-1 text-xs text-flux-text-muted font-sans">
													{session.node_id ?? "No node"} ·{" "}
													{formatProtocolLabel(protocol)}
												</div>
											</TableCell>

											<TableCell className="text-sm text-flux-text-muted font-mono py-4 hidden md:table-cell">
												{session.node_id ?? "—"}
											</TableCell>

											<TableCell className="hidden md:table-cell">
												<Badge
													variant="outline"
													className={`text-xs ${PROTOCOL_BADGE_CLASSES[protocol]}`}
												>
													{formatProtocolLabel(protocol)}
												</Badge>
											</TableCell>

											<TableCell>
												<Badge
													variant="outline"
													className={`inline-flex items-center gap-1.5 ${
														session.connected
															? "bg-flux-green/10 text-flux-green border-flux-green/20"
															: "bg-flux-red/10 text-flux-red border-flux-red/20"
													}`}
												>
													<span
														className={`w-1.5 h-1.5 rounded-full ${session.connected ? "bg-flux-green animate-pulse" : "bg-flux-red"}`}
													/>
													{session.connected ? "Connected" : "Disconnected"}
												</Badge>
											</TableCell>

											<TableCell className="text-right text-sm text-flux-text tabular-nums py-4 hidden md:table-cell">
												{session.subscription_count}
											</TableCell>

											<TableCell className="text-right text-sm tabular-nums py-4 hidden md:table-cell">
												{session.inflight_count > 0 ? (
													<span className="text-flux-orange font-medium">
														{session.inflight_count}
													</span>
												) : (
													<span className="text-flux-text-muted">0</span>
												)}
											</TableCell>

											<TableCell className="text-right text-sm tabular-nums py-4">
												{session.offline_queue_depth > 0 ? (
													<span className="text-flux-orange font-medium">
														{session.offline_queue_depth}
													</span>
												) : (
													<span className="text-flux-text-muted">0</span>
												)}
											</TableCell>

											<TableCell className="hidden md:table-cell">
												<Badge
													variant="outline"
													className={
														session.clean_start
															? "bg-flux-green/10 text-flux-green border-flux-green/20"
															: "bg-flux-text-muted/10 text-flux-text-muted border-flux-card-border"
													}
												>
													{session.clean_start ? "Yes" : "No"}
												</Badge>
											</TableCell>

											<TableCell className="text-right py-4">
												<Button
													variant="ghost"
													size="sm"
													onClick={() => {
														void openSessionDetail(session);
													}}
													className="text-xs text-flux-text-muted hover:text-flux-blue hover:bg-flux-blue/10"
												>
													View Details
												</Button>
											</TableCell>
										</TableRow>
									);
								})}

								{paginated.length === 0 && (
									<TableRow className="hover:bg-transparent">
										<TableCell
											colSpan={10}
											className="text-center text-flux-text-muted py-12"
										>
											No sessions match the current filter.
										</TableCell>
									</TableRow>
								)}
							</TableBody>
						</Table>
					</div>

					<TablePagination
						page={page}
						limit={limit}
						totalPages={totalPages}
						totalItems={filtered.length}
						setPage={setPage}
						setLimit={setLimit}
						itemLabel="sessions"
					/>
				</CardContent>
			</Card>

			<SessionDetailDialog
				session={detailSession}
				onClose={() => setDetailSession(null)}
			/>
		</div>
	);
};

export default SessionsClient;
