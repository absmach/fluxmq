"use client";

import {
	Clock,
	Hourglass,
	ListFilter,
	Radio,
	Search,
	Server,
	Trash2,
	Users,
	Wifi,
	WifiOff,
} from "lucide-react";
import { useCallback, useEffect, useState } from "react";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Card, CardContent } from "@/components/ui/card";
import {
	Dialog,
	DialogContent,
	DialogDescription,
	DialogHeader,
	DialogTitle,
} from "@/components/ui/dialog";
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
import { getSessions, type SessionsParams } from "@/lib/services/sessions";

type Filter = "all" | "connected" | "disconnected";

// Backend protocol keys → display labels
function formatProtocol(p: string): string {
	switch (p) {
		case "mqtt5":
			return "MQTT 5.0";
		case "mqtt3.1.1":
			return "MQTT 3.1.1";
		case "mqtt3.1":
			return "MQTT 3.1";
		default:
			return p;
	}
}

const PROTOCOL_COLORS: Record<string, string> = {
	mqtt5: "bg-flux-blue/10 text-flux-blue border-flux-blue/20",
	"mqtt3.1.1": "bg-flux-teal/10 text-flux-teal border-flux-teal/20",
	"mqtt3.1": "bg-flux-teal/10 text-flux-teal border-flux-teal/20",
};

function fmt(date: string) {
	return new Date(date).toLocaleString(undefined, {
		dateStyle: "medium",
		timeStyle: "medium",
	});
}

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

function SectionLabel({
	icon: Icon,
	label,
}: {
	icon: React.ElementType;
	label: string;
}) {
	return (
		<div className="flex items-center gap-2 mb-2 mt-5 first:mt-0">
			<Icon size={13} className="text-flux-text-muted" />
			<span className="text-xs font-semibold uppercase tracking-wider text-flux-text-muted">
				{label}
			</span>
		</div>
	);
}

function DetailRow({
	label,
	children,
}: {
	label: string;
	children: React.ReactNode;
}) {
	return (
		<div className="flex items-center justify-between gap-4 py-2 border-b border-flux-card-border last:border-0">
			<span className="text-sm text-flux-text-muted shrink-0">{label}</span>
			<span className="text-sm text-flux-text text-right">{children}</span>
		</div>
	);
}

function QueueStat({
	label,
	value,
	highlight,
}: {
	label: string;
	value: number;
	highlight?: boolean;
}) {
	return (
		<div className="rounded-lg border border-flux-card-border bg-flux-bg p-3 flex flex-col gap-1">
			<p className="text-xs text-flux-text-muted">{label}</p>
			<p
				className={`text-xl font-bold tabular-nums ${highlight && value > 0 ? "text-flux-orange" : "text-flux-text"}`}
			>
				{value.toLocaleString()}
			</p>
		</div>
	);
}

function SessionDetailDialog({
	session,
	onClose,
}: {
	session: SessionInfo | null;
	onClose: () => void;
}) {
	return (
		<Dialog
			open={!!session}
			onOpenChange={(open) => {
				if (!open) onClose();
			}}
		>
			<DialogContent className="bg-flux-card border-flux-card-border text-flux-text max-w-md p-0 overflow-hidden">
				{session && (
					<>
						{/* Coloured header strip */}
						<div
							className={`px-6 pt-6 pb-5 border-b border-flux-card-border ${
								session.connected ? "bg-flux-green/5" : "bg-flux-red/5"
							}`}
						>
							<DialogHeader>
								<div className="flex items-start justify-between gap-3">
									<div className="space-y-1.5">
										<DialogTitle className="text-flux-text font-mono text-lg leading-tight break-all">
											{session.client_id}
										</DialogTitle>
										<div className="flex items-center gap-2 flex-wrap">
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
											<Badge
												variant="outline"
												className={`text-xs ${PROTOCOL_COLORS[session.protocol] ?? "bg-flux-blue/10 text-flux-blue border-flux-blue/20"}`}
											>
												{formatProtocol(session.protocol)}
											</Badge>
										</div>
									</div>
								</div>
								<DialogDescription className="sr-only">
									Session details for {session.client_id}
								</DialogDescription>
							</DialogHeader>
						</div>

						{/* Body */}
						<div className="px-6 pb-6 overflow-y-auto max-h-[60vh]">
							{/* Queue stats */}
							<SectionLabel icon={Radio} label="Queue" />
							<div className="grid grid-cols-1 sm:grid-cols-3 gap-3 mb-1">
								<QueueStat
									label="Inflight"
									value={session.inflight_count}
									highlight
								/>
								<QueueStat
									label="Offline Queue"
									value={session.offline_queue_depth}
									highlight
								/>
								<QueueStat
									label="Subscriptions"
									value={session.subscription_count}
								/>
							</div>

							{/* Connection */}
							<SectionLabel icon={Server} label="Connection" />
							{session.connected_at && (
								<DetailRow label="Connected At">
									{fmt(session.connected_at)}
								</DetailRow>
							)}
							{session.disconnected_at && (
								<DetailRow label="Disconnected At">
									{fmt(session.disconnected_at)}
								</DetailRow>
							)}

							{/* MQTT Settings */}
							<SectionLabel icon={ListFilter} label="Settings" />
							<DetailRow label="Clean Start">
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
							</DetailRow>
							{session.expiry_interval > 0 && (
								<DetailRow label="Expiry Interval">
									<span className="font-mono">{session.expiry_interval}s</span>
								</DetailRow>
							)}
							{session.receive_maximum > 0 &&
								session.receive_maximum < 65535 && (
									<DetailRow label="Receive Maximum">
										<span className="font-mono">{session.receive_maximum}</span>
									</DetailRow>
								)}
							{session.max_packet_size > 0 && (
								<DetailRow label="Max Packet Size">
									<span className="font-mono">
										{(session.max_packet_size / 1024).toFixed(0)} KB
									</span>
								</DetailRow>
							)}
							<DetailRow label="Has Will">
								<Badge
									variant="outline"
									className={
										session.has_will
											? "bg-flux-orange/10 text-flux-orange border-flux-orange/20"
											: "bg-flux-text-muted/10 text-flux-text-muted border-flux-card-border"
									}
								>
									{session.has_will ? "Yes" : "No"}
								</Badge>
							</DetailRow>
							{session.topic_alias_max > 0 && (
								<DetailRow label="Topic Alias Max">
									<span className="font-mono">{session.topic_alias_max}</span>
								</DetailRow>
							)}
							<DetailRow label="Request/Response">
								<Badge
									variant="outline"
									className={
										session.request_response
											? "bg-flux-green/10 text-flux-green border-flux-green/20"
											: "bg-flux-text-muted/10 text-flux-text-muted border-flux-card-border"
									}
								>
									{session.request_response ? "Enabled" : "Disabled"}
								</Badge>
							</DetailRow>
							<DetailRow label="Problem Info">
								<Badge
									variant="outline"
									className={
										session.request_problem
											? "bg-flux-green/10 text-flux-green border-flux-green/20"
											: "bg-flux-text-muted/10 text-flux-text-muted border-flux-card-border"
									}
								>
									{session.request_problem ? "Enabled" : "Disabled"}
								</Badge>
							</DetailRow>

							{/* Subscription list */}
							{session.subscriptions && session.subscriptions.length > 0 && (
								<>
									<SectionLabel icon={Clock} label="Subscriptions" />
									<div className="space-y-1">
										{session.subscriptions.map((sub) => (
											<div
												key={sub.filter}
												className="flex items-center justify-between gap-2 py-1.5 border-b border-flux-card-border last:border-0"
											>
												<span className="text-xs font-mono text-flux-text truncate">
													{sub.filter}
												</span>
												<Badge
													variant="outline"
													className="text-xs shrink-0 bg-flux-blue/10 text-flux-blue border-flux-blue/20"
												>
													QoS {sub.qos}
												</Badge>
											</div>
										))}
									</div>
								</>
							)}
						</div>

						{/* Footer */}
						{session.connected && (
							<div className="px-6 py-4 border-t border-flux-card-border flex justify-end">
								<Button
									variant="outline"
									size="sm"
									className="border-flux-red/40 text-flux-red hover:bg-flux-red/10"
									onClick={onClose}
								>
									Disconnect Session
								</Button>
							</div>
						)}
					</>
				)}
			</DialogContent>
		</Dialog>
	);
}

const SessionsClient = () => {
	const [sessions, setSessions] = useState<SessionInfo[]>([]);
	const [filter, setFilter] = useState<Filter>("all");
	const [search, setSearch] = useState("");
	const [selected, setSelected] = useState<Set<string>>(new Set());
	const [detailSession, setDetailSession] = useState<SessionInfo | null>(null);
	const [page, setPage] = useState(1);
	const [limit, setLimit] = useState(10);

	useEffect(() => {
		const params: SessionsParams = {};
		if (filter !== "all") params.state = filter;
		if (search) params.prefix = search;
		getSessions(params)
			.then(({ sessions }) => setSessions(sessions))
			.catch(console.error);
	}, [filter, search]);

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
									onClick={() => {
										setFilter(value);
										setPage(1);
									}}
									className={`px-3 py-1.5 rounded-md text-sm font-medium transition-colors ${
										filter === value
											? "bg-flux-blue text-white"
											: "text-flux-text-muted hover:text-flux-text hover:bg-flux-hover"
									}`}
								>
									{label}
								</button>
							))}
						</div>

						<div className="relative flex-1 w-full sm:w-auto min-w-[180px] max-w-xs sm:ml-auto">
							<Search
								className="absolute left-3 top-1/2 -translate-y-1/2 text-flux-text-muted"
								size={16}
							/>
							<Input
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
											checked={
												filtered.length > 0 && selected.size === filtered.length
											}
											onChange={toggleAll}
											className="accent-flux-blue cursor-pointer"
										/>
									</TableHead>
									<TableHead>Client ID</TableHead>
									<TableHead>Protocol</TableHead>
									<TableHead>Status</TableHead>
									<TableHead className="text-right">Subscriptions</TableHead>
									<TableHead className="text-right">Inflight</TableHead>
									<TableHead className="text-right">Queued</TableHead>
									<TableHead>Clean Start</TableHead>
									<TableHead className="text-right">Actions</TableHead>
								</TableRow>
							</TableHeader>
							<TableBody>
								{paginated.map((session: SessionInfo) => (
									<TableRow
										key={session.client_id}
										className={`border-flux-card-border hover:bg-flux-hover ${
											selected.has(session.client_id) ? "bg-flux-blue/5" : ""
										}`}
									>
										<TableCell>
											<input
												type="checkbox"
												checked={selected.has(session.client_id)}
												onChange={() => toggleSelect(session.client_id)}
												className="accent-flux-blue cursor-pointer"
											/>
										</TableCell>

										<TableCell className="font-medium text-sm text-flux-text font-mono py-4">
											{session.client_id}
										</TableCell>

										<TableCell>
											<Badge
												variant="outline"
												className={`text-xs ${PROTOCOL_COLORS[session.protocol] ?? "bg-flux-blue/10 text-flux-blue border-flux-blue/20"}`}
											>
												{formatProtocol(session.protocol)}
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

										<TableCell className="text-right text-sm text-flux-text tabular-nums py-4">
											{session.subscription_count}
										</TableCell>

										<TableCell className="text-right text-sm tabular-nums py-4">
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

										<TableCell>
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
												onClick={() => setDetailSession(session)}
												className="text-xs text-flux-text-muted hover:text-flux-blue hover:bg-flux-blue/10"
											>
												View Details
											</Button>
										</TableCell>
									</TableRow>
								))}

								{paginated.length === 0 && (
									<TableRow className="hover:bg-transparent">
										<TableCell
											colSpan={9}
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
