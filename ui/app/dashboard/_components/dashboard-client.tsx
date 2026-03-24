"use client";

import {
	AlertTriangle,
	BarChart3,
	Clock,
	Database,
	Download,
	Pause,
	Play,
	Plug,
	Rss,
	Share2,
	SquareArrowRightEnter,
	SquareArrowRightExit,
	Unplug,
	Upload,
} from "lucide-react";
import { useEffect, useRef, useState } from "react";
import {
	CartesianGrid,
	Legend,
	Line,
	LineChart,
	ResponsiveContainer,
	Tooltip,
	XAxis,
	YAxis,
} from "recharts";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import {
	Table,
	TableBody,
	TableCell,
	TableHead,
	TableHeader,
	TableRow,
} from "@/components/ui/table";
import {
	type BrokerStatus,
	formatBytes,
	formatCount,
	formatUptime,
	type NodeInfo,
} from "@/lib/api";
import { getBrokerOverview } from "@/lib/services/broker";

const POLL_MS = 5_000;
const POLL_S = POLL_MS / 1000;
const MAX_POINTS = 40;

interface ChartPoint {
	time: string;
	sessions: number;
	msgsIn: number;
	msgsOut: number;
	bytesIn: number;
	bytesOut: number;
}

function now(): string {
	return new Date().toLocaleTimeString([], {
		hour: "2-digit",
		minute: "2-digit",
		second: "2-digit",
	});
}

function nodeToStatus(node: NodeInfo): BrokerStatus {
	return {
		node_id: node.node_id,
		is_leader: node.is_leader,
		cluster_mode: false,
		sessions: node.sessions ?? 0,
		sessions_total: 0,
		connections_total: 0,
		connections_disconnections: 0,
		messages_received: node.messages_received ?? 0,
		messages_sent: node.messages_sent ?? 0,
		publish_received: 0,
		publish_sent: 0,
		bytes_received: node.bytes_received ?? 0,
		bytes_sent: node.bytes_sent ?? 0,
		subscriptions: node.subscriptions ?? 0,
		retained_messages: 0,
		uptime_seconds: node.uptime_seconds,
		auth_errors: 0,
		authz_errors: 0,
		protocol_errors: 0,
		packet_errors: 0,
	};
}

export default function DashboardClient() {
	const [clusterStatus, setClusterStatus] = useState<BrokerStatus | null>(null);
	const [nodes, setNodes] = useState<NodeInfo[]>([]);
	const historiesRef = useRef<Record<string, ChartPoint[]>>({});
	const [loaded, setLoaded] = useState(false);
	const [, forceRender] = useState(0);
	const prevRef = useRef<
		Record<
			string,
			{ msgsRx: number; msgsTx: number; bytesRx: number; bytesTx: number }
		>
	>({});
	const [selectedNodeId, setSelectedNodeId] = useState<string | null>(null);
	const [liveUpdates, setLiveUpdates] = useState(true);
	const [error, setError] = useState<string | null>(null);

	useEffect(() => {
		let cancelled = false;

		async function poll() {
			if (!liveUpdates) return;
			try {
				if (cancelled) return;

				const { status, nodes: fetchedNodes } = await getBrokerOverview();
				if (cancelled) return;

				setClusterStatus(status);
				setNodes(fetchedNodes);
				setError(null);
				setLoaded(true);

				const ts = now();
				const histories = historiesRef.current;
				const prev = prevRef.current;
				function makePoint(
					sessions: number,
					msgsRx: number,
					msgsTx: number,
					bytesRx: number,
					bytesTx: number,
					key: string,
				): ChartPoint {
					const p = prev[key] ?? {
						msgsRx: 0,
						msgsTx: 0,
						bytesRx: 0,
						bytesTx: 0,
					};
					const point: ChartPoint = {
						time: ts,
						sessions,
						msgsIn: Math.max(0, (msgsRx - p.msgsRx) / POLL_S),
						msgsOut: Math.max(0, (msgsTx - p.msgsTx) / POLL_S),
						bytesIn: Math.max(0, (bytesRx - p.bytesRx) / POLL_S),
						bytesOut: Math.max(0, (bytesTx - p.bytesTx) / POLL_S),
					};
					prev[key] = { msgsRx, msgsTx, bytesRx, bytesTx };
					return point;
				}

				histories[""] = [
					...(histories[""] ?? []),
					makePoint(
						status.sessions,
						status.messages_received,
						status.messages_sent,
						status.bytes_received,
						status.bytes_sent,
						"",
					),
				].slice(-MAX_POINTS);

				for (const node of fetchedNodes) {
					histories[node.node_id] = [
						...(histories[node.node_id] ?? []),
						makePoint(
							node.sessions as number,
							node.messages_received as number,
							node.messages_sent as number,
							node.bytes_received as number,
							node.bytes_sent as number,
							node.node_id,
						),
					].slice(-MAX_POINTS);
				}

				forceRender((n) => n + 1);
			} catch (e) {
				if (cancelled) return;
				setError(e instanceof Error ? e.message : "Failed to reach broker");
			}
		}

		poll();
		const id = setInterval(poll, POLL_MS);
		return () => {
			cancelled = true;
			clearInterval(id);
		};
	}, [liveUpdates]);

	const displayStatus: BrokerStatus | null =
		clusterStatus === null
			? null
			: selectedNodeId === null
				? clusterStatus
				: nodeToStatus(
						nodes.find((n) => n.node_id === selectedNodeId) ?? nodes[0],
					);

	const historyKey = selectedNodeId ?? "";
	const displayHistory = historiesRef.current[historyKey] ?? [];

	const gridColor = "var(--flux-grid)";
	const axisColor = "var(--flux-text-muted)";
	const tooltipBg = "var(--flux-card)";
	const tooltipBdr = "var(--flux-card-border)";
	const tooltipTxt = "var(--flux-text)";

	const tooltipStyle = {
		backgroundColor: tooltipBg,
		border: `1px solid ${tooltipBdr}`,
		borderRadius: "8px",
		color: tooltipTxt,
		fontSize: 12,
	};

	const statCards = displayStatus
		? [
				{ label: "Active Connections", value: formatCount(displayStatus.sessions), icon: BarChart3 },
				{ label: "Subscriptions", value: formatCount(displayStatus.subscriptions), icon: Rss },
				{ label: "Messages Received", value: formatCount(displayStatus.messages_received), icon: SquareArrowRightEnter },
				{ label: "Messages Sent", value: formatCount(displayStatus.messages_sent), icon: SquareArrowRightExit },
				{ label: "Bytes In", value: formatBytes(displayStatus.bytes_received), icon: Download },
				{ label: "Bytes Out", value: formatBytes(displayStatus.bytes_sent), icon: Upload },
				{ label: "Retained Messages", value: formatCount(displayStatus.retained_messages), icon: Database },
				{ label: "Uptime", value: formatUptime(displayStatus.uptime_seconds), icon: Clock },
			]
		: [];

	const scopeLabel = selectedNodeId ?? "Cluster";
	const latestPoint = displayHistory.at(-1);
	const trafficSummary = latestPoint
		? `Latest ${scopeLabel} traffic: ${latestPoint.msgsIn.toFixed(1)} messages in per second and ${latestPoint.msgsOut.toFixed(1)} messages out per second.`
		: `No traffic points available yet for ${scopeLabel}.`;
	const bandwidthSummary = latestPoint
		? `Latest ${scopeLabel} bandwidth: ${formatBytes(latestPoint.bytesIn)} in per second and ${formatBytes(latestPoint.bytesOut)} out per second.`
		: `No bandwidth points available yet for ${scopeLabel}.`;
	const sessionsSummary = latestPoint
		? `Latest ${scopeLabel} active sessions: ${formatCount(latestPoint.sessions)}.`
		: `No active session trend points available yet for ${scopeLabel}.`;

	return (
		<div className="p-4 sm:p-6 lg:p-8 space-y-6">
			<div className="flex flex-wrap items-start justify-between gap-4">
				<div>
					<h1 className="text-3xl font-bold text-flux-text">Dashboard</h1>
					<p className="text-flux-text-muted mt-1 text-sm">
						{clusterStatus ? (
							clusterStatus.cluster_mode ? (
								<>
									Cluster ·{" "}
									<span className="font-medium text-flux-text">
										{nodes.length} nodes
									</span>{" "}
									· via{" "}
									<span className="font-medium text-flux-text">
										{clusterStatus.node_id}
									</span>
								</>
							) : (
								<>
									Single node ·{" "}
									<span className="font-medium text-flux-text">
										{clusterStatus.node_id}
									</span>
								</>
							)
						) : (
							"Connecting…"
						)}
					</p>
				</div>

				<div className="flex items-center gap-2 flex-wrap">
					<Button
						variant="outline"
						size="sm"
						onClick={() => setLiveUpdates((v) => !v)}
						aria-pressed={liveUpdates}
						aria-label={
							liveUpdates ? "Pause live updates" : "Resume live updates"
						}
						className={`flex items-center gap-1.5 text-xs min-h-10 ${
							liveUpdates
								? "border-flux-green/40 text-flux-green bg-flux-green/10 hover:bg-flux-green/20"
								: "border-flux-card-border text-flux-text-muted hover:bg-flux-hover"
						}`}
					>
						{liveUpdates ? (
							<>
								<Pause className="w-3 h-3" /> Live
							</>
						) : (
							<>
								<Play className="w-3 h-3" /> Paused
							</>
						)}
					</Button>

					<Badge
						variant="outline"
						className={`flex items-center gap-1.5 text-sm px-3 py-1.5 min-h-10 ${
							error
								? "text-flux-red border-flux-red/30 bg-flux-red/10"
								: "text-flux-green border-flux-green/30 bg-flux-green/10"
						}`}
					>
						{error ? (
							<Unplug className="w-4 h-4" />
						) : (
							<Plug className="w-4 h-4" />
						)}
						{error ? "Offline" : "Online"}
					</Badge>
				</div>
			</div>
			{error && (
				<div
					role="alert"
					className="rounded-lg border border-flux-red/30 bg-flux-red/10 px-4 py-3 text-sm text-flux-red"
				>
					⚠ {error}
				</div>
			)}
			<div className="flex items-center gap-2 overflow-x-auto pb-1">
				<Share2 className="w-4 h-4 text-flux-text-muted shrink-0" />
				<NodePill
					label="Cluster"
					active={selectedNodeId === null}
					onClick={() => setSelectedNodeId(null)}
				/>
				{nodes.map((node) => (
					<NodePill
						key={node.node_id}
						label={node.node_id}
						active={selectedNodeId === node.node_id}
						isLeader={node.is_leader}
						onClick={() => setSelectedNodeId(node.node_id)}
					/>
				))}
			</div>
			<div className="grid grid-cols-2 sm:grid-cols-4 gap-4">
				{statCards.map((card) => {
					const Icon = card.icon;
					return (
						<Card
							key={card.label}
							className="border-flux-card-border bg-flux-card"
						>
							<CardContent className="p-5">
								<div className="flex items-center gap-4">
									<Icon className="w-8 h-8 text-flux-blue dark:text-flux-orange shrink-0" />
									<div>
										<p className="text-2xl font-bold text-flux-text leading-none">
											{card.value}
										</p>
										<p className="text-flux-text-muted text-xs mt-1">
											{card.label}
										</p>
									</div>
								</div>
							</CardContent>
						</Card>
					);
				})}
			</div>
			<div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
				<Card className="border-flux-card-border bg-flux-card">
					<CardHeader className="pb-2">
						<CardTitle className="text-base text-flux-text">
							Message Traffic Trends
						</CardTitle>
						<p className="text-xs text-flux-text-muted">
							{scopeLabel} · msgs/s in &amp; out · polled every {POLL_S}s
						</p>
					</CardHeader>
					<CardContent>
						<div
							className="relative"
							role="img"
							aria-label={`Message traffic trend chart for ${scopeLabel}`}
						>
							{!loaded && <ChartLoadingSkeleton height={260} />}
							<ResponsiveContainer width="100%" height={260}>
								<LineChart
									data={displayHistory}
									margin={{ top: 4, right: 8, left: 0, bottom: 0 }}
								>
									<CartesianGrid strokeDasharray="3 3" stroke={gridColor} />
									<XAxis
										dataKey="time"
										stroke={axisColor}
										tick={{ fontSize: 10 }}
										interval="preserveStartEnd"
									/>
									<YAxis
										stroke={axisColor}
										tick={{ fontSize: 10 }}
										allowDecimals={false}
										unit="/s"
									/>
									<Tooltip
										contentStyle={tooltipStyle}
										formatter={(v) => [
											typeof v === "number" ? `${v.toFixed(1)}/s` : "",
										]}
									/>
									<Legend wrapperStyle={{ fontSize: 11 }} />
									<Line
										type="monotone"
										dataKey="msgsIn"
										name="In"
										stroke="var(--flux-green)"
										strokeWidth={2}
										dot={false}
										isAnimationActive={false}
									/>
									<Line
										type="monotone"
										dataKey="msgsOut"
										name="Out"
										stroke="var(--flux-blue)"
										strokeWidth={2}
										dot={false}
										isAnimationActive={false}
									/>
								</LineChart>
							</ResponsiveContainer>
							<p className="sr-only">{trafficSummary}</p>
							{error && <ChartErrorOverlay />}
						</div>
					</CardContent>
				</Card>

				<Card className="border-flux-card-border bg-flux-card">
					<CardHeader className="pb-2">
						<CardTitle className="text-base text-flux-text">
							Bandwidth
						</CardTitle>
						<p className="text-xs text-flux-text-muted">
							{scopeLabel} · bytes/s in &amp; out
						</p>
					</CardHeader>
					<CardContent>
						<div
							className="relative"
							role="img"
							aria-label={`Bandwidth trend chart for ${scopeLabel}`}
						>
							{!loaded && <ChartLoadingSkeleton height={260} />}
							<ResponsiveContainer width="100%" height={260}>
								<LineChart
									data={displayHistory}
									margin={{ top: 4, right: 8, left: 0, bottom: 0 }}
								>
									<CartesianGrid strokeDasharray="3 3" stroke={gridColor} />
									<XAxis
										dataKey="time"
										stroke={axisColor}
										tick={{ fontSize: 10 }}
										interval="preserveStartEnd"
									/>
									<YAxis
										stroke={axisColor}
										tick={{ fontSize: 10 }}
										allowDecimals={false}
										tickFormatter={(v: number) => formatBytes(v)}
									/>
									<Tooltip
										contentStyle={tooltipStyle}
										formatter={(v) => [
											typeof v === "number" ? `${formatBytes(v)}/s` : "",
										]}
									/>
									<Legend wrapperStyle={{ fontSize: 11 }} />
									<Line
										type="monotone"
										dataKey="bytesIn"
										name="In"
										stroke="var(--flux-orange)"
										strokeWidth={2}
										dot={false}
										isAnimationActive={false}
									/>
									<Line
										type="monotone"
										dataKey="bytesOut"
										name="Out"
										stroke="var(--flux-purple)"
										strokeWidth={2}
										dot={false}
										isAnimationActive={false}
									/>
								</LineChart>
							</ResponsiveContainer>
							<p className="sr-only">{bandwidthSummary}</p>
							{error && <ChartErrorOverlay />}
						</div>
					</CardContent>
				</Card>

				{selectedNodeId === null && (
					<Card className="border-flux-card-border bg-flux-card lg:col-span-2">
						<CardHeader className="pb-2">
							<CardTitle className="text-base text-flux-text">
								Active Connections
							</CardTitle>
							<p className="text-xs text-flux-text-muted">
								Cluster · connected clients over time
							</p>
						</CardHeader>
						<CardContent>
							<div
								className="relative"
								role="img"
								aria-label="Active connections trend chart for cluster"
							>
								{!loaded && <ChartLoadingSkeleton height={200} />}
								<ResponsiveContainer width="100%" height={200}>
									<LineChart
										data={displayHistory}
										margin={{ top: 4, right: 8, left: 0, bottom: 0 }}
									>
										<CartesianGrid strokeDasharray="3 3" stroke={gridColor} />
										<XAxis
											dataKey="time"
											stroke={axisColor}
											tick={{ fontSize: 10 }}
											interval="preserveStartEnd"
										/>
										<YAxis
											stroke={axisColor}
											tick={{ fontSize: 10 }}
											allowDecimals={false}
										/>
										<Tooltip contentStyle={tooltipStyle} />
										<Line
											type="monotone"
											dataKey="sessions"
											name="Connections"
											stroke="var(--flux-blue)"
											strokeWidth={2}
											dot={false}
											isAnimationActive={false}
										/>
									</LineChart>
								</ResponsiveContainer>
								<p className="sr-only">{sessionsSummary}</p>
								{error && <ChartErrorOverlay />}
							</div>
						</CardContent>
					</Card>
				)}
			</div>

			{nodes.length > 0 && (
				<Card className="border-flux-card-border bg-flux-card">
					<CardHeader className="pb-3">
						<div className="flex items-center justify-between">
							<div>
								<CardTitle className="text-base text-flux-text">
									Cluster Nodes
								</CardTitle>
								<p className="text-xs text-flux-text-muted mt-0.5">
									{nodes.length} node{nodes.length !== 1 ? "s" : ""} · click a
									row to inspect
								</p>
							</div>
						</div>
					</CardHeader>
					<CardContent className="p-0">
						<div className="overflow-x-auto">
							<Table>
								<TableHeader>
									<TableRow className="border-flux-card-border hover:bg-transparent">
										<TableHead className="pl-6">Node</TableHead>
										<TableHead>Address</TableHead>
										<TableHead className="text-right">Sessions</TableHead>
										<TableHead className="text-right">Subscriptions</TableHead>
										<TableHead className="text-right">Msgs In</TableHead>
										<TableHead className="text-right">Msgs Out</TableHead>
										<TableHead className="text-right">Bytes In</TableHead>
										<TableHead className="text-right pr-6">Uptime</TableHead>
									</TableRow>
								</TableHeader>
								<TableBody>
									{nodes.map((node) => {
										const isSelected = selectedNodeId === node.node_id;
										return (
											<TableRow
												key={node.node_id}
												className={`border-flux-card-border transition-colors ${
													isSelected
														? "bg-flux-blue/10 hover:bg-flux-blue/15"
														: "hover:bg-flux-hover"
												}`}
											>
												<TableCell className="pl-6 py-4">
													<button
														type="button"
														aria-pressed={isSelected}
														aria-label={`Inspect node ${node.node_id}`}
														onClick={() =>
															setSelectedNodeId(
																isSelected ? null : node.node_id,
															)
														}
														className="flex w-full items-center gap-2 rounded text-left focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-flux-blue"
													>
														<span className="inline-block w-2 h-2 rounded-full bg-flux-green shrink-0" />
														<span className="text-flux-text font-medium text-sm">
															{node.node_id}
														</span>
														{node.is_leader && (
															<Badge
																variant="outline"
																className="text-xs bg-flux-blue/10 text-flux-blue border-flux-blue/20"
															>
																Leader
															</Badge>
														)}
													</button>
												</TableCell>
												<TableCell className="text-flux-text-muted font-mono text-xs py-4">
													{node.addr}
												</TableCell>
												<TableCell className="text-flux-text text-sm text-right py-4">
													{node.sessions !== undefined ? (
														formatCount(node.sessions)
													) : (
														<span className="text-flux-text-muted">—</span>
													)}
												</TableCell>
												<TableCell className="text-flux-text text-sm text-right py-4">
													{node.subscriptions !== undefined ? (
														formatCount(node.subscriptions)
													) : (
														<span className="text-flux-text-muted">—</span>
													)}
												</TableCell>
												<TableCell className="text-flux-text text-sm text-right py-4">
													{node.messages_received !== undefined ? (
														formatCount(node.messages_received)
													) : (
														<span className="text-flux-text-muted">—</span>
													)}
												</TableCell>
												<TableCell className="text-flux-text text-sm text-right py-4">
													{node.messages_sent !== undefined ? (
														formatCount(node.messages_sent)
													) : (
														<span className="text-flux-text-muted">—</span>
													)}
												</TableCell>
												<TableCell className="text-flux-text text-sm text-right py-4">
													{node.bytes_received !== undefined ? (
														formatBytes(node.bytes_received)
													) : (
														<span className="text-flux-text-muted">—</span>
													)}
												</TableCell>
												<TableCell className="text-flux-text-muted text-sm text-right pr-6 py-4">
													{formatUptime(node.uptime_seconds)}
												</TableCell>
											</TableRow>
										);
									})}
								</TableBody>
							</Table>
						</div>
					</CardContent>
				</Card>
			)}
		</div>
	);
}

interface NodePillProps {
	label: string;
	active: boolean;
	isLeader?: boolean;
	onClick: () => void;
}

function ChartLoadingSkeleton({ height }: { height: number }) {
	return (
		<div
			className="absolute inset-0 z-10 flex items-center justify-center rounded-md bg-flux-card"
			style={{ height }}
		>
			<div className="flex flex-col items-center gap-2">
				<div className="h-1.5 w-24 animate-pulse rounded-full bg-flux-card-border" />
				<div className="h-1.5 w-16 animate-pulse rounded-full bg-flux-card-border" />
			</div>
		</div>
	);
}

function ChartErrorOverlay() {
	return (
		<div className="absolute inset-0 flex flex-col items-center justify-center gap-2 rounded-md backdrop-blur-[2px] bg-flux-bg/60">
			<AlertTriangle className="w-5 h-5 text-flux-red" />
			<p className="text-xs font-medium text-flux-text-muted">
				Data unavailable
			</p>
		</div>
	);
}

function NodePill({ label, active, isLeader, onClick }: NodePillProps) {
	return (
		<button
			type="button"
			onClick={onClick}
			className={`flex items-center gap-1.5 text-xs px-3 py-2 min-h-10 rounded-full border transition-colors whitespace-nowrap ${
				active
					? "bg-flux-blue text-white border-flux-blue"
					: "bg-flux-card text-flux-text-muted border-flux-card-border hover:bg-flux-hover hover:text-flux-text"
			}`}
		>
			{isLeader !== undefined && (
				<span
					className={`inline-block w-1.5 h-1.5 rounded-full ${active ? "bg-white/70" : "bg-flux-green"}`}
				/>
			)}
			{label}
			{isLeader && (
				<span
					className={`text-[10px] ${active ? "text-white/70" : "text-flux-blue"}`}
				>
					★
				</span>
			)}
		</button>
	);
}
