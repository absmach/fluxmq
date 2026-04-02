"use client";

import {
	BarChart3,
	Clock,
	Database,
	Download,
	Plug,
	Rss,
	Share2,
	SquareArrowRightEnter,
	SquareArrowRightExit,
	Unplug,
	Upload,
} from "lucide-react";
import { useEffect, useRef, useState } from "react";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Card, CardContent } from "@/components/ui/card";
import { formatBytes, formatCount, formatUptime } from "@/lib/format";
import { getBrokerOverview } from "@/lib/services/broker";
import type { BrokerStatus, NodeInfo } from "@/lib/types";
import { ClusterNodesTable } from "./cluster-nodes-table";
import { type ChartPoint, MetricCharts } from "./metric-charts";

const POLL_MS = 5_000;
const POLL_S = POLL_MS / 1000;
const MAX_POINTS = 40;

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

interface NodePillProps {
	label: string;
	active: boolean;
	isLeader?: boolean;
	onClick: () => void;
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
	const [error, setError] = useState<string | null>(null);

	useEffect(() => {
		let cancelled = false;

		async function poll() {
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
				): ChartPoint | null {
					const p = prev[key];
					prev[key] = { msgsRx, msgsTx, bytesRx, bytesTx };
					if (!p) return null;
					return {
						time: ts,
						sessions,
						msgsIn: Math.max(0, (msgsRx - p.msgsRx) / POLL_S),
						msgsOut: Math.max(0, (msgsTx - p.msgsTx) / POLL_S),
						bytesIn: Math.max(0, (bytesRx - p.bytesRx) / POLL_S),
						bytesOut: Math.max(0, (bytesTx - p.bytesTx) / POLL_S),
					};
				}

				const clusterPoint = makePoint(
					status.sessions,
					status.messages_received,
					status.messages_sent,
					status.bytes_received,
					status.bytes_sent,
					"",
				);
				if (clusterPoint) {
					histories[""] = [...(histories[""] ?? []), clusterPoint].slice(-MAX_POINTS);
				}

				for (const node of fetchedNodes) {
					const nodePoint = makePoint(
						node.sessions as number,
						node.messages_received as number,
						node.messages_sent as number,
						node.bytes_received as number,
						node.bytes_sent as number,
						node.node_id,
					);
					if (nodePoint) {
						histories[node.node_id] = [...(histories[node.node_id] ?? []), nodePoint].slice(-MAX_POINTS);
					}
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
	}, []);

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

			<MetricCharts
				displayHistory={displayHistory}
				loaded={loaded}
				error={error}
				scopeLabel={scopeLabel}
				selectedNodeId={selectedNodeId}
				pollSeconds={POLL_S}
				trafficSummary={trafficSummary}
				bandwidthSummary={bandwidthSummary}
				sessionsSummary={sessionsSummary}
			/>

			<ClusterNodesTable
				nodes={nodes}
				selectedNodeId={selectedNodeId}
				onSelectNode={setSelectedNodeId}
			/>
		</div>
	);
}
