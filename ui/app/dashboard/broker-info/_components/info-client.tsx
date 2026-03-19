"use client";

import { useEffect, useState } from "react";
import { Badge } from "@/components/ui/badge";
import { Card, CardContent } from "@/components/ui/card";
import type { BrokerStatus } from "@/lib/api";
import { formatBytes, formatCount, formatUptime } from "@/lib/api";
import { getBrokerOverview } from "@/lib/services/broker";

interface InfoRow {
	label: string;
	value: string | React.ReactNode;
}

function InfoSection({ title, rows }: { title: string; rows: InfoRow[] }) {
	return (
		<Card className="border-flux-card-border bg-flux-card overflow-hidden">
			<div className="px-6 py-3 border-b border-flux-card-border bg-flux-hover">
				<h2 className="text-sm font-semibold text-flux-text-muted uppercase tracking-wide">
					{title}
				</h2>
			</div>
			<CardContent className="p-0">
				<div className="divide-y divide-flux-card-border">
					{rows.map((row) => (
						<div
							key={row.label}
							className="px-6 py-3 flex items-center justify-between gap-4"
						>
							<p className="text-flux-text-muted text-sm">{row.label}</p>
							<span className="text-flux-text text-sm font-mono text-right">
								{row.value}
							</span>
						</div>
					))}
				</div>
			</CardContent>
		</Card>
	);
}

const InfoClient = () => {
	const [status, setStatus] = useState<BrokerStatus | null>(null);
	const [loading, setLoading] = useState(true);

	useEffect(() => {
		getBrokerOverview()
			.then(({ status }) => setStatus(status))
			.catch(console.error)
			.finally(() => setLoading(false));
	}, []);

	const roleBadge = status ? (
		status.is_leader ? (
			<Badge
				variant="outline"
				className="text-xs bg-flux-blue/10 text-flux-blue border-flux-blue/20"
			>
				Leader
			</Badge>
		) : (
			<Badge
				variant="outline"
				className="text-xs bg-flux-text-muted/10 text-flux-text-muted border-flux-card-border"
			>
				Follower
			</Badge>
		)
	) : (
		"—"
	);

	const modeBadge = status ? (
		status.cluster_mode ? (
			<Badge
				variant="outline"
				className="text-xs bg-flux-purple/10 text-flux-purple border-flux-purple/20"
			>
				Cluster
			</Badge>
		) : (
			<Badge
				variant="outline"
				className="text-xs bg-flux-teal/10 text-flux-teal border-flux-teal/20"
			>
				Single Node
			</Badge>
		)
	) : (
		"—"
	);

	const v = (n: number | undefined) => (n !== undefined ? formatCount(n) : "—");
	const b = (n: number | undefined) => (n !== undefined ? formatBytes(n) : "—");

	const sections = status
		? [
				{
					title: "Identity",
					rows: [
						{ label: "Node ID", value: status.node_id },
						{ label: "Role", value: roleBadge },
						{ label: "Mode", value: modeBadge },
						{ label: "Uptime", value: formatUptime(status.uptime_seconds) },
						...(status.cluster_mode
							? [{ label: "Cluster Nodes", value: v(status.node_count) }]
							: []),
					],
				},
				{
					title: "Sessions",
					rows: [
						{ label: "Connected", value: v(status.sessions) },
						{ label: "Total (incl. offline)", value: v(status.sessions_total) },
						{ label: "Active Subscriptions", value: v(status.subscriptions) },
						{ label: "Retained Messages", value: v(status.retained_messages) },
					],
				},
				{
					title: "Connections",
					rows: [
						{ label: "Current", value: v(status.sessions) },
						{
							label: "Total Ever Connected",
							value: v(status.connections_total),
						},
						{
							label: "Disconnections",
							value: v(status.connections_disconnections),
						},
					],
				},
				{
					title: "Messages",
					rows: [
						{ label: "Received", value: v(status.messages_received) },
						{ label: "Sent", value: v(status.messages_sent) },
						{ label: "Publish Received", value: v(status.publish_received) },
						{ label: "Publish Sent", value: v(status.publish_sent) },
					],
				},
				{
					title: "Bandwidth",
					rows: [
						{ label: "Bytes In", value: b(status.bytes_received) },
						{ label: "Bytes Out", value: b(status.bytes_sent) },
					],
				},
				{
					title: "Errors",
					rows: [
						{ label: "Protocol Errors", value: v(status.protocol_errors) },
						{ label: "Auth Errors", value: v(status.auth_errors) },
						{ label: "Authz Errors", value: v(status.authz_errors) },
						{ label: "Packet Errors", value: v(status.packet_errors) },
					],
				},
			]
		: [];

	return (
		<div className="p-4 sm:p-6 lg:p-8 space-y-6">
			<div>
				<h1 className="text-3xl font-bold text-flux-text mb-1">Broker Info</h1>
				<p className="text-flux-text-muted">
					Runtime state of the connected broker node
				</p>
			</div>

			{loading && <div className="text-flux-text-muted text-sm">Loading…</div>}

			{!loading && !status && (
				<div className="rounded-lg border border-flux-red/30 bg-flux-red/10 px-4 py-3 text-sm text-flux-red">
					Could not reach the broker. Check that{" "}
					<span className="font-mono">FLUXMQ_API_URL</span> is set correctly.
				</div>
			)}

			{!loading && status && (
				<div className="space-y-4">
					{sections.map((s) => (
						<InfoSection key={s.title} title={s.title} rows={s.rows} />
					))}
				</div>
			)}
		</div>
	);
};

export default InfoClient;
