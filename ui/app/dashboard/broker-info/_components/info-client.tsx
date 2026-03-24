"use client";

import {
	AlertTriangle,
	Clock,
	Cpu,
	MapPin,
	Shield,
	ShieldAlert,
} from "lucide-react";
import { useEffect, useState } from "react";
import { Badge } from "@/components/ui/badge";
import { Card, CardContent } from "@/components/ui/card";
import type { BrokerStatus, NodeInfo } from "@/lib/api";
import { formatCount, formatUptime } from "@/lib/api";
import { getBrokerOverview } from "@/lib/services/broker";

function NodeCard({ node }: { node: NodeInfo }) {
	const isHealthy = node.healthy !== false;

	return (
		<Card className="border-flux-card-border bg-flux-card overflow-hidden">
			<CardContent className="p-5">
				<div className="flex items-start justify-between gap-3 mb-4">
					<div className="flex items-center gap-2.5">
						<span
							className={`inline-block w-2.5 h-2.5 rounded-full shrink-0 ${
								isHealthy ? "bg-flux-green" : "bg-flux-red"
							}`}
						/>
						<h3 className="text-sm font-semibold text-flux-text">
							{node.node_id}
						</h3>
					</div>
					<div className="flex items-center gap-1.5">
						{node.is_leader && (
							<Badge
								variant="outline"
								className="text-xs bg-flux-blue/10 text-flux-blue border-flux-blue/20"
							>
								Leader
							</Badge>
						)}
						<Badge
							variant="outline"
							className={`text-xs ${
								isHealthy
									? "bg-flux-green/10 text-flux-green border-flux-green/20"
									: "bg-flux-red/10 text-flux-red border-flux-red/20"
							}`}
						>
							{isHealthy ? "Healthy" : "Unhealthy"}
						</Badge>
					</div>
				</div>

				<div className="space-y-2.5">
					<div className="flex items-center justify-between">
						<span className="flex items-center gap-2 text-xs text-flux-text-muted">
							<MapPin size={13} />
							Address
						</span>
						<span className="text-xs font-mono text-flux-text">
							{node.addr}
						</span>
					</div>
					<div className="flex items-center justify-between">
						<span className="flex items-center gap-2 text-xs text-flux-text-muted">
							<Clock size={13} />
							Uptime
						</span>
						<span className="text-xs font-mono text-flux-text">
							{formatUptime(node.uptime_seconds)}
						</span>
					</div>
					<div className="flex items-center justify-between">
						<span className="flex items-center gap-2 text-xs text-flux-text-muted">
							<Shield size={13} />
							Role
						</span>
						<span className="text-xs font-mono text-flux-text">
							{node.is_leader ? "Leader" : "Follower"}
						</span>
					</div>
				</div>
			</CardContent>
		</Card>
	);
}

function ErrorCard({
	label,
	count,
	icon: Icon,
}: {
	label: string;
	count: number;
	icon: React.ElementType;
}) {
	const hasErrors = count > 0;

	return (
		<Card className="border-flux-card-border bg-flux-card overflow-hidden">
			<CardContent className="p-5">
				<div className="flex items-center gap-3 mb-2">
					<div
						className={`p-2 rounded-lg ${
							hasErrors ? "bg-flux-red/10" : "bg-flux-green/10"
						}`}
					>
						<Icon
							size={16}
							className={hasErrors ? "text-flux-red" : "text-flux-green"}
						/>
					</div>
					<p className="text-xs text-flux-text-muted">{label}</p>
				</div>
				<p
					className={`text-2xl font-bold ${
						hasErrors ? "text-flux-red" : "text-flux-text"
					}`}
				>
					{formatCount(count)}
				</p>
			</CardContent>
		</Card>
	);
}

const InfoClient = () => {
	const [status, setStatus] = useState<BrokerStatus | null>(null);
	const [nodes, setNodes] = useState<NodeInfo[]>([]);
	const [loading, setLoading] = useState(true);

	useEffect(() => {
		getBrokerOverview()
			.then(({ status, nodes }) => {
				setStatus(status);
				setNodes(nodes);
			})
			.catch(console.error)
			.finally(() => setLoading(false));
	}, []);

	const totalErrors = status
		? status.protocol_errors +
			status.auth_errors +
			status.authz_errors +
			status.packet_errors
		: 0;

	return (
		<div className="p-4 sm:p-6 lg:p-8 space-y-6">
			<div>
				<h1 className="text-3xl font-bold text-flux-text mb-1">
					Broker Health
				</h1>
				<p className="text-flux-text-muted text-sm">
					Cluster topology, node health, and error diagnostics
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
				<>
					{/* Cluster Status */}
					<Card className="border-flux-card-border bg-flux-card overflow-hidden">
						<CardContent className="p-5">
							<div className="flex flex-wrap items-center gap-x-6 gap-y-3">
								<div className="flex items-center gap-2.5">
									<Cpu
										size={18}
										className={
											totalErrors === 0 ? "text-flux-green" : "text-flux-orange"
										}
									/>
									<span className="text-sm font-semibold text-flux-text">
										Cluster Status
									</span>
								</div>

								<div className="flex items-center gap-2">
									{status.cluster_mode ? (
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
									)}

									<Badge
										variant="outline"
										className={`text-xs ${
											totalErrors === 0
												? "bg-flux-green/10 text-flux-green border-flux-green/20"
												: "bg-flux-orange/10 text-flux-orange border-flux-orange/20"
										}`}
									>
										{totalErrors === 0
											? "All Clear"
											: `${formatCount(totalErrors)} Error${totalErrors !== 1 ? "s" : ""}`}
									</Badge>
								</div>

								<div className="flex items-center gap-4 ml-auto text-xs text-flux-text-muted">
									<span>
										Nodes:{" "}
										<span className="font-mono text-flux-text">
											{nodes.length || 1}
										</span>
									</span>
									<span>
										Uptime:{" "}
										<span className="font-mono text-flux-text">
											{formatUptime(status.uptime_seconds)}
										</span>
									</span>
								</div>
							</div>
						</CardContent>
					</Card>

					{/* Node Health Cards */}
					{nodes.length > 0 && (
						<div>
							<h2 className="text-sm font-semibold text-flux-text-muted uppercase tracking-wide mb-3">
								Node Health
							</h2>
							<div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 gap-4">
								{nodes.map((node) => (
									<NodeCard key={node.node_id} node={node} />
								))}
							</div>
						</div>
					)}

					{/* Error Counts */}
					<div>
						<h2 className="text-sm font-semibold text-flux-text-muted uppercase tracking-wide mb-3">
							Errors
						</h2>
						<div className="grid grid-cols-2 sm:grid-cols-4 gap-4">
							<ErrorCard
								label="Protocol"
								count={status.protocol_errors}
								icon={AlertTriangle}
							/>
							<ErrorCard
								label="Authentication"
								count={status.auth_errors}
								icon={ShieldAlert}
							/>
							<ErrorCard
								label="Authorization"
								count={status.authz_errors}
								icon={ShieldAlert}
							/>
							<ErrorCard
								label="Packet"
								count={status.packet_errors}
								icon={AlertTriangle}
							/>
						</div>
					</div>
				</>
			)}
		</div>
	);
};

export default InfoClient;
