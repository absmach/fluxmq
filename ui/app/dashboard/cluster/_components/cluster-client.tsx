"use client";

import { CheckCircle2, Network, XCircle } from "lucide-react";
import { useEffect, useState } from "react";
import { Badge } from "@/components/ui/badge";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import {
	Table,
	TableBody,
	TableCell,
	TableHead,
	TableHeader,
	TableRow,
} from "@/components/ui/table";
import { TablePagination } from "@/components/ui/table-pagination";
import type { NodeInfo } from "@/lib/api";
import { formatBytes, formatCount, formatUptime } from "@/lib/api";
import { getBrokerOverview } from "@/lib/services/broker";

const ClusterClient = () => {
	const [nodes, setNodes] = useState<NodeInfo[]>([]);
	const [nodeId, setNodeId] = useState<string>("");
	const [clusterMode, setClusterMode] = useState(false);
	const [loading, setLoading] = useState(true);
	const [page, setPage] = useState(1);
	const [limit, setLimit] = useState(10);

	useEffect(() => {
		getBrokerOverview()
			.then(({ status, nodes }) => {
				setNodes(nodes);
				setNodeId(status.node_id);
				setClusterMode(status.cluster_mode);
			})
			.catch(console.error)
			.finally(() => setLoading(false));
	}, []);

	const healthyCount = nodes.filter((n) => n.healthy !== false).length;
	const totalPages = Math.max(1, Math.ceil(nodes.length / limit));
	const paginated = nodes.slice((page - 1) * limit, page * limit);

	return (
		<div className="p-4 sm:p-6 lg:p-8 space-y-6">
			<div className="flex items-start justify-between gap-4">
				<div>
					<h1 className="text-3xl font-bold text-flux-text mb-1">Cluster</h1>
					<p className="text-flux-text-muted">
						Node topology and per-node statistics
					</p>
				</div>
				<div className="flex items-center gap-2 flex-wrap">
					<Badge
						variant="outline"
						className="flex items-center gap-1.5 text-sm px-3 py-1.5 min-h-10 bg-flux-blue/10 text-flux-blue border-flux-blue/30"
					>
						<Network className="w-3.5 h-3.5" />
						{nodes.length} node{nodes.length !== 1 ? "s" : ""}
					</Badge>
					<Badge
						variant="outline"
						className={`flex items-center gap-1.5 text-sm px-3 py-1.5 min-h-10 ${
							healthyCount === nodes.length
								? "bg-flux-green/10 text-flux-green border-flux-green/30"
								: "bg-flux-orange/10 text-flux-orange border-flux-orange/30"
						}`}
					>
						{healthyCount === nodes.length ? (
							<CheckCircle2 className="w-3.5 h-3.5" />
						) : (
							<XCircle className="w-3.5 h-3.5" />
						)}
						{healthyCount}/{nodes.length} healthy
					</Badge>
				</div>
			</div>

			{!clusterMode && !loading && (
				<div className="rounded-lg border border-flux-blue/20 bg-flux-blue/5 px-4 py-3 text-sm text-flux-blue">
					Running in single-node mode — connected via{" "}
					<span className="font-mono">{nodeId}</span>
				</div>
			)}

			<Card className="p-6 border-flux-card-border bg-flux-card">
				<CardHeader className="pb-3">
					<CardTitle className="text-base text-flux-text">Nodes</CardTitle>
				</CardHeader>
				<CardContent className="p-0">
					<div className="overflow-x-auto">
						<Table>
							<TableHeader>
								<TableRow className="border-flux-card-border hover:bg-transparent">
									<TableHead className="pl-6">Node</TableHead>
									<TableHead>Address</TableHead>
									<TableHead>Health</TableHead>
									<TableHead className="text-right">Sessions</TableHead>
									<TableHead className="text-right">Subscriptions</TableHead>
									<TableHead className="text-right">Msgs In</TableHead>
									<TableHead className="text-right">Msgs Out</TableHead>
									<TableHead className="text-right">Bytes In</TableHead>
									<TableHead className="text-right pr-6">Uptime</TableHead>
								</TableRow>
							</TableHeader>
							<TableBody>
								{loading && (
									<TableRow className="hover:bg-transparent">
										<TableCell
											colSpan={9}
											className="text-center text-flux-text-muted py-12"
										>
											Loading cluster topology…
										</TableCell>
									</TableRow>
								)}
								{!loading &&
									paginated.map((node) => (
										<TableRow
											key={node.node_id}
											className="border-flux-card-border hover:bg-flux-hover"
										>
											<TableCell className="pl-6 py-5">
												<div className="flex items-center gap-2">
													<span
														className={`inline-block w-2 h-2 rounded-full shrink-0 ${
															node.healthy !== false
																? "bg-flux-green"
																: "bg-flux-red"
														}`}
													/>
													<span className="text-flux-text font-medium text-sm font-mono">
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
													{node.node_id === nodeId && (
														<Badge
															variant="outline"
															className="text-xs bg-flux-purple/10 text-flux-purple border-flux-purple/20"
														>
															You
														</Badge>
													)}
												</div>
											</TableCell>
											<TableCell className="text-flux-text-muted font-mono text-xs py-5">
												{node.addr}
											</TableCell>
											<TableCell>
												{node.healthy !== false ? (
													<Badge
														variant="outline"
														className="text-xs bg-flux-green/10 text-flux-green border-flux-green/20"
													>
														Healthy
													</Badge>
												) : (
													<Badge
														variant="outline"
														className="text-xs bg-flux-red/10 text-flux-red border-flux-red/20"
													>
														Unhealthy
													</Badge>
												)}
											</TableCell>
											<TableCell className="text-right text-sm text-flux-text tabular-nums py-5">
												{node.sessions !== undefined ? (
													formatCount(node.sessions)
												) : (
													<span className="text-flux-text-muted">—</span>
												)}
											</TableCell>
											<TableCell className="text-right text-sm text-flux-text tabular-nums py-5">
												{node.subscriptions !== undefined ? (
													formatCount(node.subscriptions)
												) : (
													<span className="text-flux-text-muted">—</span>
												)}
											</TableCell>
											<TableCell className="text-right text-sm text-flux-text tabular-nums py-5">
												{node.messages_received !== undefined ? (
													formatCount(node.messages_received)
												) : (
													<span className="text-flux-text-muted">—</span>
												)}
											</TableCell>
											<TableCell className="text-right text-sm text-flux-text tabular-nums py-5">
												{node.messages_sent !== undefined ? (
													formatCount(node.messages_sent)
												) : (
													<span className="text-flux-text-muted">—</span>
												)}
											</TableCell>
											<TableCell className="text-right text-sm text-flux-text tabular-nums py-5">
												{node.bytes_received !== undefined ? (
													formatBytes(node.bytes_received)
												) : (
													<span className="text-flux-text-muted">—</span>
												)}
											</TableCell>
											<TableCell className="text-right text-sm text-flux-text-muted pr-6 py-5">
												{formatUptime(node.uptime_seconds)}
											</TableCell>
										</TableRow>
									))}
								{!loading && nodes.length === 0 && (
									<TableRow className="hover:bg-transparent">
										<TableCell
											colSpan={9}
											className="text-center text-flux-text-muted py-12"
										>
											No nodes found.
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
						totalItems={nodes.length}
						setPage={setPage}
						setLimit={setLimit}
						itemLabel="nodes"
					/>
				</CardContent>
			</Card>
		</div>
	);
};

export default ClusterClient;
