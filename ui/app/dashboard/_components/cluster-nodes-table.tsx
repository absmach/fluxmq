"use client";

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
import { formatBytes, formatCount, formatUptime } from "@/lib/format";
import type { NodeInfo } from "@/lib/types";

interface ClusterNodesTableProps {
	nodes: NodeInfo[];
	selectedNodeId: string | null;
	onSelectNode: (id: string | null) => void;
}

export function ClusterNodesTable({
	nodes,
	selectedNodeId,
	onSelectNode,
}: ClusterNodesTableProps) {
	if (nodes.length === 0) return null;

	return (
		<Card className="border-flux-card-border bg-flux-card">
			<CardHeader className="pb-3">
				<div className="flex items-center justify-between">
					<div>
						<CardTitle className="text-base text-flux-text">
							Cluster Nodes
						</CardTitle>
						<p className="text-xs text-flux-text-muted mt-0.5">
							{nodes.length} node{nodes.length !== 1 ? "s" : ""}
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
													onSelectNode(isSelected ? null : node.node_id)
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
	);
}
