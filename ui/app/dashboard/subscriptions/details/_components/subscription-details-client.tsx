"use client";

import { ArrowLeft, Search, Users } from "lucide-react";
import Link from "next/link";
import { useCallback, useEffect, useMemo, useState } from "react";
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
import type { SessionInfo } from "@/lib/api";
import { getSession } from "@/lib/services/sessions";
import {
	type AggregatedSubscription,
	getAggregatedSubscriptions,
	type SubscriptionClient,
} from "@/lib/services/subscriptions";

const QOS_COLORS = [
	"bg-flux-green/10 text-flux-green border-flux-green/20",
	"bg-flux-blue/10 text-flux-blue border-flux-blue/20",
	"bg-flux-orange/10 text-flux-orange border-flux-orange/20",
];

const SubscriptionDetailsClient = ({ filter }: { filter: string }) => {
	const [subscription, setSubscription] =
		useState<AggregatedSubscription | null>(null);
	const [loading, setLoading] = useState(true);
	const [search, setSearch] = useState("");
	const [page, setPage] = useState(1);
	const [limit, setLimit] = useState(10);
	const [detailSession, setDetailSession] = useState<SessionInfo | null>(null);
	const [loadingSessionKey, setLoadingSessionKey] = useState<string | null>(
		null,
	);

	useEffect(() => {
		async function load() {
			setLoading(true);
			try {
				const aggregated = await getAggregatedSubscriptions();
				const match = aggregated.find((item) => item.filter === filter) ?? null;
				setSubscription(match);
			} catch (err) {
				console.error(err);
				setSubscription(null);
			} finally {
				setLoading(false);
			}
		}
		if (!filter) {
			setSubscription(null);
			setLoading(false);
			return;
		}
		void load();
	}, [filter]);

	const filteredClients = useMemo(() => {
		if (!subscription) return [];
		if (!search) return subscription.clients;
		const q = search.toLowerCase();
		return subscription.clients.filter(
			(client) =>
				client.client_id.toLowerCase().includes(q) ||
				(client.node_id ?? "").toLowerCase().includes(q),
		);
	}, [search, subscription]);

	const totalPages = Math.max(1, Math.ceil(filteredClients.length / limit));
	const paginated = filteredClients.slice((page - 1) * limit, page * limit);

	const openSessionDetail = useCallback(async (client: SubscriptionClient) => {
		const key = `${client.node_id ?? ""}:${client.client_id}`;
		setLoadingSessionKey(key);
		try {
			const detail = await getSession(client.client_id, client.node_id);
			setDetailSession({
				...detail,
				node_id: detail.node_id ?? client.node_id,
			});
		} catch (err) {
			console.error(err);
		} finally {
			setLoadingSessionKey(null);
		}
	}, []);

	return (
		<div className="p-4 sm:p-6 lg:p-8 space-y-6">
			<div className="flex items-start justify-between gap-4">
				<div className="space-y-2">
					<Button asChild variant="ghost" size="sm" className="px-2">
						<Link
							href="/dashboard/subscriptions"
							className="inline-flex gap-1.5"
						>
							<ArrowLeft size={14} />
							Back to Subscriptions
						</Link>
					</Button>
					<h1 className="text-3xl font-bold text-flux-text">
						Subscription Clients
					</h1>
					<p className="text-flux-text-muted">
						{filter ? (
							<>
								Clients subscribed to{" "}
								<span className="font-mono">{filter}</span>
							</>
						) : (
							"Missing subscription filter."
						)}
					</p>
				</div>
				<Badge
					variant="outline"
					className="flex items-center gap-1.5 text-sm px-3 py-1.5 min-h-10 bg-flux-blue/10 text-flux-blue border-flux-blue/30"
				>
					<Users className="w-3.5 h-3.5" />
					{subscription?.subscriber_count ?? 0} clients
				</Badge>
			</div>

			<Card className="border-flux-card-border bg-flux-card">
				<CardContent className="p-6">
					<div className="flex items-center mb-6">
						<div className="relative flex-1 w-full sm:max-w-xs">
							<Search
								className="absolute left-3 top-1/2 -translate-y-1/2 text-flux-text-muted"
								size={16}
							/>
							<Input
								type="text"
								placeholder="Search clients or nodes..."
								value={search}
								onChange={(e) => {
									setSearch(e.target.value);
									setPage(1);
								}}
								className="pl-9 bg-flux-bg border-flux-card-border text-flux-text placeholder:text-flux-text-muted focus-visible:ring-flux-blue"
							/>
						</div>
					</div>

					<div className="overflow-x-auto">
						<Table>
							<TableHeader>
								<TableRow className="border-flux-card-border hover:bg-transparent">
									<TableHead>Client ID</TableHead>
									<TableHead>Node</TableHead>
									<TableHead>QoS</TableHead>
									<TableHead className="text-right">Actions</TableHead>
								</TableRow>
							</TableHeader>
							<TableBody>
								{loading && (
									<TableRow className="hover:bg-transparent">
										<TableCell
											colSpan={4}
											className="text-center text-flux-text-muted py-12"
										>
											Loading subscription clients…
										</TableCell>
									</TableRow>
								)}
								{!loading &&
									paginated.map((client) => {
										const key = `${client.node_id ?? ""}:${client.client_id}`;
										return (
											<TableRow
												key={key}
												className="border-flux-card-border hover:bg-flux-hover"
											>
												<TableCell className="font-mono text-sm text-flux-text font-medium py-4">
													{client.client_id}
												</TableCell>
												<TableCell className="font-mono text-sm text-flux-text-muted py-4">
													{client.node_id ?? "—"}
												</TableCell>
												<TableCell>
													<Badge
														variant="outline"
														className={`text-xs ${QOS_COLORS[client.qos] ?? QOS_COLORS[0]}`}
													>
														QoS {client.qos}
													</Badge>
												</TableCell>
												<TableCell className="text-right">
													<Button
														variant="ghost"
														size="sm"
														className="text-xs text-flux-text-muted hover:text-flux-blue hover:bg-flux-blue/10"
														onClick={() => {
															void openSessionDetail(client);
														}}
														disabled={loadingSessionKey === key}
													>
														{loadingSessionKey === key
															? "Loading…"
															: "View Session"}
													</Button>
												</TableCell>
											</TableRow>
										);
									})}
								{!loading && !filter && (
									<TableRow className="hover:bg-transparent">
										<TableCell
											colSpan={4}
											className="text-center text-flux-text-muted py-12"
										>
											Missing filter query parameter.
										</TableCell>
									</TableRow>
								)}
								{!loading && filter && !subscription && (
									<TableRow className="hover:bg-transparent">
										<TableCell
											colSpan={4}
											className="text-center text-flux-text-muted py-12"
										>
											No active clients for this subscription.
										</TableCell>
									</TableRow>
								)}
								{!loading &&
									filter &&
									subscription &&
									paginated.length === 0 &&
									filteredClients.length === 0 && (
										<TableRow className="hover:bg-transparent">
											<TableCell
												colSpan={4}
												className="text-center text-flux-text-muted py-12"
											>
												No clients match the current filter.
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
						totalItems={filteredClients.length}
						setPage={setPage}
						setLimit={setLimit}
						itemLabel="clients"
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

export default SubscriptionDetailsClient;
