"use client";

import { BookMarked, Search } from "lucide-react";
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
import { getSession, getSessions } from "@/lib/services/sessions";

interface AggregatedSubscription {
	filter: string;
	subscriber_count: number;
	max_qos: number;
	clients: string[];
}

const QOS_COLORS = [
	"bg-flux-green/10 text-flux-green border-flux-green/20",
	"bg-flux-blue/10 text-flux-blue border-flux-blue/20",
	"bg-flux-orange/10 text-flux-orange border-flux-orange/20",
];

const SubsClient = () => {
	const [subscriptions, setSubscriptions] = useState<AggregatedSubscription[]>(
		[],
	);
	const [loading, setLoading] = useState(true);
	const [search, setSearch] = useState("");
	const [page, setPage] = useState(1);
	const [limit, setLimit] = useState(10);

	useEffect(() => {
		async function load() {
			setLoading(true);
			try {
				const { sessions } = await getSessions({ state: "connected" });
				const details = await Promise.all(
					sessions.map((s) => getSession(s.client_id).catch(() => null)),
				);
				const map = new Map<string, AggregatedSubscription>();
				for (const detail of details) {
					if (!detail?.subscriptions) continue;
					for (const sub of detail.subscriptions) {
						const existing = map.get(sub.filter);
						if (existing) {
							existing.subscriber_count++;
							existing.max_qos = Math.max(existing.max_qos, sub.qos);
							existing.clients.push(detail.client_id);
						} else {
							map.set(sub.filter, {
								filter: sub.filter,
								subscriber_count: 1,
								max_qos: sub.qos,
								clients: [detail.client_id],
							});
						}
					}
				}
				setSubscriptions(
					Array.from(map.values()).sort(
						(a, b) => b.subscriber_count - a.subscriber_count,
					),
				);
			} catch (e) {
				console.error(e);
			} finally {
				setLoading(false);
			}
		}
		load();
	}, []);

	const filtered = search
		? subscriptions.filter((s) =>
				s.filter.toLowerCase().includes(search.toLowerCase()),
			)
		: subscriptions;

	const totalPages = Math.max(1, Math.ceil(filtered.length / limit));
	const paginated = filtered.slice((page - 1) * limit, page * limit);

	return (
		<div className="p-4 sm:p-6 lg:p-8 space-y-6">
			<div className="flex items-start justify-between gap-4">
				<div>
					<h1 className="text-3xl font-bold text-flux-text mb-1">
						Subscriptions
					</h1>
					<p className="text-flux-text-muted">
						Active topic filters aggregated from connected sessions
					</p>
				</div>
				<Badge
					variant="outline"
					className="flex items-center gap-1.5 text-sm px-3 py-1.5 min-h-10 bg-flux-blue/10 text-flux-blue border-flux-blue/30"
				>
					<BookMarked className="w-3.5 h-3.5" />
					{subscriptions.length} filters
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
								placeholder="Search filters..."
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
									<TableHead>Filter</TableHead>
									<TableHead className="text-right">Subscribers</TableHead>
									<TableHead>Max QoS</TableHead>
									<TableHead>Clients</TableHead>
								</TableRow>
							</TableHeader>
							<TableBody>
								{loading && (
									<TableRow className="hover:bg-transparent">
										<TableCell
											colSpan={4}
											className="text-center text-flux-text-muted py-12"
										>
											Loading subscriptions…
										</TableCell>
									</TableRow>
								)}
								{!loading &&
									paginated.map((sub) => (
										<TableRow
											key={sub.filter}
											className="border-flux-card-border hover:bg-flux-hover"
										>
											<TableCell className="font-mono text-sm text-flux-text font-medium py-4">
												{sub.filter}
											</TableCell>
											<TableCell className="text-right text-sm text-flux-text tabular-nums py-4">
												{sub.subscriber_count}
											</TableCell>
											<TableCell>
												<Badge
													variant="outline"
													className={`text-xs ${QOS_COLORS[sub.max_qos] ?? QOS_COLORS[0]}`}
												>
													QoS {sub.max_qos}
												</Badge>
											</TableCell>
											<TableCell>
												<div className="flex flex-wrap gap-1">
													{sub.clients.map((c) => (
														<span
															key={c}
															className="text-xs font-mono bg-flux-bg border border-flux-card-border text-flux-text-muted px-1.5 py-0.5 rounded"
														>
															{c}
														</span>
													))}
												</div>
											</TableCell>
										</TableRow>
									))}
								{!loading && paginated.length === 0 && (
									<TableRow className="hover:bg-transparent">
										<TableCell
											colSpan={4}
											className="text-center text-flux-text-muted py-12"
										>
											No active subscriptions.
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
						itemLabel="filters"
					/>
				</CardContent>
			</Card>
		</div>
	);
};

export default SubsClient;
