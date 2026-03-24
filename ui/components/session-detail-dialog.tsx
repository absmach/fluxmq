"use client";

import { Clock, ListFilter, Radio, Server } from "lucide-react";
import { useEffect, useState } from "react";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import {
	Dialog,
	DialogContent,
	DialogDescription,
	DialogHeader,
	DialogTitle,
} from "@/components/ui/dialog";
import {
	Table,
	TableBody,
	TableCell,
	TableHead,
	TableHeader,
	TableRow,
} from "@/components/ui/table";
import type { SessionInfo } from "@/lib/api";
import {
	formatProtocolLabel,
	PROTOCOL_BADGE_CLASSES,
	resolveSessionProtocol,
} from "@/lib/session-protocol";

function fmt(date: string) {
	return new Date(date).toLocaleString(undefined, {
		dateStyle: "medium",
		timeStyle: "medium",
	});
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

function SessionSubscriptionsDialog({
	session,
	open,
	onOpenChange,
}: {
	session: SessionInfo;
	open: boolean;
	onOpenChange: (open: boolean) => void;
}) {
	const subscriptions = session.subscriptions ?? [];

	return (
		<Dialog open={open} onOpenChange={onOpenChange}>
			<DialogContent className="bg-flux-card border-flux-card-border text-flux-text max-w-2xl">
				<DialogHeader>
					<DialogTitle className="text-flux-text">
						Active Subscriptions
					</DialogTitle>
					<DialogDescription className="text-flux-text-muted">
						<span className="font-mono">{session.client_id}</span> has{" "}
						{subscriptions.length} active subscription
						{subscriptions.length !== 1 ? "s" : ""}.
					</DialogDescription>
				</DialogHeader>

				<div className="overflow-x-auto">
					<Table>
						<TableHeader>
							<TableRow className="border-flux-card-border hover:bg-transparent">
								<TableHead>Filter</TableHead>
								<TableHead className="text-right">QoS</TableHead>
							</TableRow>
						</TableHeader>
						<TableBody>
							{subscriptions.map((sub) => (
								<TableRow
									key={sub.filter}
									className="border-flux-card-border hover:bg-flux-hover"
								>
									<TableCell className="font-mono text-sm text-flux-text py-3">
										{sub.filter}
									</TableCell>
									<TableCell className="text-right py-3">
										<Badge
											variant="outline"
											className="text-xs bg-flux-blue/10 text-flux-blue border-flux-blue/20"
										>
											QoS {sub.qos}
										</Badge>
									</TableCell>
								</TableRow>
							))}
							{subscriptions.length === 0 && (
								<TableRow className="hover:bg-transparent">
									<TableCell
										colSpan={2}
										className="text-center text-flux-text-muted py-10"
									>
										No active subscriptions.
									</TableCell>
								</TableRow>
							)}
						</TableBody>
					</Table>
				</div>
			</DialogContent>
		</Dialog>
	);
}

export function SessionDetailDialog({
	session,
	onClose,
}: {
	session: SessionInfo | null;
	onClose: () => void;
}) {
	const protocolKey = session ? resolveSessionProtocol(session) : "unknown";
	const [subscriptionsOpen, setSubscriptionsOpen] = useState(false);

	useEffect(() => {
		if (!session) {
			setSubscriptionsOpen(false);
		}
	}, [session]);

	return (
		<>
			<Dialog
				open={!!session}
				onOpenChange={(open) => {
					if (!open) onClose();
				}}
			>
				<DialogContent className="bg-flux-card border-flux-card-border text-flux-text max-w-md p-0 overflow-hidden">
					{session && (
						<>
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
													className={`text-xs ${PROTOCOL_BADGE_CLASSES[protocolKey]}`}
												>
													{formatProtocolLabel(protocolKey)}
												</Badge>
											</div>
										</div>
									</div>
									<DialogDescription className="sr-only">
										Session details for {session.client_id}
									</DialogDescription>
								</DialogHeader>
							</div>

							<div className="px-6 pb-6 overflow-y-auto max-h-[60vh]">
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

								<SectionLabel icon={Server} label="Connection" />
								{session.node_id && (
									<DetailRow label="Node ID">
										<span className="font-mono">{session.node_id}</span>
									</DetailRow>
								)}
								{session.connection_name && (
									<DetailRow label="Client Name">
										<span className="break-all">{session.connection_name}</span>
									</DetailRow>
								)}
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
										<span className="font-mono">
											{session.expiry_interval}s
										</span>
									</DetailRow>
								)}
								{session.receive_maximum > 0 &&
									session.receive_maximum < 65535 && (
										<DetailRow label="Receive Maximum">
											<span className="font-mono">
												{session.receive_maximum}
											</span>
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

								<SectionLabel icon={Clock} label="Subscriptions" />
								<DetailRow label="Active Subscriptions">
									{session.subscription_count > 0 ? (
										<Button
											variant="ghost"
											size="sm"
											className="text-xs text-flux-text-muted hover:text-flux-blue hover:bg-flux-blue/10"
											onClick={() => setSubscriptionsOpen(true)}
										>
											View list ({session.subscription_count})
										</Button>
									) : (
										<span className="text-flux-text-muted">None</span>
									)}
								</DetailRow>
							</div>

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

			{session && (
				<SessionSubscriptionsDialog
					session={session}
					open={subscriptionsOpen}
					onOpenChange={setSubscriptionsOpen}
				/>
			)}
		</>
	);
}
