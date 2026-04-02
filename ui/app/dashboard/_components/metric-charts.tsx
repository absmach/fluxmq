"use client";

import { AlertTriangle } from "lucide-react";
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
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { formatBytes } from "@/lib/format";

export interface ChartPoint {
	time: string;
	sessions: number;
	msgsIn: number;
	msgsOut: number;
	bytesIn: number;
	bytesOut: number;
}

interface MetricChartsProps {
	displayHistory: ChartPoint[];
	loaded: boolean;
	error: string | null;
	scopeLabel: string;
	selectedNodeId: string | null;
	pollSeconds: number;
	trafficSummary: string;
	bandwidthSummary: string;
	sessionsSummary: string;
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

const gridColor = "var(--flux-grid)";
const axisColor = "var(--flux-text-muted)";

const tooltipStyle = {
	backgroundColor: "var(--flux-card)",
	border: "1px solid var(--flux-card-border)",
	borderRadius: "8px",
	color: "var(--flux-text)",
	fontSize: 12,
};

export function MetricCharts({
	displayHistory,
	loaded,
	error,
	scopeLabel,
	selectedNodeId,
	pollSeconds,
	trafficSummary,
	bandwidthSummary,
	sessionsSummary,
}: MetricChartsProps) {
	return (
		<div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
			<Card className="border-flux-card-border bg-flux-card">
				<CardHeader className="pb-2">
					<CardTitle className="text-base text-flux-text">
						Message Traffic Trends
					</CardTitle>
					<p className="text-xs text-flux-text-muted">
						{scopeLabel} · msgs/s in &amp; out · polled every {pollSeconds}s
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
					<CardTitle className="text-base text-flux-text">Bandwidth</CardTitle>
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
	);
}
