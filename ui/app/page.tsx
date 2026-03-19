import Link from "next/link";
import { FluxLogo } from "@/components/flux-logo";

export default function Home() {
	return (
		<main className="flex items-center justify-center min-h-screen bg-flux-bg">
			<div className="text-center space-y-6">
				<div className="space-y-2">
					<h1 className="text-7xl font-bold tracking-tight">
						<FluxLogo />
					</h1>
				</div>

				<p className="text-flux-text-muted">Welcome to the Flux Dashboard</p>

				<Link
					href="/dashboard"
					className="inline-flex items-center gap-2 px-6 py-3 rounded-lg font-semibold text-white transition-opacity hover:opacity-90"
					style={{ background: "var(--flux-blue)" }}
				>
					View Dashboard →
				</Link>

				<div className="flex justify-center pt-4">
					<div
						className="animate-pulse h-2 w-2 rounded-full"
						style={{ background: "var(--flux-blue)" }}
					/>
				</div>
			</div>
		</main>
	);
}
