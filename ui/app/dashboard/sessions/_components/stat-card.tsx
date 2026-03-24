import { Card, CardContent } from "@/components/ui/card";

export default function StatCard({
	label,
	value,
	icon: Icon,
	color,
}: {
	label: string;
	value: number;
	icon: React.ElementType;
	color: string;
}) {
	return (
		<Card className="border-flux-card-border bg-flux-card">
			<CardContent className="p-5 flex items-center gap-4">
				<div className={`p-3 rounded-lg ${color}`}>
					<Icon size={20} />
				</div>
				<div>
					<p className="text-2xl font-bold text-flux-text">{value}</p>
					<p className="text-sm text-flux-text-muted">{label}</p>
				</div>
			</CardContent>
		</Card>
	);
}
