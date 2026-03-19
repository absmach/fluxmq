interface FluxLogoProps {
	className?: string;
}

export function FluxLogo({ className = "" }: FluxLogoProps) {
	return (
		<span className={className}>
			<span style={{ color: "var(--flux-blue)" }}>Flux</span>
			<span style={{ color: "var(--flux-orange)" }}>MQ</span>
		</span>
	);
}
