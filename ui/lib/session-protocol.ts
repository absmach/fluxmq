import type { SessionInfo } from "@/lib/api";

export type SessionProtocolKey = string;

const MQTT5_ALIASES = new Set(["mqtt5", "mqtt 5", "mqtt 5.0"]);
const MQTT311_ALIASES = new Set(["mqtt3.1.1", "mqtt 3.1.1", "mqtt311"]);
const MQTT31_ALIASES = new Set(["mqtt3.1", "mqtt 3.1", "mqtt31"]);
const AMQP091_ALIASES = new Set([
	"amqp",
	"amqp0.9.1",
	"amqp 0.9.1",
	"amqp091",
	"amqp_0_9_1",
]);
const AMQP10_ALIASES = new Set([
	"amqp1",
	"amqp1.0",
	"amqp 1.0",
	"amqp10",
	"amqp_1_0",
]);

const PROTOCOL_BADGE_MAP: Record<string, string> = {
	mqtt5: "bg-flux-blue/10 text-flux-blue border-flux-blue/20",
	"mqtt3.1.1": "bg-flux-teal/10 text-flux-teal border-flux-teal/20",
	"mqtt3.1": "bg-flux-teal/10 text-flux-teal border-flux-teal/20",
	"amqp0.9.1": "bg-flux-orange/10 text-flux-orange border-flux-orange/20",
	"amqp1.0": "bg-flux-purple/10 text-flux-purple border-flux-purple/20",
};

const FALLBACK_BADGE =
	"bg-flux-text-muted/10 text-flux-text-muted border-flux-card-border";

export const PROTOCOL_BADGE_CLASSES = new Proxy(PROTOCOL_BADGE_MAP, {
	get(target, key: PropertyKey) {
		if (typeof key !== "string") return FALLBACK_BADGE;
		return target[key] ?? FALLBACK_BADGE;
	},
});

export function resolveSessionProtocol(
	session: Pick<SessionInfo, "client_id" | "protocol" | "version">,
): SessionProtocolKey {
	const raw = String(session.protocol || "")
		.trim()
		.toLowerCase();

	if (MQTT5_ALIASES.has(raw) || session.version === 5) return "mqtt5";
	if (MQTT311_ALIASES.has(raw) || session.version === 4) return "mqtt3.1.1";
	if (MQTT31_ALIASES.has(raw) || session.version === 3) return "mqtt3.1";
	if (AMQP091_ALIASES.has(raw)) return "amqp0.9.1";
	if (AMQP10_ALIASES.has(raw)) return "amqp1.0";

	if (session.client_id.startsWith("amqp091-")) return "amqp0.9.1";
	if (session.client_id.startsWith("amqp:")) return "amqp1.0";

	return raw || "unknown";
}

const PROTOCOL_LABELS: Record<string, string> = {
	mqtt5: "MQTT 5.0",
	"mqtt3.1.1": "MQTT 3.1.1",
	"mqtt3.1": "MQTT 3.1",
	"amqp0.9.1": "AMQP 0.9.1",
	"amqp1.0": "AMQP 1.0",
};

export function formatProtocolLabel(key: SessionProtocolKey): string {
	return PROTOCOL_LABELS[key] ?? key.toUpperCase().replace(/[._-]/g, " ");
}
