/** @type {import('tailwindcss').Config} */
export default {
	content: ["./app/**/*.{js,ts,jsx,tsx}", "./components/**/*.{js,ts,jsx,tsx}"],
	darkMode: "class",
	theme: {
		extend: {
			colors: {
				/* ── Theme-aware CSS-var tokens ──────────────────────────────────── */
				"flux-bg": "var(--flux-bg)",
				"flux-bg-alt": "var(--flux-bg-alt)",
				"flux-fg": "var(--flux-fg)",
				"flux-border": "var(--flux-border)",
				"flux-grid": "var(--flux-grid)",
				"flux-text": "var(--flux-text)",
				"flux-text-muted": "var(--flux-text-muted)",
				"flux-card": "var(--flux-card)",
				"flux-card-border": "var(--flux-card-border)",
				"flux-hover": "var(--flux-hover)",
				// Aliases used in dashboard pages
				"dark-surface": "var(--flux-surface)",
				"dark-border": "var(--flux-surface-border)",

				/* ── Brand blue with full scale ─────────────────────────────────── */
				"flux-blue": {
					50: "#eef4fb",
					100: "#d5e4f5",
					200: "#adc9eb",
					300: "#85afe1",
					400: "#5d94d7",
					500: "#3579cc",
					600: "#2F69B3", // ← brand
					700: "#255898",
					800: "#1c477d",
					900: "#133662",
					950: "#091e3b",
					DEFAULT: "var(--flux-blue)",
				},

				/* ── Brand orange with full scale ───────────────────────────────── */
				"flux-orange": {
					50: "#fef6e8",
					100: "#fde9c2",
					200: "#fcd185",
					300: "#fab847",
					400: "#F9A32A", // ← brand
					500: "#e88910",
					600: "#c26f0b",
					700: "#9b5508",
					800: "#753e06",
					900: "#4e2904",
					950: "#2b1402",
					DEFAULT: "var(--flux-orange)",
				},

				/* ── Accent / semantic (fixed hex — used for decorative gradients) */
				"flux-purple": "#7c3aed",
				"flux-dark": "#1e293b",
				"flux-green": "#10b981",
				"flux-teal": "#0891b2",
				"flux-red": "#ef4444",

				/* ── shadcn/ui CSS variable references ──────────────────────────── */
				background: "hsl(var(--background))",
				foreground: "hsl(var(--foreground))",
				card: {
					DEFAULT: "hsl(var(--card))",
					foreground: "hsl(var(--card-foreground))",
				},
				primary: {
					DEFAULT: "hsl(var(--primary))",
					foreground: "hsl(var(--primary-foreground))",
				},
				secondary: {
					DEFAULT: "hsl(var(--secondary))",
					foreground: "hsl(var(--secondary-foreground))",
				},
				muted: {
					DEFAULT: "hsl(var(--muted))",
					foreground: "hsl(var(--muted-foreground))",
				},
				accent: {
					DEFAULT: "hsl(var(--accent))",
					foreground: "hsl(var(--accent-foreground))",
				},
				destructive: {
					DEFAULT: "hsl(var(--destructive))",
					foreground: "hsl(var(--destructive-foreground))",
				},
				border: "hsl(var(--border))",
				input: "hsl(var(--input))",
				ring: "hsl(var(--ring))",
			},

			borderRadius: {
				lg: "var(--radius)",
				md: "calc(var(--radius) - 2px)",
				sm: "calc(var(--radius) - 4px)",
			},

			fontFamily: {
				inter: ["Inter", "sans-serif"],
			},
		},
	},
	plugins: [],
};
