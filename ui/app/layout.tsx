import type { Metadata } from "next";
import Script from "next/script";
import "./globals.css";
import { ThemeProvider } from "@/lib/theme-provider";

export const metadata: Metadata = {
	title: "FluxMQ Dashboard",
	description: "Real-time monitoring for FluxMQ message broker",
};

const themeInitScript = `
(() => {
	try {
		const storedTheme = localStorage.getItem("fluxmq-theme");
		const prefersDark = window.matchMedia("(prefers-color-scheme: dark)").matches;
		const isDark = storedTheme ? storedTheme === "dark" : prefersDark;
		document.documentElement.classList.toggle("dark", isDark);
	} catch {}
})();
`;

export default function RootLayout({
	children,
}: {
	children: React.ReactNode;
}) {
	return (
		<html lang="en" suppressHydrationWarning>
			<head>
				<Script id="fluxmq-theme-init" strategy="beforeInteractive">
					{themeInitScript}
				</Script>
				<link rel="preconnect" href="https://fonts.googleapis.com" />
				<link
					rel="preconnect"
					href="https://fonts.gstatic.com"
					crossOrigin="anonymous"
				/>
				<link
					href="https://fonts.googleapis.com/css2?family=Geist:wght@100..900&display=swap"
					rel="stylesheet"
				/>
			</head>
			<body className="font-geist">
				<ThemeProvider>{children}</ThemeProvider>
			</body>
		</html>
	);
}
