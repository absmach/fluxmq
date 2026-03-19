import type { Metadata } from "next";
import "./globals.css";
import { ThemeProvider } from "@/lib/theme-provider";

export const metadata: Metadata = {
	title: "FluxMQ Dashboard",
	description: "Real-time monitoring for FluxMQ message broker",
};

export default function RootLayout({
	children,
}: {
	children: React.ReactNode;
}) {
	return (
		<html lang="en">
			<head>
				<link rel="preconnect" href="https://fonts.googleapis.com" />
				<link
					rel="preconnect"
					href="https://fonts.gstatic.com"
					crossOrigin="anonymous"
				/>
				<link
					href="https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700;800&display=swap"
					rel="stylesheet"
				/>
			</head>
			<body className="font-inter">
				<ThemeProvider>{children}</ThemeProvider>
			</body>
		</html>
	);
}
