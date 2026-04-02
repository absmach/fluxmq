import type { Metadata } from "next";
import { Geist } from "next/font/google";
import { Providers } from "@/components/providers";
import "./globals.css";

const geist = Geist({ subsets: ["latin"] });

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
		<html lang="en" suppressHydrationWarning>
			<body className={geist.className}>
				<Providers>{children}</Providers>
			</body>
		</html>
	);
}
