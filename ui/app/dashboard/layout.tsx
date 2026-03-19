"use client";

import {
	Activity,
	BookMarked,
	Globe,
	Home,
	Info,
	Menu,
	Moon,
	Network,
	Sun,
	X,
} from "lucide-react";
import Link from "next/link";
import { usePathname } from "next/navigation";
import type React from "react";
import { useEffect, useState } from "react";
import { FluxLogo } from "@/components/flux-logo";
import { Button } from "@/components/ui/button";
import { useTheme } from "@/lib/theme-provider";

const DashboardLayout = ({ children }: { children: React.ReactNode }) => {
	const [sidebarOpen, setSidebarOpen] = useState(false);
	const pathname = usePathname();
	const { theme, toggleTheme } = useTheme();

	useEffect(() => {
		const mediaQuery = window.matchMedia("(min-width: 1024px)");
		const syncSidebar = () => setSidebarOpen(mediaQuery.matches);
		syncSidebar();
		mediaQuery.addEventListener("change", syncSidebar);
		return () => mediaQuery.removeEventListener("change", syncSidebar);
	}, []);

	const navigationItems = [
		{ label: "Overview", href: "/dashboard", icon: Home },
		{ label: "Connections", href: "/dashboard/connections", icon: Globe },
		{ label: "Sessions", href: "/dashboard/sessions", icon: Activity },
		{
			label: "Subscriptions",
			href: "/dashboard/subscriptions",
			icon: BookMarked,
		},
		{ label: "Cluster", href: "/dashboard/cluster", icon: Network },
		{ label: "Broker Info", href: "/dashboard/broker-info", icon: Info },
	];

	const isActive = (href: string) => {
		if (href === "/dashboard") {
			return pathname === "/dashboard";
		}
		return pathname?.startsWith(href);
	};

	return (
		<div className="flex h-screen bg-flux-bg text-flux-text">
			{/* Sidebar */}
			<aside
				className={`${
					sidebarOpen
						? "flex w-64 translate-x-0"
						: "hidden lg:flex lg:w-20 lg:translate-x-0"
				} bg-flux-card border-r border-flux-card-border transition-all duration-300 flex-col fixed h-screen z-40 lg:relative shadow-md`}
			>
				{/* Sidebar Header */}
				<div
					className={`p-4 border-b border-flux-card-border flex items-center ${sidebarOpen ? "justify-between" : "justify-center"}`}
				>
					{sidebarOpen && <FluxLogo className="text-2xl font-bold" />}
					<div className="flex items-center gap-1">
						<Button
							variant="ghost"
							size="icon"
							onClick={toggleTheme}
							className={`text-flux-text-muted hover:text-flux-text hover:bg-flux-hover ${
								sidebarOpen ? "hidden lg:inline-flex" : ""
							}`}
							aria-label="Toggle theme"
						>
							{theme === "light" ? <Moon size={18} /> : <Sun size={18} />}
						</Button>
						<Button
							variant="ghost"
							size="icon"
							onClick={() => setSidebarOpen(!sidebarOpen)}
							className="text-flux-text hover:bg-flux-hover"
							aria-label="Toggle sidebar"
						>
							{sidebarOpen ? <X size={20} /> : <Menu size={20} />}
						</Button>
					</div>
				</div>

				{/* Navigation */}
				<nav className="flex-1 p-4 space-y-2 overflow-y-auto">
					{navigationItems.map((item) => {
						const Icon = item.icon;
						const active = isActive(item.href);
						return (
							<Link
								key={item.href}
								href={item.href}
								className={`flex items-center gap-3 px-4 py-3 rounded-lg transition-colors ${
									active
										? "bg-flux-blue text-white"
										: "text-flux-text-muted hover:bg-flux-hover hover:text-flux-text"
								}`}
							>
								<Icon size={20} />
								{sidebarOpen && <span>{item.label}</span>}
							</Link>
						);
					})}
				</nav>
			</aside>

			{/* Mobile Overlay */}
			{sidebarOpen && (
				<button
					type="button"
					className="fixed inset-0 bg-black/50 lg:hidden z-30"
					onClick={() => setSidebarOpen(false)}
					aria-label="Close sidebar"
				/>
			)}

			{/* Main Content */}
			<main className="flex-1 overflow-auto">
				{/* Top Bar for Mobile */}
				<div className="lg:hidden bg-flux-card border-b border-flux-card-border p-4 flex items-center justify-between shadow-sm">
					<div className="flex items-center gap-2">
						<Button
							variant="ghost"
							size="icon"
							onClick={() => setSidebarOpen(!sidebarOpen)}
							className="hover:bg-flux-hover"
							aria-label={sidebarOpen ? "Close sidebar" : "Open sidebar"}
						>
							{sidebarOpen ? <X size={20} /> : <Menu size={20} />}
						</Button>
						<h1 className="text-xl font-bold bg-gradient-to-r from-flux-blue to-flux-orange bg-clip-text text-transparent">
							FluxMQ
						</h1>
					</div>
					<Button
						variant="ghost"
						size="icon"
						onClick={toggleTheme}
						className="hover:bg-flux-hover"
						aria-label="Toggle theme"
					>
						{theme === "light" ? <Moon size={20} /> : <Sun size={20} />}
					</Button>
				</div>
				{children}
			</main>
		</div>
	);
};

export default DashboardLayout;
