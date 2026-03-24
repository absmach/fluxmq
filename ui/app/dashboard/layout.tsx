"use client";

import {
	BookOpen,
	Cable,
	Gauge,
	LayoutDashboard,
	Mail,
	Menu,
	Moon,
	PanelLeftClose,
	PanelLeftOpen,
	Rss,
	Sun,
	X,
} from "lucide-react";
import Link from "next/link";
import { usePathname } from "next/navigation";
import { useTheme } from "next-themes";
import type React from "react";
import { useEffect, useState } from "react";
import { FluxLogo } from "@/components/flux-logo";
import { Button } from "@/components/ui/button";
import {
	Tooltip,
	TooltipContent,
	TooltipProvider,
	TooltipTrigger,
} from "@/components/ui/tooltip";

const DashboardLayout = ({ children }: { children: React.ReactNode }) => {
	const [sidebarOpen, setSidebarOpen] = useState(true);
	const [mounted, setMounted] = useState(false);
	const pathname = usePathname();
	const { resolvedTheme, setTheme } = useTheme();

	useEffect(() => {
		setMounted(true);
		const mediaQuery = window.matchMedia("(min-width: 1024px)");
		const syncSidebar = () => setSidebarOpen(mediaQuery.matches);
		syncSidebar();
		mediaQuery.addEventListener("change", syncSidebar);
		return () => mediaQuery.removeEventListener("change", syncSidebar);
	}, []);

	const navigationItems = [
		{ label: "Overview", href: "/dashboard", icon: LayoutDashboard },
		{ label: "Sessions", href: "/dashboard/sessions", icon: Cable },
		{
			label: "Subscriptions",
			href: "/dashboard/subscriptions",
			icon: Rss,
		},
		{ label: "Health", href: "/dashboard/broker-info", icon: Gauge },
	];

	const isActive = (href: string) => {
		if (href === "/dashboard") {
			return pathname === "/dashboard";
		}
		return pathname?.startsWith(href);
	};

	return (
		<div className="flex h-screen overflow-hidden bg-flux-bg text-flux-text">
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
					<button
						type="button"
						onClick={() => setSidebarOpen(!sidebarOpen)}
						aria-label={sidebarOpen ? "Collapse sidebar" : "Expand sidebar"}
						className="hidden lg:flex h-8 w-8 items-center justify-center rounded-lg text-flux-text-muted hover:bg-flux-hover hover:text-flux-text transition-colors"
					>
						{sidebarOpen ? (
							<PanelLeftClose size={16} />
						) : (
							<PanelLeftOpen size={16} />
						)}
					</button>
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
								aria-label={item.label}
								aria-current={active ? "page" : undefined}
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

				{/* Footer */}
				<div className="border-t border-flux-card-border p-4">
					<TooltipProvider delayDuration={300}>
						<div className={`flex items-center ${sidebarOpen ? "justify-between" : "justify-center"}`}>
							{sidebarOpen && (
								<div className="flex items-center gap-1">
									<Tooltip>
										<TooltipTrigger asChild>
											<a
												href="https://fluxmq.absmach.eu/docs"
												target="_blank"
												rel="noopener noreferrer"
												aria-label="Documentation"
												className="flex h-8 w-8 items-center justify-center rounded-lg text-flux-text-muted hover:bg-flux-hover hover:text-flux-text transition-colors"
											>
												<BookOpen size={18} />
											</a>
										</TooltipTrigger>
										<TooltipContent side="top">Documentation</TooltipContent>
									</Tooltip>

									<Tooltip>
										<TooltipTrigger asChild>
											<a
												href="mailto:info@absmach.eu"
												aria-label="Contact"
												className="flex h-8 w-8 items-center justify-center rounded-lg text-flux-text-muted hover:bg-flux-hover hover:text-flux-text transition-colors"
											>
												<Mail size={18} />
											</a>
										</TooltipTrigger>
										<TooltipContent side="top">Contact</TooltipContent>
									</Tooltip>
								</div>
							)}

							<Tooltip>
								<TooltipTrigger asChild>
									<Button
										variant="ghost"
										size="icon"
										onClick={() =>
											setTheme(resolvedTheme === "dark" ? "light" : "dark")
										}
										className="h-8 w-8 hover:bg-flux-hover"
										aria-label="Toggle theme"
									>
										{mounted ? (
											resolvedTheme === "light" ? (
												<Moon size={16} />
											) : (
												<Sun size={16} />
											)
										) : (
											<span className="inline-block h-4 w-4" />
										)}
									</Button>
								</TooltipTrigger>
								<TooltipContent side="top">
									{mounted
										? resolvedTheme === "light"
											? "Switch to dark mode"
											: "Switch to light mode"
										: "Toggle theme"}
								</TooltipContent>
							</Tooltip>
						</div>
					</TooltipProvider>
				</div>
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
			<main className="relative flex-1 overflow-y-auto">
				{/* Top Bar */}
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
						<FluxLogo className="text-xl font-bold" />
					</div>
					<Button
						variant="ghost"
						size="icon"
						onClick={() =>
							setTheme(resolvedTheme === "dark" ? "light" : "dark")
						}
						className="hover:bg-flux-hover"
						aria-label="Toggle theme"
					>
						{mounted ? (
							resolvedTheme === "light" ? (
								<Moon size={20} />
							) : (
								<Sun size={20} />
							)
						) : (
							<span className="inline-block h-5 w-5" />
						)}
					</Button>
				</div>
				{children}
			</main>
		</div>
	);
};

export default DashboardLayout;
