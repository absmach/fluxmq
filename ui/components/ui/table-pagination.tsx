"use client";

import clsx from "clsx";
import {
	ChevronLeft,
	ChevronRight,
	ChevronsLeft,
	ChevronsRight,
} from "lucide-react";
import { type Dispatch, type SetStateAction, useId } from "react";
import { Button } from "@/components/ui/button";

interface TablePaginationProps {
	page: number;
	limit: number;
	totalPages: number;
	setPage: Dispatch<SetStateAction<number>>;
	setLimit: Dispatch<SetStateAction<number>>;
	totalItems: number;
	itemLabel?: string;
	pageSizeOptions?: number[];
}

const NavBtn = ({
	onClick,
	disabled,
	children,
}: {
	onClick: () => void;
	disabled: boolean;
	children: React.ReactNode;
}) => (
	<Button
		variant="outline"
		size="icon"
		onClick={onClick}
		disabled={disabled}
		className={clsx(
			"h-11 w-11 border-flux-card-border",
			disabled
				? "pointer-events-none text-flux-text-muted opacity-40"
				: "text-flux-text hover:bg-flux-hover",
		)}
	>
		{children}
	</Button>
);

export function TablePagination({
	page,
	limit,
	totalPages,
	setPage,
	setLimit,
	totalItems,
	itemLabel = "items",
	pageSizeOptions = [5, 10, 15, 25],
}: TablePaginationProps) {
	const rowsPerPageLabelId = useId();

	if (totalItems === 0) return null;

	const from = Math.min((page - 1) * limit + 1, totalItems);
	const to = Math.min(page * limit, totalItems);

	return (
		<div className="flex flex-wrap items-center justify-between gap-4 mt-4">
			{/* Left: count info */}
			<p className="text-xs text-flux-text-muted">
				Showing {from}–{to} of {totalItems} {itemLabel}
			</p>

			{/* Right: controls */}
			<div className="flex items-center gap-4">
				{/* Rows per page */}
				<div className="flex items-center gap-2">
					<span
						id={rowsPerPageLabelId}
						className="text-xs text-flux-text-muted whitespace-nowrap"
					>
						Rows per page
					</span>
					<select
						aria-labelledby={rowsPerPageLabelId}
						value={limit}
						onChange={(e) => {
							setLimit(Number(e.target.value));
							setPage(1);
						}}
						className="h-11 rounded-md border border-flux-card-border bg-flux-bg px-2 text-xs text-flux-text focus:outline-none focus:ring-1 focus:ring-flux-blue"
					>
						{pageSizeOptions.map((n) => (
							<option key={n} value={n}>
								{n}
							</option>
						))}
					</select>
				</div>

				{/* Page indicator */}
				<span className="text-xs text-flux-text-muted whitespace-nowrap">
					Page {page} of {totalPages}
				</span>

				{/* Navigation */}
				<div className="flex items-center gap-1">
					<NavBtn onClick={() => setPage(1)} disabled={page <= 1}>
						<ChevronsLeft className="h-4 w-4" />
						<span className="sr-only">First page</span>
					</NavBtn>
					<NavBtn onClick={() => setPage(page - 1)} disabled={page <= 1}>
						<ChevronLeft className="h-4 w-4" />
						<span className="sr-only">Previous page</span>
					</NavBtn>
					<NavBtn
						onClick={() => setPage(page + 1)}
						disabled={page >= totalPages}
					>
						<ChevronRight className="h-4 w-4" />
						<span className="sr-only">Next page</span>
					</NavBtn>
					<NavBtn
						onClick={() => setPage(totalPages)}
						disabled={page >= totalPages}
					>
						<ChevronsRight className="h-4 w-4" />
						<span className="sr-only">Last page</span>
					</NavBtn>
				</div>
			</div>
		</div>
	);
}
