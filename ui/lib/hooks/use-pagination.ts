import { useCallback, useEffect, useState } from "react";

export function usePagination<T>(items: T[], pageSize = 10) {
	const [page, setPage] = useState(1);

	const totalPages = Math.max(1, Math.ceil(items.length / pageSize));

	useEffect(() => {
		if (page > totalPages) setPage(1);
	}, [page, totalPages]);

	const pageItems = items.slice((page - 1) * pageSize, page * pageSize);

	const goTo = useCallback((p: number) => setPage(p), []);
	const reset = useCallback(() => setPage(1), []);

	return { page, totalPages, pageItems, goTo, reset };
}
