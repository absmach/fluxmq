export const BASE_PATH = process.env.NEXT_PUBLIC_BASE_PATH || "/docs/fluxmq";

export function withBasePath(path = "") {
  if (!path || path === "/") return BASE_PATH;
  return `${BASE_PATH}${path.startsWith("/") ? path : `/${path}`}`;
}

export function assetPath(path: string): string {
  if (BASE_PATH && path.startsWith(BASE_PATH)) return path;
  return withBasePath(path);
}

export function toSiteUrl(path: string): string {
  const base =
    process.env.NEXT_PUBLIC_BASE_URL || "https://www.absmach.eu/docs/fluxmq";
  const normalizedBase = base.replace(/\/$/, "");

  if (path.startsWith(BASE_PATH)) {
    return new URL(path, new URL(base).origin).toString();
  }

  return `${normalizedBase}${path.startsWith("/") ? path : `/${path}`}`;
}
