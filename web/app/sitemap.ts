import fs from "node:fs";
import path from "node:path";
import type { MetadataRoute } from "next";
import { source } from "@/lib/source";

const DOMAIN = process.env.NEXT_PUBLIC_SITE_URL || "https://fluxmq.absmach.eu";

export const dynamic = "force-static";

function collectPages(dir: string, route = ""): MetadataRoute.Sitemap {
  return fs.readdirSync(dir, { withFileTypes: true }).flatMap((entry) => {
    const fullPath = path.join(dir, entry.name);

    if (entry.isDirectory()) {
      if (entry.name.startsWith("[")) return [];
      return collectPages(fullPath, `${route}/${entry.name}`);
    }

    if (entry.name !== "page.tsx") return [];

    return [
      {
        url: DOMAIN + (route || "/"),
        lastModified: fs.statSync(fullPath).mtime,
      },
    ];
  });
}

export default function sitemap(): MetadataRoute.Sitemap {
  const pages = collectPages(path.join(process.cwd(), "app"));

  for (const page of source.getPages()) {
    pages.push({
      url: `${DOMAIN}${page.url}`,
      lastModified: new Date(),
    });
  }

  return pages;
}
