import fs from "node:fs";
import path from "node:path";
import type { MetadataRoute } from "next";
import { toSiteUrl } from "@/lib/base-path";
import { source } from "@/lib/source";

export const dynamic = "force-static";

function collectPages(dir: string, route = ""): MetadataRoute.Sitemap {
  return fs.readdirSync(dir, { withFileTypes: true }).flatMap((entry) => {
    const fullPath = path.join(dir, entry.name);

    if (entry.isDirectory()) {
      if (entry.name.startsWith("[")) return [];
      const segment = entry.name.startsWith("(")
        ? route
        : `${route}/${entry.name}`;
      return collectPages(fullPath, segment);
    }

    if (entry.name !== "page.tsx") return [];

    return [
      {
        url: toSiteUrl(route || "/"),
        lastModified: fs.statSync(fullPath).mtime,
      },
    ];
  });
}

export default function sitemap(): MetadataRoute.Sitemap {
  const pages = collectPages(path.join(process.cwd(), "app"));

  for (const page of source.getPages()) {
    pages.push({
      url: toSiteUrl(page.url),
      lastModified: new Date(),
    });
  }

  return pages;
}
