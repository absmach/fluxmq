import { source } from "@/lib/source";
import { DocsLayout } from "fumadocs-ui/layouts/docs";
import { baseOptions } from "@/lib/layout.shared";
import type { ReactNode } from "react";

export default function Layout({ children }: { children: ReactNode }) {
  const base = baseOptions();
  return (
    <DocsLayout
      {...base}
      tree={source.getPageTree()}
      links={base.links?.filter((item) => item.type === "icon")}
      nav={{
        ...base.nav,
      }}
    >
      {children}
    </DocsLayout>
  );
}
