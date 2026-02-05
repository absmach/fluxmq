import { source } from "@/lib/source";
import { DocsLayout } from "fumadocs-ui/layouts/docs";
import { baseOptions } from "@/lib/layout.shared";
import type { ReactNode } from "react";

export default function Layout({ children }: { children: ReactNode }) {
  const base = baseOptions();
  return (
    // @ts-expect-error - fumadocs type definitions don't match implementation for children prop
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
