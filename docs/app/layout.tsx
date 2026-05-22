import type { Metadata } from "next";
import { Provider } from "@/components/provider";
import { DocsLayout } from "fumadocs-ui/layouts/docs";
import { baseOptions } from "@/lib/layout.shared";
import { source } from "@/lib/source";
import "./global.css";

export const metadata: Metadata = {
  metadataBase: new URL("https://www.absmach.eu/products/fluxmq/"),
  title: {
    default: "FluxMQ Docs",
    template: "%s | FluxMQ Docs",
  },
  description:
    "FluxMQ is a high-performance, multi-protocol message broker written in Go. Supports MQTT 3.1.1/5.0, WebSocket, HTTP and CoAP with durable queues, clustering, and event-driven architecture.",
  keywords: [
    "FluxMQ",
    "MQTT broker",
    "message broker",
    "IoT",
    "high-performance",
    "durable queues",
    "clustering",
    "event-driven",
    "open-source",
    "Go",
  ],
  authors: [{ name: "Abstract Machines" }],
  robots: {
    index: true,
    follow: true,
  },
};

export default function Layout({ children }: LayoutProps<"/">) {
  const base = baseOptions();
  return (
    <html lang="en" suppressHydrationWarning>
      <body className="flex flex-col min-h-screen">
        <Provider>
          <DocsLayout
            {...base}
            tree={source.getPageTree()}
            links={base.links?.filter((item) => item.type === "icon")}
            nav={{ ...base.nav }}
          >
            {children}
          </DocsLayout>
        </Provider>
      </body>
    </html>
  );
}
