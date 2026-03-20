"use client";

import dynamic from "next/dynamic";

const NodeNetwork = dynamic(
  () => import("@/components/node-network").then((mod) => mod.NodeNetwork),
  {
    ssr: false,
    loading: () => (
      <div className="h-[80vh] w-full brutalist-border bg-theme-alt animate-pulse" />
    ),
  },
);

export function HeroNodeNetwork() {
  return <NodeNetwork />;
}
