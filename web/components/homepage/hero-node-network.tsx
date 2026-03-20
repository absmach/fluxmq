"use client";

import dynamic from "next/dynamic";

const NodeNetwork = dynamic(
  () => import("@/components/node-network").then((mod) => mod.NodeNetwork),
  {
    ssr: false,
    loading: () => <div className="h-full w-full" />,
  },
);

export function HeroNodeNetwork() {
  return <NodeNetwork />;
}
