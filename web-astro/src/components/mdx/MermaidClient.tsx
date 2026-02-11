"use client";

import { useEffect, useRef } from "react";

interface MermaidClientProps {
  chart: string;
}

const resolveTheme = () => {
  const root = document.documentElement;
  return root.classList.contains("dark") || root.dataset.theme === "dark"
    ? "dark"
    : "default";
};

export default function MermaidClient({ chart }: MermaidClientProps) {
  const ref = useRef<HTMLDivElement>(null);
  const idRef = useRef(`mermaid-${Math.random().toString(36).slice(2, 10)}`);

  useEffect(() => {
    let cancelled = false;
    let observer: MutationObserver | undefined;

    const renderMermaid = async () => {
      const target = ref.current;
      if (!target) return;

      const mermaid = (await import("mermaid")).default;
      mermaid.initialize({
        startOnLoad: false,
        securityLevel: "loose",
        fontFamily: "inherit",
        theme: resolveTheme(),
      });

      const { svg } = await mermaid.render(
        idRef.current,
        chart.replaceAll("\\n", "\n"),
      );

      if (!cancelled && ref.current) {
        ref.current.innerHTML = svg;
      }
    };

    void renderMermaid();

    observer = new MutationObserver(() => {
      void renderMermaid();
    });

    observer.observe(document.documentElement, {
      attributes: true,
      attributeFilter: ["class", "data-theme"],
    });

    return () => {
      cancelled = true;
      observer?.disconnect();
    };
  }, [chart]);

  return <div ref={ref} className="my-6 overflow-x-auto [&_svg]:mx-auto" />;
}
