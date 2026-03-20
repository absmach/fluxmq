"use client";

import { useTheme } from "next-themes";
import { useEffect, useId, useRef } from "react";

interface MermaidDiagramProps {
  chart: string;
}

export function MermaidDiagram({ chart }: MermaidDiagramProps) {
  const ref = useRef<HTMLDivElement>(null);
  const { theme } = useTheme();
  const stableId = useId().replace(/:/g, "-");

  useEffect(() => {
    const isDark = theme === "dark";
    let cancelled = false;

    async function renderDiagram() {
      const { default: mermaid } = await import("mermaid");

      // Initialize mermaid with theme-aware absolute colors
      mermaid.initialize({
        startOnLoad: false,
        theme: "base",
        themeVariables: {
          primaryColor: "#2F69B3",
          primaryTextColor: isDark ? "#111318" : "#ffffff",
          primaryBorderColor: isDark ? "#3a3d45" : "#333333",
          lineColor: isDark ? "#b7bbc4" : "#666666",
          secondaryColor: "#F9A32A",
          tertiaryColor: isDark ? "#1a1a1a" : "#ffffff",
          background: isDark ? "#1a1a1a" : "#f9f9f9",
          mainBkg: isDark ? "#1a1a1a" : "#ffffff",
          textColor: isDark ? "#e8eaed" : "#0f172a",
          fontSize: "14px",
          fontFamily: "'JetBrains Mono', 'Courier New', monospace",
        },
      });

      if (ref.current) {
        const id = `mermaid-${stableId}`;
        const { svg } = await mermaid.render(id, chart);
        if (!cancelled && ref.current) {
          ref.current.innerHTML = svg;
        }
      }
    }

    void renderDiagram();
    return () => {
      cancelled = true;
    };
  }, [chart, stableId, theme]);

  return (
    <div
      ref={ref}
      className="flex justify-center items-center my-8 brutalist-border bg-theme p-8 overflow-x-auto"
    />
  );
}
