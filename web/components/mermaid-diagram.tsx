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
    const palette = isDark
      ? {
          background: "#0f1116",
          nodeBackground: "#171c26",
          border: "#3c465c",
          edge: "#657089",
          text: "#e8edf6",
          edgeLabelBackground: "#0f1116",
        }
      : {
          background: "#ffffff",
          nodeBackground: "#f7f9fc",
          border: "#3a4458",
          edge: "#566179",
          text: "#111722",
          edgeLabelBackground: "#ffffff",
        };

    async function renderDiagram() {
      const { default: mermaid } = await import("mermaid");

      // Initialize mermaid with consistent contrast in both light and dark.
      mermaid.initialize({
        startOnLoad: false,
        theme: "base",
        themeVariables: {
          primaryColor: "#2F69B3",
          primaryTextColor: "#ffffff",
          primaryBorderColor: palette.border,
          lineColor: palette.edge,
          secondaryColor: "#F9A32A",
          tertiaryColor: palette.nodeBackground,
          background: palette.background,
          mainBkg: palette.nodeBackground,
          nodeBkg: palette.nodeBackground,
          nodeBorder: palette.border,
          nodeTextColor: palette.text,
          secondaryTextColor: palette.text,
          tertiaryTextColor: palette.text,
          textColor: palette.text,
          defaultLinkColor: palette.edge,
          edgeLabelBackground: palette.edgeLabelBackground,
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
