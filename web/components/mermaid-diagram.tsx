"use client";

import { useEffect, useRef } from "react";
import { useTheme } from "next-themes";
import mermaid from "mermaid";

interface MermaidDiagramProps {
  chart: string;
}

export function MermaidDiagram({ chart }: MermaidDiagramProps) {
  const ref = useRef<HTMLDivElement>(null);
  const { theme } = useTheme();

  useEffect(() => {
    const isDark = theme === "dark";

    // Initialize mermaid with theme-aware absolute colors
    mermaid.initialize({
      startOnLoad: true,
      theme: "base",
      themeVariables: {
        primaryColor: "#2F69B3", // Flux Blue
        primaryTextColor: "#ffffff",
        primaryBorderColor: isDark ? "#444444" : "#333333",
        lineColor: isDark ? "#cccccc" : "#666666",
        secondaryColor: "#F9A32A", // Flux Orange
        tertiaryColor: isDark ? "#1a1a1a" : "#ffffff",
        background: isDark ? "#1a1a1a" : "#f9f9f9",
        mainBkg: "#1a1a1a",
        textColor: isDark ? "#ffffff" : "#1a1a1a",
        fontSize: "14px",
        fontFamily: "'JetBrains Mono', 'Courier New', monospace",
      },
    });

    if (ref.current) {
      // Create unique ID for this diagram
      const id = `mermaid-${Math.random().toString(36).substr(2, 9)}`;

      // Render the diagram
      mermaid.render(id, chart).then(({ svg }) => {
        if (ref.current) {
          ref.current.innerHTML = svg;
        }
      });
    }
  }, [chart, theme]);

  return (
    <div
      ref={ref}
      className="flex justify-center items-center my-8 brutalist-border bg-theme p-8 overflow-x-auto"
    />
  );
}
