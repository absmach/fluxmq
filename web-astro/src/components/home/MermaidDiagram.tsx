"use client";

import { useEffect, useRef, useState } from "react";
import mermaid from "mermaid";

interface MermaidDiagramProps {
  chart: string;
}

const isDarkTheme = () => {
  const root = document.documentElement;
  return root.classList.contains("dark") || root.dataset.theme === "dark";
};

export function MermaidDiagram({ chart }: MermaidDiagramProps) {
  const ref = useRef<HTMLDivElement>(null);
  const [dark, setDark] = useState(false);

  useEffect(() => {
    setDark(isDarkTheme());

    const observer = new MutationObserver(() => {
      setDark(isDarkTheme());
    });

    observer.observe(document.documentElement, {
      attributes: true,
      attributeFilter: ["class", "data-theme"],
    });

    return () => observer.disconnect();
  }, []);

  useEffect(() => {
    mermaid.initialize({
      startOnLoad: true,
      theme: "base",
      themeVariables: {
        primaryColor: "#2F69B3",
        primaryTextColor: "#ffffff",
        primaryBorderColor: dark ? "#444444" : "#333333",
        lineColor: dark ? "#cccccc" : "#666666",
        secondaryColor: "#F9A32A",
        tertiaryColor: dark ? "#1a1a1a" : "#ffffff",
        background: dark ? "#1a1a1a" : "#f9f9f9",
        mainBkg: "#1a1a1a",
        textColor: dark ? "#ffffff" : "#1a1a1a",
        fontSize: "14px",
        fontFamily: "'JetBrains Mono', 'Courier New', monospace",
      },
    });

    const current = ref.current;
    if (!current) return;

    const id = `mermaid-${Math.random().toString(36).slice(2, 11)}`;
    let cancelled = false;

    mermaid.render(id, chart).then(({ svg }) => {
      if (!cancelled && current) current.innerHTML = svg;
    });

    return () => {
      cancelled = true;
    };
  }, [chart, dark]);

  return (
    <div
      ref={ref}
      className="my-8 flex items-center justify-center overflow-x-auto brutalist-border bg-theme p-8"
    />
  );
}
