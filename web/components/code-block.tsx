"use client";

import { useState } from "react";
import { Copy, Check } from "lucide-react";

interface CodeBlockProps {
  code: string;
  language?: string;
}

export function CodeBlock({ code, language = "bash" }: CodeBlockProps) {
  const [copied, setCopied] = useState(false);

  const handleCopy = async () => {
    await navigator.clipboard.writeText(code);
    setCopied(true);
    setTimeout(() => setCopied(false), 2000);
  };

  return (
    <div className="relative terminal p-6 mb-4 group">
      <button
        onClick={handleCopy}
        className="absolute top-4 right-4 p-2 hover:bg-(--flux-orange) hover:text-white transition-colors brutalist-border opacity-70 group-hover:opacity-100"
        aria-label="Copy code"
      >
        {copied ? <Check size={16} /> : <Copy size={16} />}
      </button>
      <pre className="overflow-x-auto">
        <code className="text-sm md:text-base">{code}</code>
      </pre>
    </div>
  );
}
