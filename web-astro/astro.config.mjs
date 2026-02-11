import { defineConfig } from 'astro/config';
import mdx from '@astrojs/mdx';
import react from '@astrojs/react';
import sitemap from '@astrojs/sitemap';
import tailwindcss from '@tailwindcss/vite';
import {
  rehypeCode,
  remarkCodeTab,
  remarkHeading,
  remarkNpm,
  remarkStructure,
} from 'fumadocs-core/mdx-plugins';

const site = process.env.NEXT_PUBLIC_SITE_URL ?? 'https://fluxmq.absmach.eu';

export default defineConfig({
  site,
  base: process.env.FQ_BASE_PATH ?? '/',
  output: 'static',
  markdown: {
    syntaxHighlight: false,

    remarkPlugins: [
      remarkHeading,
      remarkCodeTab,
      remarkNpm,
      [remarkStructure, { exportAs: "structuredData" }],
    ],
    rehypePlugins: [rehypeCode],
  },
  integrations: [
    sitemap(),
    react(),
    mdx({
      extendMarkdownConfig: false,
      syntaxHighlight: false,
      remarkPlugins: [
        remarkHeading,
        remarkCodeTab,
        remarkNpm,
        [remarkStructure, { exportAs: 'structuredData' }],
      ],
      rehypePlugins: [rehypeCode],
    }),
  ],
  vite: {
    plugins: [tailwindcss()],
  },
});
