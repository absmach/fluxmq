import {
  defineConfig,
  defineDocs,
  frontmatterSchema,
  metaSchema,
} from "fumadocs-mdx/config";

// You can customise Zod schemas for frontmatter and `meta.json` here
// see https://fumadocs.dev/docs/mdx/collections
export const docs = defineDocs({
  dir: "content/docs",
  docs: {
    schema: frontmatterSchema,
    postprocess: {
      includeProcessedMarkdown: true,
    },
  },
  meta: {
    schema: metaSchema,
  },
});

export default defineConfig({
  mdxOptions: {
    // MDX options
    rehypeCodeOptions: {
      themes: {
        light: "github-light",
        dark: "github-dark",
      },
      // Preload every language used in the docs. Setting `langAlias` makes
      // fumadocs build a custom highlighter whose bundled-language set no longer
      // lazy-loads these on demand, which otherwise fails with
      // "Language `bash` not found" during the build.
      langs: ["go", "bash", "yaml", "json"],
      langAlias: {
        promql: "text",
      },
    },
  },
});
