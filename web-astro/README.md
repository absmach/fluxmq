# FluxMQ Web (Astro + Fumadocs)

This `web-astro` folder is an Astro static site with:

- Product landing page at `/`
- Documentation powered by Fumadocs at `/docs`
- Built-in docs search (`Ctrl+K`)
- Tailwind CSS v4 for styling

## Development

```bash
npm install
npm run dev
```

or

```bash
pnpm install
pnpm dev
```

Open `http://localhost:4321`.

## Build

```bash
npm run build
npm run preview
```

or

```bash
pnpm build
pnpm preview
```

## Structure

- `src/pages/index.astro`: product landing page
- `src/pages/docs/[...slug].astro`: docs route renderer
- `src/components/docs.tsx`: Fumadocs docs layout wrapper
- `src/components/home/*.astro`: landing page components
- `src/content/docs/*`: docs content shown under `/docs/*`
- `src/lib/source.ts`: Fumadocs page tree/content source
- `src/styles/global.css`: Tailwind entry and shared styles
- `astro.config.mjs`: Astro + MDX + Fumadocs markdown pipeline config

## CSS Customizations (Astro)

All custom styling is centralized in `src/styles/global.css`.

### 1. Theme tokens and brand variables

- Defines FluxMQ color tokens in `:root` and dark theme overrides in `:root[class~='dark'], :root[data-theme='dark']`.
- Includes brand values used across both landing page and docs:
  - `--flux-blue`, `--flux-orange`
  - `--flux-bg`, `--flux-text`, `--flux-border`
  - nav/search-specific tokens like `--flux-header-*`, `--flux-pill-*`, `--flux-icon`

### 2. Base typography and utility styles

- Global body font + theme-aware text/background.
- Shared typography helpers:
  - paragraph sizing (`p`)
  - `.mono` for JetBrains Mono
- Shared visual helpers:
  - `.grid-pattern`, `.brutalist-border`, `.brutalist-card`, `.terminal`
  - table helpers (`.metrics-table`, `.metric-head`, `.metric-cell`)
  - color utilities (`.bg-theme`, `.bg-theme-alt`, `.text-theme-muted`, `.border-theme`)

### 3. Component layer styles

Under `@layer components`, reusable classes were added for Astro home/docs UI:

- Header/nav:
  - `.site-header`, `.nav-link`, `.nav-search-pill`, `.kbd-chip`
  - `.theme-toggle-pill`, `.theme-dot`, `.theme-dot-active`
  - `.nav-icon-link`
- Home sections:
  - `.feature-card`, `.bullet-list`, `.cluster-box`
  - `.code-panel` with nested `pre`/`code` styling for terminal blocks

### 4. Motion and accessibility

- Keyframes and utility animations:
  - `fadeIn`, `slideUp`, `network-dash`
  - `.animate-fade-in`, `.animate-slide-up`
- Reduced motion support via `@media (prefers-reduced-motion: reduce)`.
- Global `:focus-visible` outline is customized to Flux orange.

### 5. Fumadocs-specific overrides

- Sidebar active state and hover color are customized with:
  - `#nd-sidebar [data-active='true']`
  - `#nd-sidebar a:hover`
- Markdown code block fallback rules were added to handle Astro `.md` pages rendered as raw Shiki markup:
  - `.prose pre.shiki`
  - `.prose pre.shiki code`
  - `.prose pre.shiki code .line`
  - `.prose pre > code` (resets inline-code prose styles from leaking into fenced blocks)

## Troubleshooting

### Code blocks in docs look wrong (each line looks boxed/highlighted)

If fenced code blocks in `.md` pages render with broken styling:

1. Ensure `astro.config.mjs` applies Fumadocs `rehypeCode` to both Markdown and MDX:
   - `markdown.syntaxHighlight = false`
   - `markdown.rehypePlugins = [rehypeCode]`
   - `mdx({ syntaxHighlight: false, rehypePlugins: [rehypeCode], ... })`
2. Restart the dev server after config changes.
3. Hard refresh the browser.

Reason: without this, `.md` may use Astro default highlighting while `.mdx` uses Fumadocs highlighting, causing inconsistent code block styles.
