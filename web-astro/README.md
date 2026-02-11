# FluxMQ Web (Astro + Starlight)

This `web` folder is now an Astro static site with:

- Product landing page at `/`
- Documentation powered by Starlight at `/docs/`
- Built-in docs search (`Ctrl+K`) via Starlight + Pagefind
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
- `src/components/home/*.astro`: landing page components (Astro-only, no React)
- `src/content/docs/docs/*`: docs content shown under `/docs/*`
- `src/styles/global.css`: Tailwind entry and shared brand/component styles
- `src/styles/starlight.css`: docs-theme overrides for Starlight
- `astro.config.mjs`: Astro, Tailwind, Starlight, and sitemap config
