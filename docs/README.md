# FluxMQ Docs

Documentation site for [FluxMQ](https://github.com/absmach/fluxmq), built with [Fumadocs](https://fumadocs.dev) and Next.js.

The site is served under `/docs/fluxmq/`.

## Development

```bash
pnpm install
pnpm dev
```

Open http://localhost:3000/docs/fluxmq/ with your browser to see the result.

## Deployment

This site uses:

- **Next.js static export** — `next build` outputs static files to `out/`
- **Next.js `basePath`** — generates links and assets under `/docs/fluxmq`
- **Post-build nesting** — `scripts/nest-static-export.mjs` moves the export under `out/docs/fluxmq/` so Cloudflare static assets can serve it from the route prefix
- **A small custom Worker** (`worker.ts`, configured as `main` in `wrangler.jsonc`) sits in
  front of the static assets to serve images and video from R2 instead of the git repo —
  see [Images and video](#images-and-video) below. Cloudflare's default routing still
  serves any request matching a file in `out/` directly, without invoking this Worker, so
  pages/JS/CSS/etc. are unaffected

### Cloudflare build settings (Dashboard)

| Setting          | Value                         |
|------------------|-------------------------------|
| Build command    | `pnpm run build`              |
| Deploy command   | `npx wrangler deploy`         |
| Version command  | `npx wrangler versions upload` |
| Root directory   | `/docs`                       |

### Architecture

```mermaid
flowchart LR
  subgraph Build_and_Deploy
    A[Git push] --> B[Cloudflare build trigger]
    B --> C[pnpm run build]
    C --> D[next build - static export]
    D --> E[nest export under out/docs/fluxmq]
    B --> F[npx wrangler deploy: worker.ts + out/]
    E --> G[Cloudflare Worker + static assets]
    F --> G
  end

  subgraph Runtime_Request_Flow
    U[Browser request] --> H{Path matches a file in out/?}
    H -- yes --> J[Served directly from static assets]
    H -- no, worker.ts runs --> K{Image/video extension?}
    K -- yes --> L[(R2: websites-images/fluxmq-docs/...)]
    K -- no --> M[env.ASSETS.fetch fallback]
    J --> U
    L --> U
    M --> U
  end
```

## Images and video

Images and video referenced from `content/`, `app/`, or `components/` are **not**
committed to this repo. They live in a shared Cloudflare R2 bucket and are served back at
their normal `/docs/fluxmq/...` URLs by `worker.ts`. See
[`scripts/README.md`](./scripts/README.md) for how maintainers publish a new or updated
image.

## Environment Variables

Only one build variable is needed:

```env
NEXT_PUBLIC_BASE_URL=https://www.absmach.eu/docs/fluxmq
```

Set this as a Cloudflare build variable so it is embedded into the static output at build time.

## Project structure

| Path                         | Description                                |
|------------------------------|--------------------------------------------|
| `app/[[...slug]]/page.tsx`   | Docs page renderer                         |
| `app/llms-full.txt/route.ts` | LLM-readable full docs text                |
| `content/docs`               | MDX source files                           |
| `lib/source.ts`              | Fumadocs source adapter                    |
| `lib/layout.shared.tsx`      | Shared layout options                      |
| `scripts/generate-api-docs.mts` | Generates API docs from OpenAPI source  |
| `scripts/nest-static-export.mjs` | Moves static export under `/docs/fluxmq` |
| `worker.ts`                  | Cloudflare Worker: serves images/video from R2, falls back to static assets |
| `scripts/publish-image.mjs`  | Maintainer-only: uploads an image/video to R2 and purges the edge cache |

## Learn More

To learn more about Next.js and Fumadocs, take a look at the following
resources:

- [Next.js Documentation](https://nextjs.org/docs) - learn about Next.js
  features and API.
- [Learn Next.js](https://nextjs.org/learn) - an interactive Next.js tutorial.
- [Fumadocs](https://fumadocs.dev) - learn about Fumadocs
