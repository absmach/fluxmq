# Publishing images and video (maintainers only)

Images and video are no longer committed to this repo. They're stored in a shared
Cloudflare R2 bucket (`websites-images`, key prefix `fluxmq-docs`) and served at their
usual `/docs/fluxmq/...` URLs by a small Worker that sits in front of the static export.

The docs site is a fully static Next.js export (`next build` with `output: "export"`) —
there's no Next.js server at request time, so this can't be a Next.js route handler the
way it is on the main website. Instead, [`worker.ts`](../worker.ts) is the Cloudflare
Worker configured as `main` in [`wrangler.jsonc`](../wrangler.jsonc). Cloudflare's default
routing serves any request that matches a file in the static asset manifest (`out/`)
directly, without invoking this Worker at all — the Worker only runs for requests with no
matching asset. Since images live only in R2 now, that's exactly the requests this Worker
needs to handle: it looks the path up in `IMAGES_BUCKET` and streams the object back, or
falls through to `env.ASSETS.fetch(request)` (which reproduces whatever the site's normal
"not found" behavior is) if nothing matches there either. Pages, JS, CSS, `sitemap.xml`,
`robots.txt`, and the Next.js-managed icon files under `app/` are untouched — they still
ship inside `out/` and keep being served directly, exactly as before this change.

Nothing in `content/`, `components/`, or `app/` changes — an image reference like
`/docs/fluxmq/getting-started/hero.webp` keeps working exactly as before, whether it's a
literal path in an MDX file or written as `assetPath("/getting-started/hero.webp")` (see
[`lib/base-path.ts`](../lib/base-path.ts) and the `img` override in
[`mdx-components.tsx`](../mdx-components.tsx), which both already add the `/docs/fluxmq`
prefix automatically).

Only maintainers publish images, using [`publish-image.mjs`](./publish-image.mjs). The
script is safe to have in a public repo because it's inert without a token — nobody can
upload to the bucket just by reading this file. See "Why maintainer-only" below for the
reasoning.

## One-time setup

1. Create `scripts/.env.publish-image` from the template:

   ```bash
   cp scripts/.env.publish-image.example scripts/.env.publish-image
   ```

2. Create a Cloudflare API token: dashboard -> **My Profile -> API Tokens -> Create Token
   -> Custom Token**, with both permissions on the same token:
   - `Workers R2 Storage: Edit`
   - `Zone -> Cache Purge -> Purge`, **Zone Resources** scoped to the `absmach.eu` zone
     (the docs site is served from `https://www.absmach.eu/docs/fluxmq`, the same zone as
     the main website)

3. Paste the token into `CLOUDFLARE_API_TOKEN` in `scripts/.env.publish-image`. The zone
   ID is already filled in (it's not secret, safe to share/commit — it can't authenticate
   anything by itself, and it's the same zone ID used by the main absmach-website repo).

4. Sanity-check the token before first use:

   ```bash
   curl -s https://api.cloudflare.com/client/v4/user/tokens/verify \
     -H "Authorization: Bearer $CLOUDFLARE_API_TOKEN"
   ```

   Should return `"status":"active"`. If it doesn't, the token value itself is wrong
   (bad copy/paste, expired, revoked) — fix that before troubleshooting anything else.

`scripts/.env.publish-image` is gitignored. Never commit it, never paste the token value
into a PR, issue, or chat.

## Publishing an image or video

```bash
pnpm run publish-image <local-file> <public-path>
```

`<public-path>` is everything after the domain in the final URL — it must start with
`docs/fluxmq/` (the site's Next.js `basePath`) so the script knows the object belongs to
this property. Examples:

```bash
pnpm run publish-image ./hero.webp docs/fluxmq/getting-started/hero.webp
# -> https://www.absmach.eu/docs/fluxmq/getting-started/hero.webp

pnpm run publish-image ./demo.mp4 docs/fluxmq/concepts/demo.mp4
# -> https://www.absmach.eu/docs/fluxmq/concepts/demo.mp4
```

The script does two things, in order:

1. `wrangler r2 object put ... --remote` — uploads to the **real** bucket, under
   `websites-images/fluxmq-docs/<path-after-docs/fluxmq/>`. `--remote` is required;
   without it, `wrangler` silently writes to a local simulated bucket and prints a
   normal-looking "Upload complete" with no error, and the object is never actually live.
2. Purges that exact URL from Cloudflare's edge cache (`POST /zones/{id}/purge_cache`), so
   the update is visible within seconds instead of waiting out the cache TTL.

If you re-run the same command for an existing path, it overwrites the object in place and
purges again — that's the intended way to update an image without changing its URL.

## Why maintainer-only

This repo is public. The risk isn't the script being visible — it's inert without a
credential. The risk is _credential distribution_: whoever holds `CLOUDFLARE_API_TOKEN`
can write to the shared bucket (which other properties also publish to). So nobody,
internal or external, gets a personal R2 token. Only a maintainer, holding this one scoped
token, runs `publish-image`.

Practical flow for a PR that adds a doc image (contributor is internal or external,
doesn't matter): the contributor attaches the image to the PR the normal GitHub way
(drag-and-drop into the description or a comment). A maintainer reviewing the PR runs
`pnpm run publish-image` locally before merging, then approves. If this becomes a frequent
bottleneck, the natural next step is a label- or comment-triggered GitHub Action that runs
the same script with the token stored as a repo secret — but that automation must only ever
read the attachment URL/destination path from the PR, never execute code from the PR
branch while the token is in scope (the standard `pull_request_target` secret-exfiltration
pitfall).

## Troubleshooting

- **`Local file not found: --`** — you ran `pnpm run publish-image -- <file> <dest>`. pnpm
  forwards a leading `--` to the script literally instead of stripping it like npm does.
  The script strips it defensively now, but plain `pnpm run publish-image <file> <dest>`
  (no `--`) is the form to use.
- **`Destination must start with "docs/fluxmq/"`** — the second argument must be the full
  public path including the site's basePath, e.g. `docs/fluxmq/logo.png`, not `logo.png`.
- **`Resource location: local` in the upload output** — means `--remote` didn't get
  applied for some reason (e.g. running the underlying `wrangler` command by hand without
  copying the full flag list from the script). The object was never written to the real
  bucket even though the CLI reports success. Always use `pnpm run publish-image`, or add
  `--remote` yourself if invoking wrangler directly.
- **`Cache purge failed` / `Authentication error` (code 10000)** — Cloudflare reuses this
  code for both "bad token" and "token valid but missing this permission." Run the token
  verify curl command above first to rule out a bad token. If that succeeds, the token is
  missing `Zone -> Cache Purge -> Purge` for the `absmach.eu` zone, or that permission's
  Zone Resources selector doesn't include it — edit the token in the dashboard and add it.
- To confirm an object actually made it into the bucket after a `--remote` upload:

  ```bash
  wrangler r2 object get websites-images/fluxmq-docs/<path-after-docs/fluxmq/> --remote --file=/tmp/check
  ```
