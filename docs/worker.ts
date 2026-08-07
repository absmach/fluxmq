// Custom Worker fronting the static export.
//
// The docs site is a fully static Next.js export (`output: "export"`) served
// by Cloudflare's Workers Static Assets — there's no Next.js server at
// request time, so a Next.js route handler can't do per-request work. This
// small hand-written Worker is the only place that can.
//
// Cloudflare's default routing (no `run_worker_first`) already serves any
// request that matches a file in the static asset manifest without
// invoking this Worker at all. This Worker only runs for requests with no
// matching asset — which, after removing images from `public/`, is exactly
// the image/video requests we want to serve from R2 instead. Everything
// else (pages, JS, CSS, sitemap.xml, favicon.ico, etc.) is untouched and
// keeps being served directly from the asset manifest as before.
//
// Shared bucket ("websites-images") holds assets for multiple properties;
// KEY_PREFIX keeps this site's objects from colliding with other
// properties in the same bucket.
import { BASE_PATH } from "./lib/base-path";

const KEY_PREFIX = "fluxmq-docs";

// Extensions this Worker will look up in R2. Anything else (including
// Next.js's own build-time icon files under app/, which stay bundled into
// the static export) never reaches this check because a matching asset
// already exists and Cloudflare serves it before the Worker runs.
const IMAGE_EXTENSION_RE = /\.(png|jpe?g|webp|gif|svg|avif|mp4|webm)$/i;

interface R2ObjectBody {
  body: ReadableStream;
  size: number;
  httpEtag: string;
  writeHttpMetadata(headers: Headers): void;
}

interface R2Bucket {
  get(key: string): Promise<R2ObjectBody | null>;
}

interface Env {
  ASSETS: { fetch(request: Request): Promise<Response> };
  IMAGES_BUCKET: R2Bucket;
}

const notFound = () =>
  new Response("Not found", {
    status: 404,
    headers: { "cache-control": "no-store" },
  });

function toObjectKey(pathname: string): string {
  const relativePath = pathname.startsWith(BASE_PATH)
    ? pathname.slice(BASE_PATH.length)
    : pathname;
  return `${KEY_PREFIX}${relativePath}`;
}

export default {
  async fetch(request: Request, env: Env): Promise<Response> {
    const { pathname } = new URL(request.url);

    if (IMAGE_EXTENSION_RE.test(pathname)) {
      const object = await env.IMAGES_BUCKET.get(toObjectKey(pathname));
      if (object) {
        const headers = new Headers();
        object.writeHttpMetadata(headers);
        headers.set("etag", object.httpEtag);
        headers.set("content-length", String(object.size));
        // Short browser TTL (revalidates quickly) + long edge TTL (until
        // purged explicitly by the publish-image script on upload).
        headers.set("cache-control", "public, max-age=300, s-maxage=31536000");
        return new Response(object.body, { headers });
      }
      return notFound();
    }

    return env.ASSETS.fetch(request);
  },
};
