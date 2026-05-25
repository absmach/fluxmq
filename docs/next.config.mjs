import { createMDX } from "fumadocs-mdx/next";

const withMDX = createMDX();

const BASE_PATH = "/docs/fluxmq";

/** @type {import('next').NextConfig} */
const config = {
  output: "export",
  trailingSlash: true,
  reactStrictMode: true,
  images: {
    unoptimized: true,
  },
  basePath: BASE_PATH,
  assetPrefix: BASE_PATH,
  env: {
    NEXT_PUBLIC_BASE_PATH: BASE_PATH,
  },
};

export default withMDX(config);
