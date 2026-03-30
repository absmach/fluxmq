# syntax=docker/dockerfile:1
FROM node:25.1.0-alpine AS base

ENV PNPM_HOME="/pnpm"
ENV PATH="$PNPM_HOME:$PATH"
ENV NEXT_TELEMETRY_DISABLED=1

RUN rm -f /usr/local/bin/yarn
RUN rm -rf /usr/local/bin/yarnpkg
RUN npm install -g corepack
RUN corepack enable
RUN apk add --no-cache libc6-compat

# Install dependencies
FROM base AS deps
WORKDIR /app
COPY pnpm-lock.yaml package.json ./
RUN --mount=type=cache,id=pnpm,target=/pnpm/store pnpm install --frozen-lockfile --prefer-offline

# Build
FROM base AS builder
WORKDIR /app
COPY --from=deps /app/node_modules ./node_modules
COPY . .
RUN pnpm run build

# Production image
FROM base AS runner
WORKDIR /app

RUN addgroup --system --gid 1001 nodejs \
    && adduser --system --uid 1001 nextjs

COPY --from=builder --chown=nextjs:nodejs /app/.next/standalone ./
COPY --from=builder --chown=nextjs:nodejs /app/.next/static ./.next/static

USER nextjs

ENV PORT=3001
ENV HOSTNAME="0.0.0.0"
EXPOSE 3001

CMD ["node", "server.js"]
