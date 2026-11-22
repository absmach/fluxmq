FROM node:26.13.1-alpine AS frontend
WORKDIR /src
COPY package.json package-lock.json ./
RUN npm ci
COPY index.html vite.config.ts svelte.config.js tsconfig.json ./
COPY src ./src
COPY public ./public
RUN npm run check && npm run build

FROM golang:1.27.4-alpine AS backend
WORKDIR /src
COPY go.mod *.go ./
COPY --from=frontend /src/dist ./dist
RUN go test ./... && CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /fluxmq-console .

FROM alpine:3.24
RUN apk add --no-cache ca-certificates
COPY --from=backend /fluxmq-console /usr/local/bin/fluxmq-console
USER 65532:65532
EXPOSE 3002
ENTRYPOINT ["/usr/local/bin/fluxmq-console"]
CMD ["-listen", "0.0.0.0:3002"]
