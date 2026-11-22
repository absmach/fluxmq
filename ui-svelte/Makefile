.PHONY: build check dev demo

build:
	npm ci
	npm run check
	npm run build
	CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o fluxmq-console .

check:
	npm run check
	npm run build
	go test -race ./...

dev:
	npm run dev

demo: build
	./fluxmq-console -demo
