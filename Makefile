VERSION ?= dev
COMMIT ?= $(shell git rev-parse --short HEAD 2>/dev/null || printf unknown)
BUILD_DATE ?= $(shell date -u +%Y-%m-%dT%H:%M:%SZ)
LDFLAGS := -s -w -X main.version=$(VERSION) -X main.commit=$(COMMIT) -X main.buildDate=$(BUILD_DATE)

.PHONY: build release test

build:
	CGO_ENABLED=0 go build -trimpath -ldflags='$(LDFLAGS)' -o xsync-client ./cmd/xsync-client

release:
	mkdir -p dist
	CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -trimpath -ldflags='$(LDFLAGS)' -o dist/xsync-client-linux-amd64 ./cmd/xsync-client
	CGO_ENABLED=0 GOOS=linux GOARCH=arm64 go build -trimpath -ldflags='$(LDFLAGS)' -o dist/xsync-client-linux-arm64 ./cmd/xsync-client

test:
	go test ./...
	go test -race ./...
	go vet ./...
