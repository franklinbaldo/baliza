.PHONY: build test lint install clean

build:
	go build -o bin/pncp-pp-cli ./cmd/pncp-pp-cli

test:
	go test ./...

lint:
	golangci-lint run

install:
	go install ./cmd/pncp-pp-cli

clean:
	rm -rf bin/

build-mcp:
	go build -o bin/pncp-pp-mcp ./cmd/pncp-pp-mcp

install-mcp:
	go install ./cmd/pncp-pp-mcp

build-all: build build-mcp
