MODULE := gdisw/metric-store
IMAGE ?= otlp-metrics-server:dev

.PHONY: build run test test-integration test-all fmt vet lint tidy clean docker-build docker-run

build:
	go build ./...

run:
	go run ./cmd/server --store=clickhouse

run-memory:
	go run ./cmd/server --store=memory

test:
	go test -count=1 ./...

test-integration:
	go test -tags integration -count=1 -v ./...

test-all: test test-integration

fmt:
	go fmt ./...

vet:
	go vet ./...

lint: vet
	@if command -v staticcheck >/dev/null 2>&1; then \
		staticcheck ./...; \
	else \
		echo "staticcheck not installed, skipping (go install honnef.co/go/tools/cmd/staticcheck@latest)"; \
	fi

tidy:
	go mod tidy

clean:
	go clean ./...

docker-build:
	docker build -t $(IMAGE) .

docker-run:
	docker run --rm -p 4317:4317 -e STORE=memory $(IMAGE)
