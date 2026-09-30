.PHONY: run build fmt vet test up down deps-up logs

# Load .env if it exists, so `make run` works with local settings.
ifneq (,$(wildcard .env))
include .env
export
endif

run:            ## run the gateway on your machine (needs redis+postgres running)
	go run ./cmd/gateway

build:          ## build a binary into bin/
	go build -o bin/gateway ./cmd/gateway

fmt:
	gofmt -w .

vet:
	go vet ./...

test:
	go test -race ./...

up:             ## start everything (app + redis + postgres) in Docker
	docker compose up --build

down:           ## stop everything
	docker compose down

deps-up:        ## start only redis + postgres (run the app with `make run`)
	docker compose up -d postgres redis

logs:
	docker compose logs -f app
