# ---- Stage 1: build the binary ----
FROM golang:1.24-alpine AS build
WORKDIR /src

# Copy go.mod/go.sum first so Docker caches the dependency download layer.
COPY go.mod go.sum ./
RUN go mod download

COPY . .
# CGO_ENABLED=0 gives a static binary that runs on a tiny image.
RUN CGO_ENABLED=0 go build -ldflags="-s -w" -o /gateway ./cmd/gateway

# ---- Stage 2: small runtime image ----
FROM alpine:3.20
# ca-certificates: needed for HTTPS calls to Groq/Gemini/Neon/Upstash.
RUN apk add --no-cache ca-certificates && adduser -D -u 10001 app
USER app
WORKDIR /app
COPY --from=build /gateway /app/gateway
COPY --from=build /src/web /app/web

EXPOSE 8080
ENTRYPOINT ["/app/gateway"]
