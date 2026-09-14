# Multi-stage build for the knowledge-base gateway.
#
# The builder is pinned to the Go version declared by go.mod; the runtime
# stage is a minimal Alpine image running as a dedicated non-root user.
# Nothing secret is copied into either stage: provider credentials, DSNs, and
# API keys are injected at run time by the deployment environment (see
# docker-compose.yml), never at build time.
#
# syntax=docker/dockerfile:1

# --- Build stage -----------------------------------------------------------
# Keep in sync with the `go` directive in go.mod.
FROM golang:1.27.1-alpine AS builder

WORKDIR /src

# Download modules first so dependency layers cache independently of source.
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod \
    go mod download

COPY . .

RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/gateway ./cmd/gateway \
    && CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/migrate ./cmd/migrate

# --- Runtime stage ---------------------------------------------------------
FROM alpine:3.20

# TLS roots copied from the builder so openai/anthropic provider kinds can
# dial HTTPS at run time; no package install is needed in the runtime image.
COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt

# Dedicated unprivileged user; the numeric USER keeps runAsNonRoot checks
# working even when the image is renamed or re-tagged.
RUN addgroup -g 10001 -S gateway \
    && adduser -u 10001 -S -G gateway -H -D gateway

WORKDIR /app

COPY --from=builder /out/gateway /app/gateway
COPY --from=builder /out/migrate /app/migrate
COPY migrations /app/migrations

RUN chmod 0755 /app/gateway /app/migrate \
    && chmod -R a+rX /app/migrations \
    && chown -R gateway:gateway /app

USER 10001:10001

EXPOSE 8080

# The gateway is the default entrypoint; the one-shot migration job runs the
# same image with `command: ["/app/migrate", ...]` (see docker-compose.yml).
ENTRYPOINT ["/app/gateway"]
