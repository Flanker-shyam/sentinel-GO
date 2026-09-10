# syntax=docker/dockerfile:1

# ---------------------------------------------------------------------------
# Sentinel-GO — multi-stage build
#
# The org standard is to build from the internal ECR mirror, e.g.:
#   FROM 307422823171.dkr.ecr.eu-west-1.amazonaws.com/golang:<ver>-alpine<ver> AS build
# The reference n1p golang-build image is golang:1.22.5-alpine3.20, but this
# module requires Go 1.26 (see go.mod `go 1.26.0`), so we pin the build stage to
# golang:1.26-alpine (a published official tag). Swap the registry prefix to the
# org ECR mirror once golang:1.26-alpine* is mirrored there.
# ---------------------------------------------------------------------------
ARG GO_VERSION=1.26

# ----------------------------- build stage ---------------------------------
FROM golang:${GO_VERSION}-alpine AS build

# git is needed if any module is fetched from a VCS; build-base covers cgo edge cases.
RUN apk add --no-cache git ca-certificates && update-ca-certificates

WORKDIR /src

# Cache module downloads separately from source for faster rebuilds.
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod \
    go mod download

# Copy the rest of the source.
COPY . .

# Build a fully static binary. CGO disabled so it runs on a scratch/distroless base.
# TARGETOS/TARGETARCH are provided automatically by BuildKit for buildx multi-arch.
ARG TARGETOS=linux
ARG TARGETARCH=arm64
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} \
    go build -trimpath -ldflags="-s -w" -o /out/sentinel ./cmd/sentinel/

# --------------------------- runtime stage ---------------------------------
# static-debian12 (distroless) gives us CA certs + a nonroot user, no shell.
FROM gcr.io/distroless/static-debian12:nonroot AS runtime

WORKDIR /app

# Ship the config files; the binary loads configs/sentinel.<env>.yaml at runtime.
COPY --from=build /out/sentinel /app/sentinel
COPY configs/ /app/configs/

# distroless "nonroot" is uid/gid 65532 — matches runAsNonRoot in the manifests.
USER nonroot:nonroot

# SENTINEL_ENV selects the config file (dev|prod|...). Overridden in the Deployment.
ENV SENTINEL_ENV=prod

ENTRYPOINT ["/app/sentinel"]
