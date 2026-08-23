# Multi-stage build producing a minimal, non-root image.
#
# One Dockerfile builds both binaries; TARGET selects which. Duplicating this
# file per binary would mean two places to forget a security flag.

# ---------------------------------------------------------------------------
# Build stage
# ---------------------------------------------------------------------------
FROM golang:1.26.5 AS build

WORKDIR /src

# Dependencies are copied and downloaded BEFORE the source. Docker caches each
# layer by the checksum of its inputs, so as long as go.mod and go.sum are
# unchanged this layer is reused and a source-only edit does not re-download the
# module graph. Copying everything at once would invalidate the cache on every
# single code change.
COPY go.mod go.sum ./
RUN go mod download

COPY . .

ARG TARGET=api
ARG VERSION=dev

# CGO_ENABLED=0 produces a statically linked binary with no libc dependency,
#   which is what allows the scratch-like final image below.
# -trimpath strips local filesystem paths from the binary, so stack traces do
#   not leak the build machine's directory layout, and builds are reproducible.
# -s -w drop the symbol table and DWARF data, cutting binary size substantially.
# timetzdata embeds the IANA timezone database, because the final image has no
#   /usr/share/zoneinfo and any time.LoadLocation call would otherwise fail at
#   runtime rather than at build time.
RUN CGO_ENABLED=0 GOOS=linux go build \
        -trimpath \
        -tags timetzdata \
        -ldflags="-s -w -X main.version=${VERSION}" \
        -o /out/app ./cmd/${TARGET}

# ---------------------------------------------------------------------------
# Runtime stage
# ---------------------------------------------------------------------------
# distroless/static holds a CA bundle, /etc/passwd and nothing else: no shell,
# no package manager, no coreutils. If an attacker achieves code execution there
# is nothing in the image to pivot with, and the vulnerability-scan surface is
# close to zero. It is also about 2 MB.
FROM gcr.io/distroless/static-debian12:nonroot

COPY --from=build /out/app /app

# Run as an unprivileged user. Containers run as root by default, which means a
# container escape starts as root on the host.
USER nonroot:nonroot

EXPOSE 8080 9091

ENTRYPOINT ["/app"]
