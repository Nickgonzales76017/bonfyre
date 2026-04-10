# Bonfyre — multi-stage build
# Produces a ~15 MB image with all binaries + SQLite runtime
# Usage:
#   docker build -t bonfyre .
#   docker compose up

# ── Stage 1: Build ───────────────────────────────────────────
FROM debian:bookworm-slim AS build

RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc make libc6-dev libsqlite3-dev zlib1g-dev pkg-config ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /src
COPY lib/ lib/
COPY cmd/ cmd/
COPY Makefile .

# Build everything that compiles with standard deps.
# Binaries needing whisper.cpp / onnxruntime / tree-sitter are
# skipped gracefully by the top-level Makefile (they just fail).
RUN make all CC=gcc \
    CFLAGS="-O2 -Wall -Wextra -std=c11 -DNDEBUG -static" 2>&1 \
    || true

# Collect all successfully-built binaries into /out
RUN mkdir -p /out/bin /out/lib && \
    for dir in cmd/*/; do \
        find "$dir" -maxdepth 1 -name 'bonfyre-*' -type f -executable \
            -exec cp {} /out/bin/ \; 2>/dev/null; \
    done && \
    cp lib/libbonfyre/libbonfyre.a /out/lib/ 2>/dev/null || true && \
    cp lib/liblambda-tensors/liblambda-tensors.a /out/lib/ 2>/dev/null || true && \
    echo "Built binaries:" && ls -lhS /out/bin/

# ── Stage 2: Runtime ─────────────────────────────────────────
FROM debian:bookworm-slim AS runtime

RUN apt-get update && apt-get install -y --no-install-recommends \
    libsqlite3-0 curl ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && useradd -r -s /usr/sbin/nologin -m bonfyre

COPY --from=build /out/bin/ /usr/local/bin/
COPY --from=build /out/lib/ /usr/local/lib/

# Data directory for SQLite DB + uploads
RUN mkdir -p /data/uploads && chown -R bonfyre:bonfyre /data
VOLUME /data

ENV BONFYRE_DB=/data/queue.db
ENV BONFYRE_UPLOAD_DIR=/data/uploads
ENV BONFYRE_PORT=9999

USER bonfyre
EXPOSE 9999

# Default: run the API server
CMD ["bonfyre-api", "serve", "--port", "9999", "--db", "/data/queue.db"]
