# A distlib node with its web UI, built from source.
#
#   docker build -t distlib .
#
# Three stages: Node builds the UI, Rust builds the node with that UI
# embedded, and the image that runs holds the binary alone. See README.md,
# "Docker", for running it.

FROM node:24-bookworm-slim AS ui
WORKDIR /src/crates/distlib-ui/web
# The lockfile first, so the dependency layer is reused until it changes.
COPY crates/distlib-ui/web/package.json crates/distlib-ui/web/package-lock.json crates/distlib-ui/web/.npmrc crates/distlib-ui/web/.nvmrc ./
RUN npm ci
COPY crates/distlib-ui/web/ ./
RUN npm run build

FROM rust:1-bookworm AS node
WORKDIR /src
COPY . .
# A release build embeds whatever `web/dist` holds when it is compiled.
COPY --from=ui /src/crates/distlib-ui/web/dist crates/distlib-ui/web/dist
RUN cargo build --release --locked -p distlib \
    && cp target/release/distlib /usr/local/bin/distlib

FROM debian:bookworm-slim
LABEL org.opencontainers.image.source="https://github.com/izderadicka/distlib" \
      org.opencontainers.image.description="A distributed media library for a closed, trusted group" \
      org.opencontainers.image.licenses="MIT OR Apache-2.0"
# CA certificates for the relays a node dials when a direct path fails.
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --system --uid 10001 --home-dir /data distlib \
    && mkdir /data && chown distlib:distlib /data
COPY --from=node /usr/local/bin/distlib /usr/local/bin/distlib
USER distlib

# Everything a node keeps — identity, config, database, blobs, downloads — is
# under the data directory, so a volume there is the whole node.
ENV DISTLIB_DATA_DIR=/data
VOLUME /data

# Inside a container, loopback is the container's own: both listeners bind
# every interface, and which of them the host exposes, and where, is decided
# by `docker run -p`. The API is guarded by its token either way; publish it
# on the host's loopback (`-p 127.0.0.1:11280:11280`) unless a reverse proxy
# is in front of it.
ENV DISTLIB_API__BIND_ADDR=0.0.0.0:11280 \
    DISTLIB_NET__BIND_ADDR_V4=0.0.0.0:11204
EXPOSE 11280/tcp 11204/udp

ENTRYPOINT ["distlib"]
CMD ["run"]
