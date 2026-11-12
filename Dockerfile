# AwanFS S3 server (#49). Build: docker build -t awanfs .
# Run: docker run -p 9000:9000 -v awanfs-data:/data \
#        -e AWANFS_ACCESS_KEY=... -e AWANFS_SECRET_KEY=... awanfs
# The container listens on 0.0.0.0 inside its network namespace; publish the port to choose
# who can reach it. For HTTPS, mount a certificate and set AWANFS_TLS_CERT and AWANFS_TLS_KEY.

FROM rust:1-bookworm AS build
WORKDIR /src
COPY server/Cargo.toml server/Cargo.lock ./server/
COPY server/src ./server/src
COPY server/include ./server/include
RUN cargo build --locked --release --bin s3-server --manifest-path server/Cargo.toml

FROM debian:bookworm-slim
RUN useradd --system --uid 10001 --home-dir /data --shell /usr/sbin/nologin awanfs \
 && mkdir -p /data && chown awanfs:awanfs /data
COPY --from=build /src/server/target/release/s3-server /usr/local/bin/s3-server
USER awanfs
VOLUME ["/data"]
EXPOSE 9000
ENV AWANFS_LISTEN=0.0.0.0
# SIGTERM (docker stop) runs the graceful shutdown: finish requests, sync, exit 0.
STOPSIGNAL SIGTERM
ENTRYPOINT ["/usr/local/bin/s3-server", "--port", "9000", "--data-dir", "/data"]
