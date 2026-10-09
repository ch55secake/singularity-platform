FROM rust:1.88-bookworm AS builder

WORKDIR /build
COPY Cargo.toml ./
COPY src ./src
RUN cargo build --release

FROM debian:bookworm-slim

RUN apt-get update \
    && apt-get install --yes --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /build/target/release/singularity-platform /usr/local/bin/singularity-platform

RUN useradd --create-home --uid 10001 singularity \
    && mkdir --parents /data \
    && chown --recursive singularity:singularity /data

USER singularity
WORKDIR /data
ENV SINGULARITY_BIND=0.0.0.0:8080
ENV SINGULARITY_SPOOL_PATH=/data/salts.jsonl
ENV SINGULARITY_CREDENTIALS_PATH=/data/collectors.json

EXPOSE 8080
ENTRYPOINT ["/usr/local/bin/singularity-platform"]
