FROM rust:1.88-bookworm AS builder

WORKDIR /build
RUN apt-get update \
    && apt-get install --yes --no-install-recommends libprotobuf-dev libssl-dev pkg-config protobuf-compiler \
    && rm -rf /var/lib/apt/lists/*

# prost-build needs the system protobuf definitions for imports such as
# google/protobuf/descriptor.proto.
ENV PROTOC_INCLUDE=/usr/include

COPY Cargo.toml ./
COPY Cargo.lock ./
COPY src ./src
RUN cargo build --release --locked

FROM debian:bookworm-slim

RUN apt-get update \
    && apt-get install --yes --no-install-recommends ca-certificates libssl3 \
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
