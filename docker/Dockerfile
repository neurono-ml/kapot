FROM docker.io/rust:1.81-bullseye as builder

ENV RUST_LOG=info
ENV RUST_BACKTRACE=full
ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update && \
    apt-get -y install libssl-dev openssl zlib1g zlib1g-dev libpq-dev cmake protobuf-compiler netcat curl unzip

# create build user with same UID as
RUN adduser -q builder --home /home/builder && \
    mkdir -p /home/builder/workspace

ENV HOME=/home/builder
ENV PATH=$HOME/.cargo/bin:$PATH

# prepare rust
RUN rustup update && \
    rustup component add rustfmt && \
    cargo install cargo-chef --version 0.1.62

WORKDIR /home/builder/workspace

ADD kapot/ /home/builder/workspace/kapot/
ADD examples/ /home/builder/workspace/examples/
ADD Cargo.toml /home/builder/workspace/Cargo.toml

RUN cargo build --release
RUN mkdir -p /home/builder/built/bin/ && \
    cp /home/builder/workspace/target/release/kapot-cli /home/builder/built/bin/ && \
    cp /home/builder/workspace/target/release/kapot-scheduler /home/builder/built/bin/ && \
    cp /home/builder/workspace/target/release/kapot-executor /home/builder/built/bin/ && \
    chmod 777 -R /home/builder/

FROM docker.io/ubuntu:24.04

LABEL org.opencontainers.image.source="https://github.com/andreclaudino/datafusion-kapot"
LABEL org.opencontainers.image.description="Kapôt: Distributed SQL Query Engine, built on Apache Arrow"
LABEL org.opencontainers.image.licenses="Apache-2.0"

RUN apt-get update && \
    apt-get -y install libssl-dev openssl zlib1g zlib1g-dev libpq-dev protobuf-compiler curl unzip

# Expose kapôt Scheduler gRPC port
EXPOSE 50050

# Expose kapôt Executor gRPC port
EXPOSE 50051

ENV RUST_LOG=info

COPY --from=builder /home/builder/built/bin/* /usr/bin/

