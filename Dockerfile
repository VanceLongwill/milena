FROM rust:1.79-bookworm as builder
RUN apt-get update && apt-get install -y build-essential \
    curl \
    openssl libssl-dev \
    pkg-config \
    zlib1g-dev \
    cmake
WORKDIR /app
COPY Cargo.* ./
COPY src ./src
RUN cargo install --path .

FROM debian:bookworm-slim
COPY --from=builder /usr/local/cargo/bin/milena /usr/local/bin/milena
ENTRYPOINT ["milena"]
