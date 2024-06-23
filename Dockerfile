FROM --platform=linux/amd64 rust:1.79-bookworm as builder
WORKDIR /app
COPY Cargo.* ./
COPY src ./src
RUN cargo install --path .

FROM --platform=linux/amd64 debian:bookworm-slim
COPY --from=builder /usr/local/cargo/bin/milena /usr/local/bin/milena
ENTRYPOINT ["milena"]
