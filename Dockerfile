# build stage
FROM rust:1.83-slim AS builder
WORKDIR /app
COPY Cargo.toml Cargo.lock build.rs ./
COPY proto ./proto
COPY src ./src
COPY examples ./examples
RUN cargo build --release --bins

# runtime stage
FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates && rm -rf /var/lib/apt/lists/*
COPY --from=builder /app/target/release/rustproxy /usr/local/bin/rustproxy
COPY --from=builder /app/target/release/demo-backend /usr/local/bin/demo-backend
COPY config.yaml /etc/rustproxy/config.yaml

EXPOSE 8080 9090 50051
ENTRYPOINT ["rustproxy"]
CMD ["--config", "/etc/rustproxy/config.yaml"]
