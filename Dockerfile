# Demo rex node image: no Erlang inside, rex serves EPMD itself.
FROM rust:1-bookworm AS build
WORKDIR /src
COPY Cargo.toml Cargo.lock ./
COPY src ./src
COPY examples ./examples
RUN cargo build --release --example rex_node

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates && rm -rf /var/lib/apt/lists/*
COPY --from=build /src/target/release/examples/rex_node /usr/local/bin/rex_node
EXPOSE 4369
ENTRYPOINT ["rex_node"]
