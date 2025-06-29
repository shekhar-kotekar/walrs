FROM docker.io/library/rust:alpine3.20 AS builder

RUN rustup default stable
RUN apk add --no-cache musl-dev libpq gcc

WORKDIR /walrs

# Copy workspace Cargo.toml and all package Cargo.toml files
COPY ./Cargo.toml .
COPY ./Cargo.lock .
COPY ./server/Cargo.toml ./server/
COPY ./commons/Cargo.toml ./commons/
COPY ./cli/Cargo.toml ./cli/

# Create dummy source files for dependency caching
RUN mkdir -p ./server/src ./commons/src ./cli/src
RUN echo "fn main() {}" > ./server/src/main.rs
RUN echo "pub fn hello() {}" > ./commons/src/lib.rs
RUN echo "pub fn greet() {}" > ./cli/src/main.rs

# Build dependencies (this layer will be cached)
RUN cargo build --release --bin server

# Remove dummy source files
RUN rm -rf ./server/src && rm -rf ./commons/src

# Copy actual source code
COPY ./commons/src ./commons/src
COPY ./server/src ./server/src

RUN cargo clean

# Build final application
RUN cargo build --release --bin server

FROM builder AS test
RUN cargo test --package server --release

FROM scratch AS runtime
COPY --from=builder /walrs/target/release/server /walrs_server
EXPOSE 5056
CMD [ "/walrs_server" ]