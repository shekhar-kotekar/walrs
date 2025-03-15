FROM mcr.microsoft.com/devcontainers/rust:latest

RUN apt-get update && apt-get install vim -y && apt-get clean -y && rm -rf /var/lib/apt/lists/*

RUN rustup update && cargo install cargo-watch