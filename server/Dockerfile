FROM docker.io/library/rust:alpine3.20 AS builder

RUN rustup default stable
RUN apk add --no-cache musl-dev libpq gcc

ARG module_name=walrs

WORKDIR /${module_name}
COPY Cargo.toml Cargo.lock /${module_name}/
COPY . ./
RUN echo $(ls -ltrh /${module_name})

RUN cargo test && cargo build --release

RUN echo $(ls -ltrh /${module_name}/target/release/)

#FROM scratch
FROM alpine:latest

RUN apk add --no-cache bind-tools

ARG module_name=walrs

COPY --from=builder /${module_name}/target/release/server /walrs

CMD [ "/walrs" ]
