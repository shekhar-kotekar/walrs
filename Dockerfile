FROM docker.io/library/rust:alpine3.20 AS builder

RUN rustup default stable
RUN apk add --no-cache musl-dev libpq gcc

ARG module_name=kraft_rs

WORKDIR /${module_name}
COPY Cargo.toml Cargo.lock /${module_name}/
COPY . ./
RUN echo $(ls -ltrh /${module_name})

RUN cargo test && cargo build --release

RUN echo $(ls -ltrh /${module_name}/target/release/)

FROM scratch
ARG module_name=kraft_rs

COPY --from=builder /${module_name}/target/release/kraft_rs /kraft_rs

CMD [ "/kraft_rs" ]
