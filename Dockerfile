FROM rust as build

COPY ./ ./

ENV CARGO_NET_GIT_FETCH_WITH_CLI=true

RUN --mount=ssh cargo build --release

RUN mkdir -p /build-out

RUN cp target/release/trader /build-out/

FROM ubuntu:18.04

ENV DEBIAN_FRONTEND=noninteractive
ENV RUST_LOG=debug
RUN apt-get update && apt-get -y install ca-certificates libssl-dev && rm -rf /var/lib/apt/lists/*

COPY --from=build /build-out/trader /

CMD /trader
