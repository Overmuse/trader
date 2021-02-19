FROM rust as build

COPY ./ ./

RUN --mount=type=ssh cargo build --release

RUN mkdir -p /build-out

RUN cp target/release/trader /build-out/

# Ubuntu 18.04
FROM ubuntu@sha256:5f4bdc3467537cbbe563e80db2c3ec95d548a9145d64453b06939c4592d67b6d

ENV DEBIAN_FRONTEND=noninteractive
ENV RUST_LOG=debug
RUN apt-get update && apt-get -y install ca-certificates libssl-dev && rm -rf /var/lib/apt/lists/*

COPY --from=build /build-out/trader /

CMD /trader
