FROM rust as planner
WORKDIR trader
# We only pay the installation cost once, 
# it will be cached from the second build onwards
# To ensure a reproducible build consider pinning 
# the cargo-chef version with `--version X.X.X`
RUN cargo install cargo-chef 

COPY . .

RUN cargo chef prepare  --recipe-path recipe.json

FROM rust as cacher
WORKDIR trader
RUN cargo install cargo-chef
COPY --from=planner /trader/recipe.json recipe.json
RUN --mount=type=ssh cargo chef cook --release --recipe-path recipe.json

FROM rust as builder
WORKDIR trader
COPY . .
# Copy over the cached dependencies
COPY --from=cacher /trader/target target
COPY --from=cacher /usr/local/cargo /usr/local/cargo
RUN --mount=type=ssh cargo build --release --bin trader

FROM debian:buster-slim as runtime
WORKDIR trader
COPY --from=builder /trader/target/release/trader /usr/local/bin
ENV RUST_LOG=trader=debug
ENTRYPOINT ["/usr/local/bin/trader"]
