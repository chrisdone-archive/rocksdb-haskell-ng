FROM ubuntu:16.04
MAINTAINER Mukul Sati

# Setup

RUN apt-get update
RUN apt-get install -yq --no-install-suggests --no-install-recommends --force-yes -y librocksdb-dev  netbase git ca-certificates xz-utils build-essential curl

RUN curl -sSL https://get.haskellstack.org/ | sh

RUN stack --version

# Clone repo

RUN git clone https://github.com/chrisdone/rocksdb-haskell-ng --depth 1

# Install GHC

RUN cd rocksdb-haskell-ng; stack setup

# Build and run test suite

RUN cd rocksdb-haskell-ng; stack test
