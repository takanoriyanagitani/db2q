#!/bin/sh

listen_addr="127.0.0.1:9115"

RUST_LOG=info \
ENV_LISTEN_ADDR="${listen_addr}" \
	./simple
