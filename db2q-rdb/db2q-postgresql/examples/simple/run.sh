#!/bin/sh

listen_addr="127.0.0.1:9115"
wait_next_min_interval_ns=$( echo 1,000 | tr -d , )

RUST_LOG=info \
ENV_INTERVAL_NS_MINIMUM="${wait_next_min_interval_ns}" \
ENV_LISTEN_ADDR="${listen_addr}" \
	./simple
