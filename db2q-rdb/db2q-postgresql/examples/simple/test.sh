#!/bin/sh

listen_addr="127.0.0.1:9115"
protodir="./db2q-proto"

tcreate(){
	jq -n -c '{
		request_id: {
			hi: 20230911,
			lo: 092555,
		},
		topic_id: {
			hi: 3776,
			lo:  599,
		},
	}' |
	grpcurl \
		-plaintext \
		-d @ \
		-import-path "${protodir}" \
		-proto db2q/proto/queue/v1/q.proto \
		"${listen_addr}" \
		db2q.proto.queue.v1.TopicService/Create
}

tcreate2(){
	jq -n -c '{
		request_id: {
			hi: 20230914,
			lo: 091625,
		},
		topic_id: {
			hi: 634,
			lo: 333,
		},
	}' |
	grpcurl \
		-plaintext \
		-d @ \
		-import-path "${protodir}" \
		-proto db2q/proto/queue/v1/q.proto \
		"${listen_addr}" \
		db2q.proto.queue.v1.TopicService/Create
}

tdrop(){
	jq -n -c '{
		request_id: {
			hi: 20230911,
			lo: 093006,
		},
		topic_id: {
			hi: 3776,
			lo:  599,
		},
	}' |
	grpcurl \
		-plaintext \
		-d @ \
		-import-path "${protodir}" \
		-proto db2q/proto/queue/v1/q.proto \
		"${listen_addr}" \
		db2q.proto.queue.v1.TopicService/Drop
}

tpush(){
	jq -n -c '{
		request_id: {
			hi: 20230911,
			lo: 092555,
		},
		topic_id: {
			hi: 3776,
			lo:  599,
		},
		value: "aGVs"
	}' |
	grpcurl \
		-plaintext \
		-d @ \
		-import-path "${protodir}" \
		-proto db2q/proto/queue/v1/q.proto \
		"${listen_addr}" \
		db2q.proto.queue.v1.QueueService/PushBack
}

tcount(){
	jq -n -c '{
		request_id: {
			hi: 20230911,
			lo: 092555,
		},
		topic_id: {
			hi: 3776,
			lo:  599,
		},
	}' |
	grpcurl \
		-plaintext \
		-d @ \
		-import-path "${protodir}" \
		-proto db2q/proto/queue/v1/q.proto \
		"${listen_addr}" \
		db2q.proto.queue.v1.QueueService/Count
}

tlist(){
	jq -n -c '{
		request_id: {
			hi: 20230914,
			lo: 091327,
		},
	}' |
	grpcurl \
		-plaintext \
		-d @ \
		-import-path "${protodir}" \
		-proto db2q/proto/queue/v1/q.proto \
		"${listen_addr}" \
		db2q.proto.queue.v1.TopicService/List
}

tdrop
tcreate
tdrop
tdrop

tcreate
tcreate2
tpush
tpush
tpush
tcount
tlist
