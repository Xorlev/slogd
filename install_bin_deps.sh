#! /usr/bin/env bash

go get -u github.com/golang/dep/cmd/dep && \
    dep ensure && \
    pushd vendor/github.com/gogo/protobuf/protoc-gen-gofast && \
    go install && \
	popd && \
	pushd vendor/github.com/grpc-ecosystem/grpc-gateway/protoc-gen-grpc-gateway && \
    go install && \
	popd
