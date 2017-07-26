#!/usr/bin/env bash
protoc \
    -I. \
    -I../vendor/google.golang.org \
    -I../vendor/github.com/grpc-ecosystem/grpc-gateway/third_party/googleapis \
    --grpc-gateway_out=logtostderr=true:. \
    --gofast_out=Mgoogle/protobuf/any.proto=github.com/gogo/protobuf/types,Mgoogle/protobuf/timestamp.proto=github.com/gogo/protobuf/types,plugins=grpc:. \
    data.proto rpc.proto
