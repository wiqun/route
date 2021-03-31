@echo off

protoc --gofast_out=../internal/crdt  ./state.proto

protoc --gofast_out=../internal/message  ./message.proto