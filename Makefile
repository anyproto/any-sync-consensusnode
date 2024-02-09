.PHONY: proto build test deps
export GOPRIVATE=github.com/anyproto
export PATH:=deps:$(PATH)
BUILD_GOARCH:=$(shell go env GOARCH)

proto:
	protoc --gogofaster_out=:. --go-drpc_out=protolib=github.com/gogo/protobuf:. consensusproto/protos/*.proto

build:
	@$(eval FLAGS := $$(shell PATH=$(PATH) govvv -flags -pkg github.com/anyproto/any-sync/app))
	GOARCH=$(BUILD_GOARCH) go build -v -o bin/any-sync-consensusnode -ldflags "$(FLAGS) -X github.com/anyproto/any-sync/app.AppName=any-sync-consensusnode" github.com/anyproto/any-sync-consensusnode/cmd

test:
	go test ./... --cover

deps:
	go mod download
	go build -o deps/protoc-gen-go-drpc storj.io/drpc/cmd/protoc-gen-go-drpc
	go build -o deps/protoc-gen-gogofaster github.com/gogo/protobuf/protoc-gen-gogofaster
	go build -o deps github.com/ahmetb/govvv