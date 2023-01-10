.PHONY: proto build test deps
export GOPRIVATE=github.com/anytypeio
export PATH:=deps:$(PATH)

proto:
	protoc --gogofaster_out=:. --go-drpc_out=protolib=github.com/gogo/protobuf:. consensusproto/protos/*.proto

build:
	@$(eval FLAGS := $$(shell govvv -flags -pkg github.com/anytypeio/any-sync/app))
	go build -v -o bin/any-sync-consensusnode -ldflags "$(FLAGS)" github.com/anytypeio/any-sync-consensusnode/cmd

test:
	go test ./... --cover

deps:
	go mod download
	go build -o deps/protoc-gen-go-drpc storj.io/drpc/cmd/protoc-gen-go-drpc
	go build -o deps/protoc-gen-gogofaster github.com/gogo/protobuf/protoc-gen-gogofaster
