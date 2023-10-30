dev:
	go run .

build:
	go build -o ./bin/hb -ldflags "-X main.version=$(ver)" .

run: build
	./bin/hb -config config.json

proto:
	protoc --go_out=./pb --go_opt=paths=source_relative --go-grpc_out=./pb --go-grpc_opt=paths=source_relative honey_badger.proto

bench: build
	./bin/hb -bench 127.0.0.1:18950

test:
	go test ./... -v -race

docker:
	docker build --build-arg ver=$(ver) -t meeron/honey-badger:$(ver) -t meeron/honey-badger:latest .

build-cli:
	go build -o ./bin/hbclient ./cli

cli: build-cli
	./bin/hbclient

