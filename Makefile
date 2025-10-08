dev:
	go run .

build:
	go build -o ./bin/hb -ldflags "-X main.version=$(ver)" .

run: build
	./bin/hb

test:
	go test ./... -v -race

docker:
	docker build --build-arg ver=$(ver) -t meeron/honey-badger:$(ver) -t meeron/honey-badger:latest .
