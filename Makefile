BINARY_NAME = dgrep

build:
	go build -o ./bin/$(BINARY_NAME) main.go

run: build
	./bin/$(BINARY_NAME)

clean:
	rm -rf ./bin/$(BINARY_NAME)