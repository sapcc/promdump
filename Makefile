VERSION := $(shell git describe --tags --always --dirty)

build:
	go build -o promdump -ldflags "-X main.version=$(VERSION)"
