.PHONY: build
build:
	go build -o bin/hldbx ./hldbx/main.go

.PHONY: clean
clean:
	rm -rf bin

.PHONY: vet
vet:
	go vet ./...

.PHONY: test
test:
	go test -v ./...