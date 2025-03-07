.PHONY: build
build:
	go build -o bin/hldbx ./hldbx/main.go

.PHONY: clean
clean:
	rm -rf bin

.PHONY: vet
vet:
	go vet ./...

.PHONY: unit-test
unit-test:
	go test -v ./...