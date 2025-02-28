.PHONY: build
build:
	go build -o bin/hldbx ./hldbx/main.go

.PHONY: clean
clean:
	rm -rf bin