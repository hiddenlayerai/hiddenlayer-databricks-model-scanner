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

.PHONY: setupGitHooks
setupGitHooks:
	@if [ -z "$$ANTHROPIC_API_KEY" ]; then \
		echo "Error: ANTHROPIC_API_KEY environment variable is not set" >&2; \
		exit 1; \
	fi
	pip install -r .githooks/requirements.txt
	git config --local core.hooksPath .githooks/
	chmod +x .githooks/pre-commit
	chmod +x .githooks/pre-commit.py

	
