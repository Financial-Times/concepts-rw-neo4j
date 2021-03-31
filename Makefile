.PHONY: all test clean

all: clean build-readonly test

build:
	@echo ">>> Building Application..."
	go build -v -o bin/ ./...

build-readonly:
	@echo ">>> Building Application with -mod=readonly..."
	go build -mod=readonly -v -o bin/ ./...

test:
	@echo ">>> Running Tests..."
	 go test  -timeout 60s -race ./... -count=1

compose-test: compose-up compose-print compose-down

compose-print:
	docker logs -f test-runner
compose-down:
	docker-compose -f docker-compose-tests.yml down -v

compose-up:
	docker-compose -f docker-compose-tests.yml up -d --build

clean:
	@echo ">>> Removing binaries..."
	@rm -rf bin
	@echo ">>> Cleaning modules cache..."
	go clean -modcache
