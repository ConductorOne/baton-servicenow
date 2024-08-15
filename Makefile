GOOS = $(shell go env GOOS)
GOARCH = $(shell go env GOARCH)
BUILD_DIR = dist/${GOOS}_${GOARCH}

ifeq ($(GOOS),windows)
OUTPUT_PATH = ${BUILD_DIR}/baton-servicenow.exe
else
OUTPUT_PATH = ${BUILD_DIR}/baton-servicenow
endif

.PHONY: build
build:
	go build -o ${OUTPUT_PATH} ./cmd/baton-servicenow

.PHONY: update-deps
update-deps:
	go get -d -u ./...
	go mod tidy -v
	go mod vendor

.PHONY: add-dep
add-dep:
	go mod tidy -v
	go mod vendor

.PHONY: protogen
protogen:
	buf generate

.PHONY: protofmt
protofmt:
	buf format -w

.PHONY: lint
lint:
	golangci-lint run
