SHELL := /bin/sh
BINDIR := bin
DISTDIR := dist
NAME := go-rpc
VERSION ?= 1.0.0
PLATFORM := $(shell go env GOOS)-$(shell go env GOARCH)

.PHONY: all test vet build check run-server run-client package stats clean
all: check
test:
	go test ./...
vet:
	go vet ./...
build:
	mkdir -p $(BINDIR)
	go build -trimpath -o $(BINDIR)/go-rpc-server ./cmd/server
	go build -trimpath -o $(BINDIR)/go-rpc-client ./cmd/client
check: test vet build
run-server:
	go run ./cmd/server -addr :9001
run-client:
	go run ./cmd/client -target 127.0.0.1:9001 -web :8080
package: check
	mkdir -p $(DISTDIR)/$(NAME)-$(VERSION)-$(PLATFORM)
	cp $(BINDIR)/go-rpc-server $(BINDIR)/go-rpc-client README.md $(DISTDIR)/$(NAME)-$(VERSION)-$(PLATFORM)/
	cp -R docs $(DISTDIR)/$(NAME)-$(VERSION)-$(PLATFORM)/docs
	LC_ALL=C tar -C $(DISTDIR) -czf $(DISTDIR)/$(NAME)-$(VERSION)-$(PLATFORM).tar.gz $(NAME)-$(VERSION)-$(PLATFORM)
	rm -rf $(DISTDIR)/$(NAME)-$(VERSION)-$(PLATFORM)
stats:
	@echo "Go files (non-test): $$(find . -name '*.go' -not -name '*_test.go' | wc -l | tr -d ' ')"
	@echo "Go lines (non-test): $$(find . -name '*.go' -not -name '*_test.go' -print0 | xargs -0 cat | wc -l | tr -d ' ')"
clean:
	rm -rf $(BINDIR) $(DISTDIR)/*.tar.gz
