.PHONY: test-verbose
test-verbose:
	# here is timeout for all tests
	go test ./... -count=1 -test.v -test.timeout=180s -p 1

generate:
	@go generate -v ./...

.PHONY: test
test: export CHAT_LOGGER.LEVEL = warn
test: export CHAT_POSTGRESQL.PRETTYLOG = false
test: export CHAT_POSTGRESQL.DUMP = false
test: export CHAT_CQRS.PRETTYLOG = false
test: export CHAT_CQRS.DUMP = false
test: export CHAT_HTTP.PRETTYLOG = false
test: export CHAT_HTTP.DUMP = false
test:
	# here is timeout for all tests
	go test ./... -count=1 -test.v -test.timeout=180s -p 1
