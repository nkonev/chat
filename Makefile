EXECUTABLE := chat

.PHONY: test-verbose
test-verbose:
	# here is timeout for all the tests
	go test ./... -count=1 -test.v -test.timeout=360s -p 1

.PHONY: download
download:
	go install github.com/vektra/mockery/v3@v3.5.0

generate:
	mockery

package: package-go

package-go:
	rm -f $(EXECUTABLE)
	CGO_ENABLED=0 go build -o $(EXECUTABLE) -trimpath -ldflags '-w -extldflags "-static"'

.PHONY: infra
infra: infra_up

.PHONY: infra_up
infra_up:
	docker compose up -d
	./scripts/wait-for-it.sh -t 30 127.0.0.1:5432 -- echo 'postgresql-citus-coordinator-1 is up'
	./scripts/wait-for-it.sh -t 30 127.0.0.1:9092 -- echo 'kafka is up'
	./scripts/wait-for-it.sh -t 30 127.0.0.1:16686 -- echo 'jaeger web ui is up'
	./scripts/wait-for-it.sh -t 30 127.0.0.1:15672 -- echo 'rabbitmq web ui is up'

.PHONY: infra_down
infra_down:
	docker compose down -v

run: package-go infra
	./$(EXECUTABLE) serve --cqrs.testhelpermethods=true

.PHONY: test
test: export CHAT_LOGGER.LEVEL = warn
test: export CHAT_POSTGRESQL.PRETTYLOG = false
test: export CHAT_POSTGRESQL.DUMP = false
test: export CHAT_CQRS.PRETTYLOG = false
test: export CHAT_CQRS.DUMP = false
test: export CHAT_HTTP.PRETTYLOG = false
test: export CHAT_HTTP.DUMP = false
test: export CHAT_SERVER.PRETTYLOG = false
test: export CHAT_SERVER.DUMP = false
test: export CHAT_RABBITMQ.DUMPTESTACCUMULATOR = false
test: export CHAT_RABBITMQ.PRETTYLOG = false
test: export CHAT_RABBITMQ.DUMP = false
test:
	# here is timeout for all tests
	go test ./... -count=1 -test.v -test.timeout=180s -p 1
