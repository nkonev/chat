# Example Golang CQRS chat application

This application is using [Watermill CQRS](http://watermill.io/docs/cqrs) component.

It's based on [6-cqrs-ordered-events](https://github.com/ThreeDotsLabs/watermill/tree/v1.4.6/_examples/basic/6-cqrs-ordered-events) example.

Events are stored in Kafka, projections are stored in PostgreSQL.

We can reset our projections and then restore their state from Kafka by resetting offsets.

See [It's Okay To Store Data In Kafka](https://www.confluent.io/blog/okay-store-data-apache-kafka/).

# Start
```bash
docker compose up -d
go run . serve
```

# Play with
```bash
# create a chat
curl -i -X POST -H 'Content-Type: application/json' -H 'X-UserId: 1' --url 'http://localhost:8080/api/chat' -d '{"title": "new chat"}'

# rename the chat
curl -i -X PUT -H 'Content-Type: application/json' --url 'http://localhost:8080/api/chat' -d '{"id": 1, "title": "super new chat"}'

# show chats
curl -Ss -X GET -H 'X-UserId: 1' --url 'http://localhost:8080/api/chat/search' | jq
# show chats with pagination
curl -Ss -X GET --url 'http://localhost:8080/api/chat/search?size=40&pinned=false&lastUpdateDateTime=2024-10-31T22:37:34.643937Z&id=477&reverse=true&includeStartingFrom=true' -H 'Accept: application/json' -H 'X-UserId: 1' | jq
curl -Ss -X GET --url 'http://localhost:8080/api/chat/search?size=40reverse=false&includeStartingFrom=true' -H 'Accept: application/json' -H 'X-UserId: 1' | jq

# pin chat
curl -i -X PUT -H 'X-UserId: 1' --url 'http://localhost:8080/api/chat/1/pin?pin=true'

# create a message
curl -i -X POST -H 'Content-Type: application/json' -H 'X-UserId: 1' --url 'http://localhost:8080/api/chat/1/message' -d '{"content": "new message"}'
curl -i -X POST -H 'Content-Type: application/json' -H 'X-UserId: 1' --url 'http://localhost:8080/api/chat/1/message' -d '{"content": "new message 2"}'
curl -i -X POST -H 'Content-Type: application/json' -H 'X-UserId: 1' --url 'http://localhost:8080/api/chat/1/message' -d '{"content": "new message 3"}'

# show messages
curl -Ss -X GET -H 'X-UserId: 1' --url 'http://localhost:8080/api/chat/1/message/search' | jq

# add a reaction
curl -i -X PUT -H 'X-UserId: 1' -H 'Content-Type: application/json' --url 'http://localhost:8080/api/chat/1/message/1/reaction' -d '{"reaction": "😀"}'

# read message
curl -i -X PUT -H 'X-UserId: 1' --url 'http://localhost:8080/api/chat/1/message/2/read'

# add participant into chat
curl -i -X PUT -H 'X-UserId: 1' -H 'Content-Type: application/json' --url 'http://localhost:8080/api/chat/1/participant' -d '{"participantIds": [2, 3]}'

# remove participant from chat
curl -i -X DELETE -H 'Content-Type: application/json' --url 'http://localhost:8080/api/chat/1/participant' -d '{"participantIds": [3]}'

# show participants
curl -Ss -X GET --url 'http://localhost:8080/api/chat/1/participants' | jq

# get his chats - show unreads
curl -Ss -X GET -H 'X-UserId: 2' --url 'http://localhost:8080/api/chat/search' | jq

# remove message from chat
curl -i -X DELETE  -H 'X-UserId: 1' --url 'http://localhost:8080/api/chat/1/message/1'

# show has new messages (unreads)
curl -Ss -X GET -H 'X-UserId: 2' --url 'http://localhost:8080/api/chat/has-new-messages' | jq

# read
curl -i -X PUT -H 'X-UserId: 2' --url 'http://localhost:8080/api/chat/1/message/500/read'

# ... or set to consider (contribute)
curl -i -X PUT -H 'Content-Type: application/json' -H 'X-UserId: 2' --url 'http://localhost:8080/api/chat/2/notification' -d '{"considerMessagesOfThisChatAsUnread": false}'

# make blog
curl -i -X PUT -H 'Content-Type: application/json' --url 'http://localhost:8080/api/chat' -d '{"id": 1, "title": "new chat", "blog": true}'
curl -i -X PUT --url 'http://localhost:8080/api/chat/1/message/1/blog-post'

# show blog
curl -Ss -X GET --url 'http://localhost:8080/api/blog/search' | jq
curl -Ss -X GET --url 'http://localhost:8080/api/blog/1' | jq
curl -Ss -X GET --url 'http://localhost:8080/api/blog/1/comment/search' | jq

# with correlation id
curl -i -X POST -H 'Content-Type: application/json' -H 'X-UserId: 1' -H 'X-CorrelationId: 9e49b4dd-4068-4c6a-ada0-da78f44bdeba' --url 'http://localhost:8080/api/chat' -d '{"title": "new chat"}'
# then see kafka

# reset offsets for consumer groups
go run . reset
```

# Tracing
See `Trace-Id` header and put its value into [Jaeger UI](http://localhost:16686)

# Various commands
```bash
# show logs
docker compose logs -f kafka
docker compose logs -f postgresql

# show projections
docker compose exec -it postgresql psql -U postgres

docker compose exec -it kafka bash
docker compose exec -it kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server kafka:29092 --list
docker compose exec -it kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server kafka:29092 --describe --group CommonProjection --offsets

# show kafka topic's messages
docker compose exec -it kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka:29092 --topic event --from-beginning --property print.key=true --property print.headers=true

# non-actual resetting - missed fast-forwarding of sequences
docker compose exec -it kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server kafka:29092 --group CommonProjection --reset-offsets --to-earliest --execute --topic event
# reset db
docker rm -f postgresql
docker volume rm go-cqrs-example_postgres_data
docker compose up -d postgresql
```

```sql
SELECT * FROM citus_shards;
SELECT * from pg_dist_shard;
SELECT * from citus_get_active_worker_nodes();
SELECT * FROM citus_check_cluster_node_health();
```
