package producer

import (
	"context"
	"encoding/json"
	"github.com/beliyav/go-amqp-reconnect/rabbitmq"
	"github.com/streadway/amqp"
	"go-cqrs-chat-example/logger"
	myRabbitmq "go-cqrs-chat-example/rabbitmq"
	"go-cqrs-chat-example/type_registry"
	"time"
)

const EventsFanoutExchange = "async-events-exchange"
const correlationIdName = "correlationId"

func (rp *RabbitEventsPublisher) Publish(ctx context.Context, correlationId *string, aDto interface{}) error {
	headers := myRabbitmq.InjectAMQPHeaders(ctx)
	if correlationId != nil {
		headers[correlationIdName] = *correlationId
	}

	aType := rp.typeRegistry.GetType(aDto)

	bytea, err := json.Marshal(aDto)
	if err != nil {
		rp.lgr.ErrorContext(ctx, "Failed during marshal dto", "err", err)
		return err
	}

	msg := amqp.Publishing{
		DeliveryMode: amqp.Transient,
		Timestamp:    time.Now().UTC(),
		ContentType:  "application/json",
		Body:         bytea,
		Type:         aType,
		Headers:      headers,
	}

	if err := rp.channel.Publish(EventsFanoutExchange, "", false, false, msg); err != nil {
		rp.lgr.ErrorContext(ctx, "Error during publishing dto", "err", err)
		return err
	} else {
		return nil
	}
}

type RabbitEventsPublisher struct {
	channel      *rabbitmq.Channel
	lgr          *logger.LoggerWrapper
	typeRegistry *type_registry.TypeRegistryInstance
}

func NewRabbitEventsPublisher(lgr *logger.LoggerWrapper, connection *rabbitmq.Connection, typeRegistry *type_registry.TypeRegistryInstance) (*RabbitEventsPublisher, error) {
	cha, err := myRabbitmq.CreateRabbitMqChannel(lgr, connection)
	if err != nil {
		return nil, err
	}
	return &RabbitEventsPublisher{
		channel:      cha,
		lgr:          lgr,
		typeRegistry: typeRegistry,
	}, nil
}
