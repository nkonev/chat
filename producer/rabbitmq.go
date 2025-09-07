package producer

import (
	"context"
	"encoding/json"
	"github.com/beliyav/go-amqp-reconnect/rabbitmq"
	"github.com/streadway/amqp"
	"go-cqrs-chat-example/logger"
	myRabbitmq "go-cqrs-chat-example/rabbitmq"
	"go-cqrs-chat-example/utils"
	"time"
)

const EventsFanoutExchange = "async-events-exchange"

func (rp *RabbitEventsPublisher) Publish(ctx context.Context, aDto interface{}) error {
	headers := myRabbitmq.InjectAMQPHeaders(ctx)

	aType := utils.GetType(aDto)

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
	channel *rabbitmq.Channel
	lgr     *logger.LoggerWrapper
}

func NewRabbitEventsPublisher(lgr *logger.LoggerWrapper, connection *rabbitmq.Connection) (*RabbitEventsPublisher, error) {
	cha, err := myRabbitmq.CreateRabbitMqChannel(lgr, connection)
	if err != nil {
		return nil, err
	}
	return &RabbitEventsPublisher{
		channel: cha,
		lgr:     lgr,
	}, nil
}
