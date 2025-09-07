package listener

import (
	"context"
	"github.com/beliyav/go-amqp-reconnect/rabbitmq"
	"github.com/streadway/amqp"
	"go-cqrs-chat-example/logger"
	"go-cqrs-chat-example/producer"
	myRabbit "go-cqrs-chat-example/rabbitmq"
	"go.uber.org/fx"
)

const ChatExchange = producer.EventsFanoutExchange
const testQueueName = "chat-event-test"

type ChatChannel struct{ *rabbitmq.Channel }

func create(lgr *logger.LoggerWrapper, name string, consumeCh *rabbitmq.Channel) (*amqp.Queue, error) {
	var err error
	var q amqp.Queue
	q, err = consumeCh.QueueDeclare(
		name,  // name
		true,  // durable - it prevents queue loss on rabbitmq restart
		false, // delete when unused
		false, // exclusive
		false, // no-wait
		nil,   // arguments
	)
	if err != nil {
		lgr.Warn("Unable to declare to queue, restarting.", "queue", name, "err", err)
		return nil, err
	}
	return &q, nil
}

func createAndBind(lgr *logger.LoggerWrapper, name string, key string, exchange string, consumeCh *rabbitmq.Channel) (*amqp.Queue, error) {
	var err error
	var q amqp.Queue
	q, err = consumeCh.QueueDeclare(
		name,  // name
		true,  // durable - it prevents queue loss on rabbitmq restart
		true,  // delete when unused
		false, // exclusive
		false, // no-wait
		nil,   // arguments
	)
	if err != nil {
		lgr.Warn("Unable to declare to queue, restarting.", "queue", name, "err", err)
		return nil, err
	}
	err = consumeCh.QueueBind(q.Name, key, exchange, false, nil)
	if err != nil {
		lgr.Warn("Unable to bind to queue, restarting.", "queue", name, "err", err)
		return nil, err
	}
	return &q, nil
}

func DeleteTestEventQueue(lgr *logger.LoggerWrapper, connection *rabbitmq.Connection) error {
	ch, err := myRabbit.CreateRabbitMqChannel(
		lgr,
		connection,
	)
	if err != nil {
		return err
	}

	lgr.Warn("Deleting test queue", "queue", testQueueName)
	_, err = ch.QueueDelete(testQueueName, false, false, false)
	if err != nil {
		lgr.Warn("An error during delete", "queue", testQueueName, "err", err)
	}
	return nil
}

func CreateAndListenTestEventChannel(lgr *logger.LoggerWrapper, connection *rabbitmq.Connection, onMessage TestEventListener, lc fx.Lifecycle, sh fx.Shutdowner) (*ChatChannel, error) {

	ch, err := myRabbit.CreateRabbitMqChannelWithCallback(
		lgr,
		connection,
		func(channel *rabbitmq.Channel) error {
			lc.Append(fx.Hook{
				OnStop: func(ctx context.Context) error {
					lgr.Info("Stopping queue listening", "queue", testQueueName)
					return channel.Close()
				},
			})

			err := channel.ExchangeDeclare(ChatExchange, "direct", true, false, false, false, nil)
			if err != nil {
				return err
			}

			aQueue, err := createAndBind(lgr, testQueueName, "", ChatExchange, channel)
			if err != nil {
				return err
			}

			listen(lgr, channel, aQueue, onMessage, sh)
			return nil
		},
	)
	if err != nil {
		return nil, err
	}

	return &ChatChannel{ch}, nil
}

func listen(
	lgr *logger.LoggerWrapper,
	channel *rabbitmq.Channel,
	queue *amqp.Queue,
	onMessage func(*amqp.Delivery) error,
	sh fx.Shutdowner) {
	lgr.Info("Listening queue", "queue", queue.Name)
	go func() {
		var deliveries <-chan amqp.Delivery
		var errOuter error
		deliveries, errOuter = channel.Consume(queue.Name, "", false, false, false, false, nil)
		if errOuter != nil {
			lgr.Error("Unable to connect to queue, restarting", "queue", queue.Name, "err", errOuter)
			sh.Shutdown()
			return
		} else {
			lgr.Info("Successfully connected to queue", "queue", queue.Name)
		}

		for msg := range deliveries {
			func() {
				defer func() {
					if err := recover(); err != nil {
						lgr.Error("In processing queue panic recovered", "queue", queue.Name, "err", err)
					}
				}()

				err := onMessage(&msg)
				if err != nil {
					lgr.Error("In processing queue error", "queue", queue.Name, "err", err)
				}
				err = msg.Ack(false)
				if err != nil {
					lgr.Error("In acking delivery for queue error", "queue", queue.Name, "err", err)
				}
			}()
		}
	}()
}
