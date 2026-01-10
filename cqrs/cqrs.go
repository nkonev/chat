package cqrs

import (
	"context"
	"fmt"
	"go-cqrs-chat-example/db"
	"go-cqrs-chat-example/logger"
	"go-cqrs-chat-example/sanitizer"
	"go-cqrs-chat-example/utils"

	"github.com/IBM/sarama"
	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill-kafka/v3/pkg/kafka"
	"github.com/ThreeDotsLabs/watermill/message/router/middleware"
	"github.com/google/uuid"

	"go-cqrs-chat-example/config"
	"log/slog"
	"time"

	"github.com/ThreeDotsLabs/watermill/components/cqrs"
	"github.com/ThreeDotsLabs/watermill/message"
	wotel "github.com/nkonev/watermill-opentelemetry/pkg/opentelemetry"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.uber.org/fx"
)

func ConfigureKafkaMarshaller(
	lgr *logger.LoggerWrapper,
) kafka.MarshalerUnmarshaler {
	// This marshaler converts Watermill messages to Kafka messages.
	// We are using it to add partition key to the Kafka message.
	return kafka.NewWithPartitioningMarshaler(GenerateKafkaPartitionKey(lgr))
}

func ConfigureWatermillLogger(
	lgr *logger.LoggerWrapper,
) watermill.LoggerAdapter {
	return watermill.NewSlogLoggerWithLevelMapping(
		lgr.With("watermill", true),
		map[slog.Level]slog.Level{
			slog.LevelInfo: slog.LevelDebug,
		},
	)
}

func ConfigurePublisher(
	cfg *config.AppConfig,
	watermillLogger watermill.LoggerAdapter,
	propagator propagation.TextMapPropagator,
	tp *sdktrace.TracerProvider,
	kafkaMarshaler kafka.MarshalerUnmarshaler,
) (message.Publisher, error) {
	// You can use any Pub/Sub implementation from here: https://watermill.io/pubsubs/
	kafkaProducerConfig := sarama.NewConfig()
	kafkaProducerConfig.Producer.Retry.Max = cfg.Kafka.Producer.RetryMax
	kafkaProducerConfig.Producer.Return.Successes = cfg.Kafka.Producer.ReturnSuccess
	kafkaProducerConfig.Version = sarama.V4_1_0_0
	kafkaProducerConfig.Metadata.Retry.Backoff = cfg.Kafka.Producer.RetryBackoff
	kafkaProducerConfig.ClientID = cfg.Kafka.Producer.ClientId

	publisher, err := kafka.NewPublisher(
		kafka.PublisherConfig{
			Brokers:               cfg.Kafka.BootstrapServers,
			OverwriteSaramaConfig: kafkaProducerConfig,
			Marshaler:             kafkaMarshaler,
		},
		watermillLogger,
	)
	if err != nil {
		return nil, err
	}

	tr := tp.Tracer("chat-publisher")

	publisherDecorator := wotel.NewPublisherDecorator(publisher, wotel.WithTextMapPropagator(propagator), wotel.WithTracer(tr))

	return publisherDecorator, nil
}

func ConfigureCqrsRouter(
	lgr *logger.LoggerWrapper,
	watermillLoggerAdapter watermill.LoggerAdapter,
	propagator propagation.TextMapPropagator,
	tp *sdktrace.TracerProvider,
	cfg *config.AppConfig,
	lc fx.Lifecycle,
) (*message.Router, error) {
	// CQRS is built on messages router. Detailed documentation: https://watermill.io/docs/messages-router/
	cqrsRouter, err := message.NewRouter(message.RouterConfig{}, watermillLoggerAdapter)
	if err != nil {
		return nil, err
	}

	tr := tp.Tracer("chat-subscriber")

	// Simple middleware which will recover panics from event or command handlers.
	// More about router middlewares you can find in the documentation:
	// https://watermill.io/docs/messages-router/#middleware
	//
	// List of available middlewares you can find in message/router/middleware.
	cqrsRouter.AddMiddleware(middleware.Recoverer)
	cqrsRouter.AddMiddleware(wotel.Trace(wotel.WithTextMapPropagator(propagator), wotel.WithTracer(tr)))
	cqrsRouter.AddMiddleware(func(h message.HandlerFunc) message.HandlerFunc {
		return func(msg *message.Message) ([]*message.Message, error) {
			if cfg.Cqrs.SleepBeforeEvent > 0 {
				lgr.InfoContext(msg.Context(), "Sleeping")
				time.Sleep(cfg.Cqrs.SleepBeforeEvent)
			}

			if cfg.Cqrs.Dump {
				if cfg.Cqrs.PrettyLog && !cfg.Logger.Json {
					fmt.Printf("[kafka subscriber] Received message: trace_id=%s, metadata=%v, body: %v\n", logger.GetTraceId(msg.Context()), msg.Metadata, string(msg.Payload))
				} else {
					lgr.InfoContext(msg.Context(), fmt.Sprintf("[kafka subscriber] Received message: trace_id=%s, metadata=%v, body: %v\n", logger.GetTraceId(msg.Context()), msg.Metadata, string(msg.Payload)))
				}
			}
			return h(msg)
		}
	})

	lc.Append(fx.Hook{
		OnStop: func(ctx context.Context) error {
			lgr.Info("Stopping cqrs router")

			if err := cqrsRouter.Close(); err != nil {
				lgr.Error("Error shutting down router", "err", err)
			}
			return nil
		},
	})

	return cqrsRouter, nil
}

func RunCqrsRouter(
	lgr *logger.LoggerWrapper,
	cqrsRouter *message.Router,
	processor *cqrs.EventGroupProcessor,
) error {
	go func() {
		lgr.Info("Starting CQRS router with a given Event Processor", "eventProcessor", fmt.Sprintf("%T", processor)) // to configure it before this

		err := cqrsRouter.Run(context.Background())
		if err != nil {
			lgr.Error("Got cqrs error", "err", err)
		}
	}()
	return nil
}

func ConfigureCqrsMarshaller() *CqrsMarshalerDecorator {
	// We are decorating ProtobufMarshaler to add extra metadata to the message.
	return &CqrsMarshalerDecorator{
		cqrs.JSONMarshaler{
			NewUUID: func() string {
				uuidV7, err := uuid.NewV7()
				if err != nil {
					panic(err)
				}
				return uuidV7.String()
			},
			// It will generate topic names based on the event/command type.
			// So for example, for "RoomBooked" name will be "RoomBooked".
			GenerateName: cqrs.NamedStruct(func(v interface{}) string {
				panic(fmt.Sprintf("not implemented Name() for %T", v))
			}),
		}}
}

func ConfigureEventBus(
	lgr *logger.LoggerWrapper,
	cfg *config.AppConfig,
	publisher message.Publisher,
	cqrsMarshaler *CqrsMarshalerDecorator,
	watermillLoggerAdapter watermill.LoggerAdapter,
) (*PartitionAwareEventBus, error) {
	eventBusRoot, err := cqrs.NewEventBusWithConfig(publisher, cqrs.EventBusConfig{
		GeneratePublishTopic: func(params cqrs.GenerateEventPublishTopicParams) (string, error) {
			pm, ok := params.Event.(PartitionableMessage)
			if !ok {
				return "", fmt.Errorf("Wrong type %T of event, it should be PartitionableMessage", params.Event)
			}

			switch pm.GetEventKind() {
			case EventKindChat:
				return cfg.Kafka.Topic, nil
			case EventKindUser:
				return cfg.Kafka.Topic2, nil
			}

			return "", fmt.Errorf("Wrong kind %v of event", pm.GetEventKind())
		},
		Marshaler: cqrsMarshaler,
		Logger:    watermillLoggerAdapter,
		OnPublish: func(params cqrs.OnEventSendParams) error {
			pm, ok := params.Event.(PartitionableMessage)
			if !ok {
				return fmt.Errorf("Wrong type %T of event, it should be PartitionableMessage", params.Event)
			}

			if cfg.Cqrs.Dump {
				if cfg.Cqrs.PrettyLog && !cfg.Logger.Json {
					fmt.Printf("[kafka publisher] Sending message: trace_id=%s, kind=%v, metadata=%v, body: %v\n", logger.GetTraceId(params.Message.Context()), pm.GetEventKind(), params.Message.Metadata, string(params.Message.Payload))
				} else {
					lgr.InfoContext(params.Message.Context(), fmt.Sprintf("[kafka publisher] Sending message: trace_id=%s, kind=%v, metadata=%v, body: %v\n", logger.GetTraceId(params.Message.Context()), pm.GetEventKind(), params.Message.Metadata, string(params.Message.Payload)))
				}
			}
			return nil
		},
	})
	if err != nil {
		return nil, err
	}

	return &PartitionAwareEventBus{eventBusRoot}, nil
}

func ConfigureEventProcessor(
	cfg *config.AppConfig,
	cqrsRouter *message.Router,
	watermillLoggerAdapter watermill.LoggerAdapter,
	kafkaMarshaler kafka.MarshalerUnmarshaler,
	cqrsMarshaler *CqrsMarshalerDecorator,
	commonProjection *CommonProjection,
	cqrsEventHandler *EventHandler,
) (*cqrs.EventGroupProcessor, error) {
	kafkaConsumerConfig := sarama.NewConfig()
	kafkaConsumerConfig.Consumer.Return.Errors = cfg.Kafka.Consumer.ReturnErrors
	kafkaConsumerConfig.Version = sarama.V4_1_0_0
	kafkaConsumerConfig.ClientID = cfg.Kafka.Consumer.ClientId
	kafkaConsumerConfig.Consumer.Offsets.Initial = sarama.OffsetOldest // need for to work after import
	kafkaConsumerConfig.Consumer.Offsets.AutoCommit.Interval = cfg.Kafka.Consumer.OffsetCommitInterval

	eventProcessor, err := cqrs.NewEventGroupProcessorWithConfig(
		cqrsRouter,
		cqrs.EventGroupProcessorConfig{
			GenerateSubscribeTopic: func(params cqrs.EventGroupProcessorGenerateSubscribeTopicParams) (string, error) {
				switch params.EventGroupName {
				case cfg.Kafka.ConsumerGroup:
					return cfg.Kafka.Topic, nil

				case cfg.Kafka.ConsumerGroup2:
					return cfg.Kafka.Topic2, nil
				}
				return "", fmt.Errorf("Unknown eventGroup: %v", params.EventGroupName)
			},
			SubscriberConstructor: func(params cqrs.EventGroupProcessorSubscriberConstructorParams) (message.Subscriber, error) {
				return kafka.NewSubscriber(
					kafka.SubscriberConfig{
						Brokers:               cfg.Kafka.BootstrapServers,
						OverwriteSaramaConfig: kafkaConsumerConfig,
						ConsumerGroup:         params.EventGroupName,
						Unmarshaler:           kafkaMarshaler,
						NackResendSleep:       cfg.Kafka.Consumer.NackResendSleep,
						ReconnectRetrySleep:   cfg.Kafka.Consumer.ReconnectRetrySleep,
					},
					watermillLoggerAdapter,
				)
			},
			Marshaler: cqrsMarshaler,
			Logger:    watermillLoggerAdapter,
		},
	)
	if err != nil {
		return nil, err
	}

	// All messages from this group will have one subscription.
	// When message arrives, Watermill will match it with the correct handler.
	err = eventProcessor.AddHandlersGroup(
		cfg.Kafka.ConsumerGroup,
		cqrs.NewGroupEventHandler(cqrsEventHandler.OnChatCreated),
		cqrs.NewGroupEventHandler(cqrsEventHandler.OnChatEdited),
		cqrs.NewGroupEventHandler(cqrsEventHandler.OnChatRemoved),
		cqrs.NewGroupEventHandler(cqrsEventHandler.OnParticipantAdded),
		cqrs.NewGroupEventHandler(cqrsEventHandler.OnParticipantRemoved),
		cqrs.NewGroupEventHandler(cqrsEventHandler.OnParticipantChanged),
		cqrs.NewGroupEventHandler(cqrsEventHandler.OnChatPinned),
		cqrs.NewGroupEventHandler(cqrsEventHandler.OnChatNotificationSettingsSetted),
		cqrs.NewGroupEventHandler(cqrsEventHandler.OnMessageCreated),
		cqrs.NewGroupEventHandler(cqrsEventHandler.OnMessageEdited),
		cqrs.NewGroupEventHandler(cqrsEventHandler.OnChatViewRefreshed),
		cqrs.NewGroupEventHandler(cqrsEventHandler.OnUnreadMessageReaded),
		cqrs.NewGroupEventHandler(cqrsEventHandler.OnMessageBlogPostMade),
		cqrs.NewGroupEventHandler(cqrsEventHandler.OnMessageRemoved),
		cqrs.NewGroupEventHandler(cqrsEventHandler.OnMessageReactionFlipped),
		cqrs.NewGroupEventHandler(cqrsEventHandler.OnMessagePinned),
		cqrs.NewGroupEventHandler(cqrsEventHandler.OnMessagePublished),
		cqrs.NewGroupEventHandler(commonProjection.OnTechnicalProjectionsTruncated),
		cqrs.NewGroupEventHandler(commonProjection.OnTechnicalAbandonedChatRemoved),
	)
	if err != nil {
		return nil, err
	}

	err = eventProcessor.AddHandlersGroup(
		cfg.Kafka.ConsumerGroup2,
		cqrs.NewGroupEventHandler(cqrsEventHandler.OnUserEvented),
	)
	if err != nil {
		return nil, err
	}

	return eventProcessor, nil
}

func ConfigureCommonProjection(
	dba *db.DB,
	lgr *logger.LoggerWrapper,
	cfg *config.AppConfig,
	stripTags *sanitizer.StripTagsPolicy,
) *CommonProjection {
	return NewCommonProjection(dba, lgr, cfg, stripTags)
}

func SetIsNeedToFastForwardSequences(commonProjection *CommonProjection) error {
	return commonProjection.SetIsNeedToFastForwardSequences(context.Background())
}

func RunSequenceFastforwarder(
	lgr *logger.LoggerWrapper,
	commonProjection *CommonProjection,
	dba *db.DB,
) error {
	ctx := context.Background()

	lgr.Info("Attempting to fast-forward sequences")
	txErr := db.Transact(ctx, dba, func(tx *db.Tx) error {
		xerr := commonProjection.SetXactFastForwardSequenceLock(ctx, tx)
		if xerr != nil {
			return xerr
		}

		stillNeedFastForwardSequences, gxerr := commonProjection.GetIsNeedToFastForwardSequences(ctx, tx)
		if gxerr != nil {
			return gxerr
		}
		if !stillNeedFastForwardSequences {
			lgr.Info("Now is not need to fast-forward sequences")
			return nil
		}

		errI0 := commonProjection.InitializeChatIdSequenceIfNeed(ctx, tx)
		if errI0 != nil {
			lgr.Error("Error during setting message id sequences", "err", errI0)
			return errI0
		}

		shouldContinue := true
		for page := int64(0); shouldContinue; page++ {
			offset := utils.GetOffset(page, utils.DefaultSize)

			chatIdsPortion, errI1 := commonProjection.GetChatIds(ctx, tx, utils.DefaultSize, offset)
			if errI1 != nil {
				lgr.Error("Error during getting all chats", "err", errI1)
				return errI1
			}
			if len(chatIdsPortion) < utils.DefaultSize {
				shouldContinue = false
			}

			for _, chatId := range chatIdsPortion {
				errI2 := commonProjection.InitializeMessageIdSequenceIfNeed(ctx, tx, chatId)
				if errI2 != nil {
					lgr.Error("Error during setting message id sequences", "err", errI2)
					return errI2
				}
			}
		}

		errU := commonProjection.UnsetIsNeedToFastForwardSequences(ctx, tx)
		if errU != nil {
			lgr.Error("Error during removing need fast-forward sequences", "err", errU)
			return errU
		}

		lgr.Info("All the sequences was fast-forwarded successfully")

		return nil
	})
	if txErr != nil {
		return txErr
	}

	return nil
}
