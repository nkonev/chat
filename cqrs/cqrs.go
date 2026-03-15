package cqrs

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"go-cqrs-chat-example/db"
	"go-cqrs-chat-example/logger"
	"go-cqrs-chat-example/sanitizer"
	"go-cqrs-chat-example/utils"
	"time"

	"github.com/google/uuid"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.opentelemetry.io/otel/trace"

	"go-cqrs-chat-example/config"

	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.uber.org/fx"

	"github.com/twmb/franz-go/plugin/kotel"
)

const kafkaHeaderEventType = "eventType"
const kafkaHeaderEventId = "eventId" // for debug and logging purposes

type KafkaProducer struct {
	tr  trace.Tracer
	cl  *kgo.Client
	cfg *config.AppConfig
	lgr *logger.LoggerWrapper
}

func (p *KafkaProducer) Publish(ctx context.Context, msg CqrsEvent) error {
	// Start a new span with options.
	opts := []trace.SpanStartOption{
		trace.WithSpanKind(trace.SpanKindProducer),
	}
	ctx, span := p.tr.Start(ctx, "event", opts...)
	// End the span when function exits.
	defer span.End()

	var topic string

	kind := msg.GetEventPartitioningBy()
	switch kind {
	case EventPartitioningByChatId:
		topic = p.cfg.Kafka.TopicChat.Topic
	case EventPartitioningByUserId:
		topic = p.cfg.Kafka.TopicUser.Topic
	default:
		return fmt.Errorf("Unknown kind: %v", kind)
	}

	key := msg.GetPartitionKey()

	eventType := msg.GetEventType()

	uv7, err := uuid.NewV7()
	if err != nil {
		return err
	}

	headers := []kgo.RecordHeader{
		kgo.RecordHeader{
			Key:   kafkaHeaderEventId,
			Value: []byte(uv7.String()),
		},
		kgo.RecordHeader{
			Key:   kafkaHeaderEventType,
			Value: []byte(eventType),
		},
	}

	value, err := json.Marshal(msg)
	if err != nil {
		return err
	}

	record := &kgo.Record{
		Topic:   topic,
		Key:     []byte(key),
		Headers: headers,
		Value:   value,
	}

	if p.cfg.Cqrs.Dump {
		if p.cfg.Cqrs.PrettyLog && !p.cfg.Logger.Json {
			fmt.Printf("[kafka cqrs publisher] Sending record: trace_id=%s, topic=%s, kind=%v, event_type=%v, body: %v\n", logger.GetTraceId(ctx), record.Topic, kind, eventType, string(value))
		} else {
			p.lgr.InfoContext(ctx, "[kafka cqrs publisher] Sending record:", "topic", record.Topic, "event_type", eventType, "key", string(record.Key), "event_kind", kind, "value", string(record.Value))
		}
	}

	prs := p.cl.ProduceSync(ctx, record)

	var serr error
	var aerr []error
	for i := range prs {
		if prs[i].Err != nil {
			aerr = append(aerr, prs[i].Err)
		}
	}
	serr = errors.Join(aerr...)
	if serr != nil {
		return serr
	}

	return nil
}

func ConfigurePublisher(
	lgr *logger.LoggerWrapper,
	cfg *config.AppConfig,
	tp *sdktrace.TracerProvider,
	kotelService *kotel.Kotel,
	lc fx.Lifecycle,
) (*KafkaProducer, error) {
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(cfg.Kafka.BootstrapServers...),
		kgo.WithHooks(kotelService.Hooks()...),
	)
	if err != nil {
		return nil, err
	}
	lc.Append(fx.Hook{
		OnStop: func(ctx context.Context) error {
			lgr.Info("Begin stopping kafka publisher")

			cl.Close()
			return nil
		},
	})

	tr := tp.Tracer("kafka-cqrs-publisher")

	return &KafkaProducer{tr, cl, cfg, lgr}, nil
}

type KafkaListener struct {
	lgr              *logger.LoggerWrapper
	cfg              *config.AppConfig
	cqrsEventHandler *EventHandler
	kotelService     *kotel.Kotel
	tracer           *kotel.Tracer
	lc               fx.Lifecycle
	batchOptimizer   *BatchOptimizer
}

func NewKafkaListener(
	lgr *logger.LoggerWrapper,
	cfg *config.AppConfig,
	cqrsEventHandler *EventHandler,
	kotelService *kotel.Kotel,
	tracer *kotel.Tracer,
	lc fx.Lifecycle,
	batchOptimizer *BatchOptimizer,
) *KafkaListener {
	return &KafkaListener{
		lgr:              lgr,
		cfg:              cfg,
		cqrsEventHandler: cqrsEventHandler,
		kotelService:     kotelService,
		tracer:           tracer,
		lc:               lc,
		batchOptimizer:   batchOptimizer,
	}
}

func ListenChatTopic(
	p *KafkaListener,
	lc fx.Lifecycle,
) error {
	parseFunctionMapping := map[string]func(eventId, eventType string, record *kgo.Record) (CqrsEvent, context.Context, error){
		EventChatCreated: func(eventId, eventType string, record *kgo.Record) (CqrsEvent, context.Context, error) {
			return prepareEvent[*ChatCreated](p.lgr, p.cfg, eventId, eventType, record, p.tracer)
		},
		EventChatEdited: func(eventId, eventType string, record *kgo.Record) (CqrsEvent, context.Context, error) {
			return prepareEvent[*ChatEdited](p.lgr, p.cfg, eventId, eventType, record, p.tracer)
		},
		EventChatDeleted: func(eventId, eventType string, record *kgo.Record) (CqrsEvent, context.Context, error) {
			return prepareEvent[*ChatDeleted](p.lgr, p.cfg, eventId, eventType, record, p.tracer)
		},
		// this event need to be in event-chat topic, because only this topic is backupable
		EventChatPinned: func(eventId, eventType string, record *kgo.Record) (CqrsEvent, context.Context, error) {
			return prepareEvent[*ChatPinned](p.lgr, p.cfg, eventId, eventType, record, p.tracer)
		},
		// this event need to be in event-chat topic, because only this topic is backupable
		EventChatNotificationSettingsSetted: func(eventId, eventType string, record *kgo.Record) (CqrsEvent, context.Context, error) {
			return prepareEvent[*ChatNotificationSettingsSetted](p.lgr, p.cfg, eventId, eventType, record, p.tracer)
		},
		EventChatViewRefreshed: func(eventId, eventType string, record *kgo.Record) (CqrsEvent, context.Context, error) {
			return prepareEvent[*ChatViewRefreshed](p.lgr, p.cfg, eventId, eventType, record, p.tracer)
		},
		EventParticipantsAdded: func(eventId, eventType string, record *kgo.Record) (CqrsEvent, context.Context, error) {
			return prepareEvent[*ParticipantsAdded](p.lgr, p.cfg, eventId, eventType, record, p.tracer)
		},
		EventParticipantsDeleted: func(eventId, eventType string, record *kgo.Record) (CqrsEvent, context.Context, error) {
			return prepareEvent[*ParticipantDeleted](p.lgr, p.cfg, eventId, eventType, record, p.tracer)
		},
		EventParticipantsChanged: func(eventId, eventType string, record *kgo.Record) (CqrsEvent, context.Context, error) {
			return prepareEvent[*ParticipantChanged](p.lgr, p.cfg, eventId, eventType, record, p.tracer)
		},
		EventMessageCreated: func(eventId, eventType string, record *kgo.Record) (CqrsEvent, context.Context, error) {
			return prepareEvent[*MessageCreated](p.lgr, p.cfg, eventId, eventType, record, p.tracer)
		},
		EventMessageEdited: func(eventId, eventType string, record *kgo.Record) (CqrsEvent, context.Context, error) {
			return prepareEvent[*MessageEdited](p.lgr, p.cfg, eventId, eventType, record, p.tracer)
		},
		EventMessageDeleted: func(eventId, eventType string, record *kgo.Record) (CqrsEvent, context.Context, error) {
			return prepareEvent[*MessageDeleted](p.lgr, p.cfg, eventId, eventType, record, p.tracer)
		},
		// this event need to be in event-chat topic, because only this topic is backupable
		EventMessageReaded: func(eventId, eventType string, record *kgo.Record) (CqrsEvent, context.Context, error) {
			return prepareEvent[*MessageReaded](p.lgr, p.cfg, eventId, eventType, record, p.tracer)
		},
		EventMessageBlogPostMade: func(eventId, eventType string, record *kgo.Record) (CqrsEvent, context.Context, error) {
			return prepareEvent[*MessageBlogPostMade](p.lgr, p.cfg, eventId, eventType, record, p.tracer)
		},
		EventMessageReactionFlipped: func(eventId, eventType string, record *kgo.Record) (CqrsEvent, context.Context, error) {
			return prepareEvent[*MessageReactionFlipped](p.lgr, p.cfg, eventId, eventType, record, p.tracer)
		},
		EventMessagePinned: func(eventId, eventType string, record *kgo.Record) (CqrsEvent, context.Context, error) {
			return prepareEvent[*MessagePinned](p.lgr, p.cfg, eventId, eventType, record, p.tracer)
		},
		EventMessagePublished: func(eventId, eventType string, record *kgo.Record) (CqrsEvent, context.Context, error) {
			return prepareEvent[*MessagePublished](p.lgr, p.cfg, eventId, eventType, record, p.tracer)
		},
		EventProjectionsResetted: func(eventId, eventType string, record *kgo.Record) (CqrsEvent, context.Context, error) {
			return prepareEvent[*ProjectionsTruncated](p.lgr, p.cfg, eventId, eventType, record, p.tracer)
		},
		EventTechnicalAbandonedChatRemoved: func(eventId, eventType string, record *kgo.Record) (CqrsEvent, context.Context, error) {
			return prepareEvent[*TechnicalAbandonedChatRemoved](p.lgr, p.cfg, eventId, eventType, record, p.tracer)
		},
	}

	batchFunctionMapping := map[string]func(b BatchEvent) (context.Context, error){
		EventChatCreated: func(b BatchEvent) (context.Context, error) {
			return processEvent(p.lgr, p.cfg, b, unwrapSingleBatch(p.cqrsEventHandler.OnChatCreated))
		},
		EventChatEdited: func(b BatchEvent) (context.Context, error) {
			return processEvent(p.lgr, p.cfg, b, unwrapSingleBatch(p.cqrsEventHandler.OnChatEdited))
		},
		EventChatDeleted: func(b BatchEvent) (context.Context, error) {
			return processEvent(p.lgr, p.cfg, b, unwrapSingleBatch(p.cqrsEventHandler.OnChatRemoved))
		},
		// this event need to be in event-chat topic, because only this topic is backupable
		EventChatPinned: func(b BatchEvent) (context.Context, error) {
			return processEvent(p.lgr, p.cfg, b, unwrapSingleBatch(p.cqrsEventHandler.OnChatPinned))
		},
		// this event need to be in event-chat topic, because only this topic is backupable
		EventChatNotificationSettingsSetted: func(b BatchEvent) (context.Context, error) {
			return processEvent(p.lgr, p.cfg, b, unwrapSingleBatch(p.cqrsEventHandler.OnChatNotificationSettingsSetted))
		},
		EventChatViewRefreshed: func(b BatchEvent) (context.Context, error) {
			return processEvent(p.lgr, p.cfg, b, unwrapSingleBatch(p.cqrsEventHandler.OnChatViewRefreshed))
		},
		EventParticipantsAdded: func(b BatchEvent) (context.Context, error) {
			return processEvent(p.lgr, p.cfg, b, unwrapSingleBatch(p.cqrsEventHandler.OnParticipantAdded))
		},
		EventParticipantsDeleted: func(b BatchEvent) (context.Context, error) {
			return processEvent(p.lgr, p.cfg, b, unwrapSingleBatch(p.cqrsEventHandler.OnParticipantRemoved))
		},
		EventParticipantsChanged: func(b BatchEvent) (context.Context, error) {
			return processEvent(p.lgr, p.cfg, b, unwrapSingleBatch(p.cqrsEventHandler.OnParticipantChanged))
		},

		BatchMessagesCreated: func(b BatchEvent) (context.Context, error) {
			return processEvent(p.lgr, p.cfg, b, p.cqrsEventHandler.OnBatchMessagesCreated)
		},

		EventMessageEdited: func(b BatchEvent) (context.Context, error) {
			return processEvent(p.lgr, p.cfg, b, unwrapSingleBatch(p.cqrsEventHandler.OnMessageEdited))
		},
		EventMessageDeleted: func(b BatchEvent) (context.Context, error) {
			return processEvent(p.lgr, p.cfg, b, unwrapSingleBatch(p.cqrsEventHandler.OnMessageRemoved))
		},
		// this event need to be in event-chat topic, because only this topic is backupable
		EventMessageReaded: func(b BatchEvent) (context.Context, error) {
			return processEvent(p.lgr, p.cfg, b, unwrapSingleBatch(p.cqrsEventHandler.OnUnreadMessageReaded))
		},
		EventMessageBlogPostMade: func(b BatchEvent) (context.Context, error) {
			return processEvent(p.lgr, p.cfg, b, unwrapSingleBatch(p.cqrsEventHandler.OnMessageBlogPostMade))
		},
		EventMessageReactionFlipped: func(b BatchEvent) (context.Context, error) {
			return processEvent(p.lgr, p.cfg, b, unwrapSingleBatch(p.cqrsEventHandler.OnMessageReactionFlipped))
		},
		EventMessagePinned: func(b BatchEvent) (context.Context, error) {
			return processEvent(p.lgr, p.cfg, b, unwrapSingleBatch(p.cqrsEventHandler.OnMessagePinned))
		},
		EventMessagePublished: func(b BatchEvent) (context.Context, error) {
			return processEvent(p.lgr, p.cfg, b, unwrapSingleBatch(p.cqrsEventHandler.OnMessagePublished))
		},
		EventProjectionsResetted: func(b BatchEvent) (context.Context, error) {
			return processEvent(p.lgr, p.cfg, b, unwrapSingleBatch(p.cqrsEventHandler.OnTechnicalProjectionsTruncated))
		},
		EventTechnicalAbandonedChatRemoved: func(b BatchEvent) (context.Context, error) {
			return processEvent(p.lgr, p.cfg, b, unwrapSingleBatch(p.cqrsEventHandler.OnTechnicalAbandonedChatRemoved))
		},
	}

	err := p.runKafkaListener(
		"chat-subscriber",
		p.cfg.Kafka.TopicChat.Topic,
		p.cfg.Kafka.ConsumerGroupChat,
		parseFunctionMapping,
		batchFunctionMapping,
		lc,
	)
	if err != nil {
		return err
	}

	return nil
}

func ListenUserTopic(
	p *KafkaListener,
	lc fx.Lifecycle,
) error {

	parseFunctionMapping := map[string]func(eventId, eventType string, record *kgo.Record) (CqrsEvent, context.Context, error){
		EventUserChatPinned: func(eventId, eventType string, record *kgo.Record) (CqrsEvent, context.Context, error) {
			return prepareEvent[*UserChatPinned](p.lgr, p.cfg, eventId, eventType, record, p.tracer)
		},
		EventUserChatNotificationSettingsSetted: func(eventId, eventType string, record *kgo.Record) (CqrsEvent, context.Context, error) {
			return prepareEvent[*UserChatNotificationSettingsSetted](p.lgr, p.cfg, eventId, eventType, record, p.tracer)
		},
		EventUserMessageReaded: func(eventId, eventType string, record *kgo.Record) (CqrsEvent, context.Context, error) {
			return prepareEvent[*UserMessageReaded](p.lgr, p.cfg, eventId, eventType, record, p.tracer)
		},
		// we introduced a dedicated event-user topic in order to eliminate the distributed deadlock in event_handler_chat.go::OnChatViewRefreshed(),
		// which would be due to mutating userId-partitioned chat_user_view and has_unread_messages tables from the chatId-partitioned event-chat topic
		// see also https://docs.citusdata.com/en/v13.0/reference/common_errors.html#canceling-the-transaction-since-it-was-involved-in-a-distributed-deadlock
		// https://www.cybertec-postgresql.com/en/postgresql-understanding-deadlocks/
		EventUserChatViewCreated: func(eventId, eventType string, record *kgo.Record) (CqrsEvent, context.Context, error) {
			return prepareEvent[*UserChatViewCreated](p.lgr, p.cfg, eventId, eventType, record, p.tracer)
		},
		EventUserChatViewUpdated: func(eventId, eventType string, record *kgo.Record) (CqrsEvent, context.Context, error) {
			return prepareEvent[*UserChatViewUpdated](p.lgr, p.cfg, eventId, eventType, record, p.tracer)
		},
		EventUserChatViewRemoved: func(eventId, eventType string, record *kgo.Record) (CqrsEvent, context.Context, error) {
			return prepareEvent[*UserChatViewRemoved](p.lgr, p.cfg, eventId, eventType, record, p.tracer)
		},
	}

	batchFunctionMapping := map[string]func(b BatchEvent) (context.Context, error){
		EventUserChatPinned: func(b BatchEvent) (context.Context, error) {
			return processEvent(p.lgr, p.cfg, b, unwrapSingleBatch(p.cqrsEventHandler.OnUserChatPinned))
		},
		EventUserChatNotificationSettingsSetted: func(b BatchEvent) (context.Context, error) {
			return processEvent(p.lgr, p.cfg, b, unwrapSingleBatch(p.cqrsEventHandler.OnUserChatNotificationSettingsSetted))
		},
		EventUserMessageReaded: func(b BatchEvent) (context.Context, error) {
			return processEvent(p.lgr, p.cfg, b, unwrapSingleBatch(p.cqrsEventHandler.OnUserUnreadMessageReaded))
		},
		// we introduced a dedicated event-user topic in order to eliminate the distributed deadlock in event_handler_chat.go::OnChatViewRefreshed(),
		// which would be due to mutating userId-partitioned chat_user_view and has_unread_messages tables from the chatId-partitioned event-chat topic
		// see also https://docs.citusdata.com/en/v13.0/reference/common_errors.html#canceling-the-transaction-since-it-was-involved-in-a-distributed-deadlock
		// https://www.cybertec-postgresql.com/en/postgresql-understanding-deadlocks/
		EventUserChatViewCreated: func(b BatchEvent) (context.Context, error) {
			return processEvent(p.lgr, p.cfg, b, unwrapSingleBatch(p.cqrsEventHandler.OnUserChatViewCreated))
		},
		EventUserChatViewUpdated: func(b BatchEvent) (context.Context, error) {
			return processEvent(p.lgr, p.cfg, b, unwrapSingleBatch(p.cqrsEventHandler.OnUserChatViewUpdated))
		},
		EventUserChatViewRemoved: func(b BatchEvent) (context.Context, error) {
			return processEvent(p.lgr, p.cfg, b, unwrapSingleBatch(p.cqrsEventHandler.OnUserChatViewRemoved))
		},
	}

	err := p.runKafkaListener(
		"user-subscriber",
		p.cfg.Kafka.TopicUser.Topic,
		p.cfg.Kafka.ConsumerGroupUser,
		parseFunctionMapping,
		batchFunctionMapping,
		lc,
	)
	if err != nil {
		return err
	}

	return nil
}

func unwrapSingleBatch[T CqrsEvent](
	handler func(ctx context.Context, event T) error,
) func(event BatchEvent) (context.Context, error) {
	return func(b BatchEvent) (context.Context, error) {
		sin, ok := b.(*SingleEventBatch)
		if !ok {
			return b.GetContext(), fmt.Errorf("Expected *SingleEventBatch, was: %T", b)
		}

		var t T
		t, ok = sin.event.(T)
		if !ok {
			return b.GetContext(), fmt.Errorf("Expected %T, was: %T", t, sin.event)
		}

		err := handler(sin.ctx, t)
		return sin.ctx, err
	}
}

func NewKotelTracer(tracerProvider *sdktrace.TracerProvider, pr propagation.TextMapPropagator) *kotel.Tracer {
	// Create a new kotel tracer with the provided tracer provider and
	// propagator.
	tracerOpts := []kotel.TracerOpt{
		kotel.TracerProvider(tracerProvider),
		kotel.TracerPropagator(pr),
	}
	return kotel.NewTracer(tracerOpts...)
}

func NewKotel(tracer *kotel.Tracer) *kotel.Kotel {
	kotelOps := []kotel.Opt{
		kotel.WithTracer(tracer),
	}
	return kotel.NewKotel(kotelOps...)
}

func (p *KafkaListener) runKafkaListener(
	name string,
	topic, consumerGroup string,
	parseFunctionMapping map[string]func(eventId, eventType string, record *kgo.Record) (CqrsEvent, context.Context, error),
	batchFunctionMapping map[string]func(b BatchEvent) (context.Context, error),
	lc fx.Lifecycle,
) error {
	// One client can both produce and consume!
	// Consuming can either be direct (no consumer group), or through a group. Below, we use a group.
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(p.cfg.Kafka.BootstrapServers...),
		kgo.ClientID(p.cfg.Kafka.Consumer.ClientId),
		kgo.ConsumerGroup(consumerGroup),
		kgo.ConsumeTopics(topic),
		kgo.WithHooks(p.kotelService.Hooks()...),
		kgo.BlockRebalanceOnPoll(),
		// kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()), // was need for to work after import in the previous implementation. now TestImport can work without it
		kgo.FetchMaxWait(p.cfg.Kafka.Consumer.FetchMaxWait),
	)
	if err != nil {
		return err
	}

	p.lgr.Info("Starting " + name + " subscriber")

	retryStop := make(chan struct{}, 1)
	lc.Append(fx.Hook{
		OnStop: func(ctx context.Context) error {
			p.lgr.Info("Begin stopping kafka " + name + " subscriber")

			retryStop <- struct{}{}
			cl.Close()
			return nil
		},
	})

	ctx := context.Background()

	go func() {
		for {
			// https://github.com/twmb/franz-go/blob/master/examples/group_committing/main.go
			// we use autocommit as the simplest option
			fetches := cl.PollRecords(ctx, p.cfg.Kafka.Consumer.BatchSize)
			if fetches.IsClientClosed() {
				p.lgr.Info("Client is closed, exiting " + name + " subscriber")
				return
			}

			fetches.EachError(func(to string, pa int32, err error) {
				p.lgr.Error("Got fetch error in "+name+" subscriber", "topic", to, "partition", pa, logger.AttributeError, err)
			})

			fetches.EachPartition(func(partition kgo.FetchTopicPartition) {
				records := partition.Records
				if len(records) == 0 {
					return
				}

				p.processWithRetry(partition, retryStop, name, records, parseFunctionMapping, batchFunctionMapping)
			})

			cl.AllowRebalance()
		}
	}()

	return nil
}

func (p *KafkaListener) processWithRetry(tp kgo.FetchTopicPartition, retryStop chan struct{}, name string, records []*kgo.Record, parseFunctionMapping map[string]func(eventId string, eventType string, record *kgo.Record) (CqrsEvent, context.Context, error), batchFunctionMapping map[string]func(b BatchEvent) (context.Context, error)) {
	for {
		select {
		case <-retryStop:
			p.lgr.Info("Exiting processing retrier in " + name + " subscriber")
			return
		default:
			p.lgr.Debug("got records in "+name+" subscriber", "partition", tp.Partition, "len", len(records))
			errCtx, perr := p.processEventBatch(records, name, tp, parseFunctionMapping, batchFunctionMapping)
			if perr != nil {
				if errCtx != nil {
					p.lgr.ErrorContext(errCtx, "Got error during processing in "+name+" subscriber", "topic", tp.Topic, "partition", tp.Partition, logger.AttributeError, perr)
				} else {
					p.lgr.Error("Got error during processing in "+name+" subscriber", "topic", tp.Topic, "partition", tp.Partition, logger.AttributeError, perr)
				}

				// https://github.com/twmb/franz-go/issues/590#issuecomment-1759883590
				continue // retry
			}
		}

		break
	}
}

func processEvent[T BatchEvent](lgr *logger.LoggerWrapper, cfg *config.AppConfig, batchEvent BatchEvent, handler func(event T) (context.Context, error)) (context.Context, error) {
	var ev T
	var ok bool

	ev, ok = batchEvent.(T)
	if !ok {
		return nil, fmt.Errorf("error during processing %v: expected %T, but got %T", batchEvent, ev, batchEvent)
	}

	ctx, err := handler(ev)
	if err != nil {
		return ctx, err
	}

	return ctx, nil
}

func prepareEvent[T CqrsEvent](lgr *logger.LoggerWrapper, cfg *config.AppConfig, eventId, eventType string, record *kgo.Record, tracer *kotel.Tracer) (T, context.Context, error) {
	ctx, span := tracer.WithProcessSpan(record)
	defer span.End()

	if cfg.Cqrs.SleepBeforeEvent > 0 {
		lgr.InfoContext(ctx, "Sleeping")
		time.Sleep(cfg.Cqrs.SleepBeforeEvent)
	}

	if cfg.Cqrs.Dump {
		if cfg.Cqrs.PrettyLog && !cfg.Logger.Json {
			fmt.Printf("[kafka cqrs subscriber] Processing record: trace_id=%s, topic=%s, offset=%d, partition=%d, event_id=%v, event_type=%v, body: %v\n", logger.GetTraceId(ctx), record.Topic, record.Offset, record.Partition, eventId, eventType, string(record.Value))
		} else {
			lgr.InfoContext(ctx, "[kafka cqrs subscriber] Processing record:", "topic", record.Topic, "offset", record.Offset, "partition", record.Partition, "event_id", eventId, "event_type", eventType, "key", string(record.Key), "value", string(record.Value))
		}
	}

	mi, err := parseRecord[T](record)
	if err != nil {
		lgr.ErrorContext(ctx, "Error during unmarshalling", logger.AttributeError, err)
		return mi, ctx, err
	}

	return mi, ctx, nil
}

type EventHolder struct {
	event CqrsEvent
	ctx   context.Context
}

type BatchOptimizer struct {
	lgr *logger.LoggerWrapper
}

func NewBatchOptimizer(
	lgr *logger.LoggerWrapper,
) *BatchOptimizer {
	return &BatchOptimizer{
		lgr: lgr,
	}
}

func (p *BatchOptimizer) Optimize(events []EventHolder) ([]BatchEvent, context.Context, error) {
	batchItems := []BatchEvent{}

	// we don't split by chatId (partitionKey) here, actual batchItem should care about it
	// try to append event to any of batches
	for _, eventHolder := range events {

		// try to append to any of existing batch items
		var anyConsumed = false
		for _, bi := range batchItems {
			anyConsumed = bi.TryAppend(eventHolder)
			if anyConsumed {
				break
			}
		}

		// initialize the first batch or
		// if not appended to any of batch then create a new batch
		if !anyConsumed {
			bi, ctx, err := eventHolder.MakeBatchItem()
			if err != nil {
				return nil, ctx, err
			}
			batchItems = append(batchItems, bi)
		}
	}

	if len(events) > len(batchItems) {
		p.lgr.Info(fmt.Sprintf("Batch optimizer reduced %d events into %d", len(events), len(batchItems)))
	}

	return batchItems, nil, nil
}

func (p *KafkaListener) processEventBatch(
	records []*kgo.Record, // assumes records from the one partition
	name string,
	tp kgo.FetchTopicPartition,
	parseFunctionMapping map[string]func(eventId, eventType string, record *kgo.Record) (CqrsEvent, context.Context, error),
	batchFunctionMapping map[string]func(b BatchEvent) (context.Context, error),
) (retErrCtx context.Context, retErr error) {
	// defer recover
	defer func() {
		if rerr := recover(); rerr != nil {
			ferr := fmt.Errorf("Recovered: %v", rerr)
			p.lgr.Error("In processing records panic recovered in "+name+" subscriber", "topic", tp.Topic, "partition", tp.Partition, logger.AttributeError, ferr)

			retErr = ferr
			return
		}
	}()

	events := []EventHolder{}

	for _, record := range records {
		eventId, eventType, err := parseKnownEventHeaders(record)
		if err != nil {
			retErr = err
			return
		}

		f, ok := parseFunctionMapping[eventType]
		if !ok {
			retErr = fmt.Errorf("unknown event type %v", eventType)
			return
		}
		parsedEvent, ctx, err := f(eventId, eventType, record)
		if err != nil {
			retErr = err
			retErrCtx = ctx
			return
		}
		events = append(events, EventHolder{
			event: parsedEvent,
			ctx:   ctx,
		})
	}

	batchItems, errCtx, err := p.batchOptimizer.Optimize(events)
	if err != nil {
		retErr = err
		retErrCtx = errCtx
		return
	}

	// apply batches
	for _, batchItem := range batchItems {
		f, ok := batchFunctionMapping[batchItem.GetBatchType()]
		if !ok {
			retErr = fmt.Errorf("unknown batch type %v", batchItem.GetBatchType())
			return
		}
		ctx, err := f(batchItem)
		if err != nil {
			retErr = err
			retErrCtx = ctx
			return
		}
	}

	return nil, nil
}

func parseRecord[T CqrsEvent](record *kgo.Record) (T, error) {
	var res T
	if record == nil {
		return res, errors.New("record is nil")
	}

	err := json.Unmarshal(record.Value, &res)
	if err != nil {
		return res, fmt.Errorf("error unmarshalling record %v: %w", string(record.Value), err)
	}

	return res, nil
}

func parseKnownEventHeaders(record *kgo.Record) (string, string, error) {
	if record == nil {
		return "", "", errors.New("record is nil")
	}

	var eventId, eventType string
	for i := range record.Headers {
		switch record.Headers[i].Key {
		case kafkaHeaderEventId:
			eventId = string(record.Headers[i].Value)
		case kafkaHeaderEventType:
			eventType = string(record.Headers[i].Value)
		}
	}

	if len(eventId) == 0 {
		return "", "", errors.New("no event id header found")
	}

	if len(eventType) == 0 {
		return "", "", errors.New("no event type header found")
	}

	return eventId, eventType, nil
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
			lgr.Error("Error during setting message id sequences", logger.AttributeError, errI0)
			return errI0
		}

		shouldContinue := true
		for page := int64(0); shouldContinue; page++ {
			offset := utils.GetOffset(page, utils.DefaultSize)

			chatIdsPortion, errI1 := commonProjection.GetChatIds(ctx, tx, utils.DefaultSize, offset)
			if errI1 != nil {
				lgr.Error("Error during getting all chats", logger.AttributeError, errI1)
				return errI1
			}
			if len(chatIdsPortion) < utils.DefaultSize {
				shouldContinue = false
			}

			for _, chatId := range chatIdsPortion {
				errI2 := commonProjection.InitializeMessageIdSequenceIfNeed(ctx, tx, chatId)
				if errI2 != nil {
					lgr.Error("Error during setting message id sequences", logger.AttributeError, errI2)
					return errI2
				}
			}
		}

		errU := commonProjection.UnsetIsNeedToFastForwardSequences(ctx, tx)
		if errU != nil {
			lgr.Error("Error during removing need fast-forward sequences", logger.AttributeError, errU)
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
