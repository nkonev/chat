package cqrs

import (
	"go-cqrs-chat-example/client"
	"go-cqrs-chat-example/config"
	"go-cqrs-chat-example/db"
	"go-cqrs-chat-example/logger"
	"go-cqrs-chat-example/producer"
	"go-cqrs-chat-example/sanitizer"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"
)

// performs Authorization,
// sending before events,
// mutations are made via delegating to projection
// sending after events
type EventHandler struct {
	commonProjection                    *CommonProjection
	enrichingProjection                 *EnrichingProjection
	rabbitmqOutputEventPublisher        *producer.RabbitOutputEventsPublisher
	rabbitmqNotificationEventsPublisher *producer.RabbitNotificationEventsPublisher
	db                                  *db.DB
	lgr                                 *logger.LoggerWrapper
	tr                                  trace.Tracer
	aaaRestClient                       client.AaaRestClient
	cfg                                 *config.AppConfig
	stripSourceContent                  *sanitizer.StripSourcePolicy
	stripAllTags                        *sanitizer.StripTagsPolicy
	eventBus                            *PartitionAwareEventBus
}

func NewEventHandler(commonProjection *CommonProjection, enrichingProjection *EnrichingProjection, rabbitmqEventPublisher *producer.RabbitOutputEventsPublisher, rabbitmqNotificationEventsPublisher *producer.RabbitNotificationEventsPublisher, db *db.DB, lgr *logger.LoggerWrapper, aaaRestClient client.AaaRestClient, cfg *config.AppConfig, stripSourceContent *sanitizer.StripSourcePolicy, stripAllTags *sanitizer.StripTagsPolicy, eventBus *PartitionAwareEventBus) *EventHandler {
	tr := otel.Tracer("event")

	return &EventHandler{
		commonProjection:                    commonProjection,
		enrichingProjection:                 enrichingProjection,
		rabbitmqOutputEventPublisher:        rabbitmqEventPublisher,
		rabbitmqNotificationEventsPublisher: rabbitmqNotificationEventsPublisher,
		db:                                  db,
		lgr:                                 lgr,
		tr:                                  tr,
		aaaRestClient:                       aaaRestClient,
		cfg:                                 cfg,
		stripSourceContent:                  stripSourceContent,
		stripAllTags:                        stripAllTags,
		eventBus:                            eventBus,
	}
}
