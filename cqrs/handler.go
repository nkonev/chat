package cqrs

import (
	"context"
	"fmt"
	"go-cqrs-chat-example/db"
	"go-cqrs-chat-example/dto"
	"go-cqrs-chat-example/logger"
	"go-cqrs-chat-example/producer"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"
)

const EventTypeChatCreated = "chat_created"
const EventTypeChatEdited = "chat_edited"

type EventHandler struct {
	commonProjection       *CommonProjection
	enrichingProjection    *EnrichingProjection
	rabbitmqEventPublisher *producer.RabbitEventsPublisher
	db                     *db.DB
	lgr                    *logger.LoggerWrapper
	tr                     trace.Tracer
}

func NewEventHandler(commonProjection *CommonProjection, enrichingProjection *EnrichingProjection, rabbitmqEventPublisher *producer.RabbitEventsPublisher, db *db.DB, lgr *logger.LoggerWrapper) *EventHandler {
	tr := otel.Tracer("event")

	return &EventHandler{
		commonProjection:       commonProjection,
		enrichingProjection:    enrichingProjection,
		rabbitmqEventPublisher: rabbitmqEventPublisher,
		db:                     db,
		lgr:                    lgr,
		tr:                     tr,
	}
}

func (m *EventHandler) OnParticipantAdded(ctx context.Context, event *ParticipantsAdded) error {
	eventType := EventTypeChatCreated
	userIds := event.GetParticipantIds()
	m.lgr.DebugContext(ctx, "Sending notification about the chat to participants", "event_type", eventType, "user_ids", userIds)

	ctx, messageSpan := m.tr.Start(ctx, fmt.Sprintf("chat.%s", eventType))
	defer messageSpan.End()

	errp := m.commonProjection.OnParticipantAdded(ctx, event)
	if errp != nil {
		return errp
	}

	chatViews, err := m.enrichingProjection.GetChatsEnriched(ctx, userIds, int32(len(userIds)), nil, true, false, dto.NoSearchString, &event.ChatId)
	if err != nil {
		return err
	}

	for _, cv := range chatViews {
		err = m.rabbitmqEventPublisher.Publish(ctx, dto.GlobalUserEvent{
			UserId:           cv.UserId,
			EventType:        eventType,
			ChatNotification: &cv,
		})
		if err != nil {
			m.lgr.ErrorContext(ctx, "Error during sending to rabbitmq", "err", err)
		}
	}
	return nil
}

func (m *EventHandler) OnChatViewRefreshed(ctx context.Context, event *ChatViewRefreshed) error {
	eventType := EventTypeChatEdited
	userIds := event.ParticipantIds
	m.lgr.DebugContext(ctx, "Sending notification about the chat to participants", "event_type", eventType, "user_ids", userIds)

	ctx, messageSpan := m.tr.Start(ctx, fmt.Sprintf("chat.%s", eventType))
	defer messageSpan.End()

	errp := m.commonProjection.OnChatViewRefreshed(ctx, event)
	if errp != nil {
		return errp
	}

	chatViews, err := m.enrichingProjection.GetChatsEnriched(ctx, userIds, int32(len(userIds)), nil, true, false, dto.NoSearchString, &event.ChatId)
	if err != nil {
		return err
	}

	for _, cv := range chatViews {
		err = m.rabbitmqEventPublisher.Publish(ctx, dto.GlobalUserEvent{
			UserId:           cv.UserId,
			EventType:        eventType,
			ChatNotification: &cv,
		})
		if err != nil {
			m.lgr.ErrorContext(ctx, "Error during sending to rabbitmq", "err", err)
		}
	}
	return nil
}
