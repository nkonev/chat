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
const EventTypeChatDeleted = "chat_deleted"
const EventTypeParticipantAdded = "participant_added"

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
	eventTypeChatCreated := EventTypeChatCreated
	userIds := event.GetParticipantIds()
	m.lgr.DebugContext(ctx, "Sending notification about the chat to participants", "event_type", eventTypeChatCreated, "user_ids", userIds)

	ctx, chatAddSpan := m.tr.Start(ctx, fmt.Sprintf("chat.%s", eventTypeChatCreated))
	defer chatAddSpan.End()

	errp := m.commonProjection.OnParticipantAdded(ctx, event)
	if errp != nil {
		return errp
	}

	// we don't need to change GetChatsEnriched to additionally process [behalf]userIds because we've already added users in our projection and the projection return all the users
	chatViews, usersMap, err := m.enrichingProjection.GetChatsEnriched(ctx, userIds, int32(len(userIds)), nil, true, false, dto.NoSearchString, &event.ChatId)
	if err != nil {
		return err
	}

	for _, cv := range chatViews {
		err = m.rabbitmqEventPublisher.Publish(ctx, dto.GlobalUserEvent{
			UserId:           cv.UserId,
			EventType:        eventTypeChatCreated,
			ChatNotification: &cv,
		})
		if err != nil {
			m.lgr.ErrorContext(ctx, "Error during sending to rabbitmq", "err", err)
		}
	}

	addedUsersWithAdmins := buildUserWithAdminBasedOnParticipantWithAdmin(event.Participants, usersMap)

	eventTypeParticipantAdded := EventTypeParticipantAdded
	m.lgr.DebugContext(ctx, "Sending notification about the participants", "event_type", eventTypeParticipantAdded, "user_ids", userIds)
	ctx, participantAddSpan := m.tr.Start(ctx, fmt.Sprintf("chat.%s", eventTypeParticipantAdded))
	defer participantAddSpan.End()

	err = m.commonProjection.IterateOverChatParticipantIds(ctx, m.db, event.ChatId, nil, func(participantIdsPortion []int64) error {
		// for every participant of chat we send an info about the newly added participants
		for _, participantId := range participantIdsPortion {
			err = m.rabbitmqEventPublisher.Publish(ctx, dto.ChatEvent{
				EventType:    eventTypeParticipantAdded,
				UserId:       participantId,
				ChatId:       event.ChatId,
				Participants: &addedUsersWithAdmins,
			})
			if err != nil {
				m.lgr.ErrorContext(ctx, "Error during sending to rabbitmq", "err", err)
			}
		}
		return nil
	})
	if err != nil {
		m.lgr.ErrorContext(ctx, "Error during sending to rabbitmq", "err", err)
	}

	return nil
}

func (m *EventHandler) OnParticipantRemoved(ctx context.Context, event *ParticipantDeleted) error {
	eventType := EventTypeChatDeleted
	userIds := event.ParticipantIds
	m.lgr.DebugContext(ctx, "Sending notification about the chat to participants", "event_type", eventType, "user_ids", userIds)

	ctx, messageSpan := m.tr.Start(ctx, fmt.Sprintf("chat.%s", eventType))
	defer messageSpan.End()

	errp := m.commonProjection.OnParticipantRemoved(ctx, event)
	if errp != nil {
		return errp
	}

	for _, participantId := range userIds {
		err := m.rabbitmqEventPublisher.Publish(ctx, dto.GlobalUserEvent{
			UserId:         participantId,
			EventType:      eventType,
			ChatDeletedDto: &dto.ChatDeletedDto{Id: event.ChatId},
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

	chatViews, _, err := m.enrichingProjection.GetChatsEnriched(ctx, userIds, int32(len(userIds)), nil, true, false, dto.NoSearchString, &event.ChatId)
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

func buildUserWithAdminBasedOnParticipantWithAdmin(participants []ParticipantWithAdmin, usersMap map[int64]*dto.User) []*dto.UserWithAdmin {
	usersWithAdmins := make([]*dto.UserWithAdmin, 0, len(participants))
	for _, p := range participants {
		user := usersMap[p.ParticipantId]
		if user != nil {
			usersWithAdmins = append(usersWithAdmins, &dto.UserWithAdmin{
				User:      *user,
				ChatAdmin: p.ChatAdmin,
			})
		}
	}

	return usersWithAdmins
}
