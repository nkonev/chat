package cqrs

import (
	"context"
	"fmt"
	"go-cqrs-chat-example/client"
	"go-cqrs-chat-example/db"
	"go-cqrs-chat-example/dto"
	"go-cqrs-chat-example/logger"
	"go-cqrs-chat-example/producer"
	"go-cqrs-chat-example/utils"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"
)

const EventTypeChatCreated = "chat_created"
const EventTypeChatEdited = "chat_edited"
const EventTypeChatDeleted = "chat_deleted"
const EventTypeParticipantAdded = "participant_added"
const EventTypeParticipantDeleted = "participant_deleted"
const EventTypeParticipantChanged = "participant_edited"
const EventTypeMessageCreated = "message_created"
const EventTypeHasUnreadMessagesChanged = "has_unread_messages_changed"
const EventTypeChatUnreadMessagesChanged = "chat_unread_messages_changed"
const EventTypeMessageEdited = "message_edited"

type EventHandler struct {
	commonProjection       *CommonProjection
	enrichingProjection    *EnrichingProjection
	rabbitmqEventPublisher *producer.RabbitEventsPublisher
	db                     *db.DB
	lgr                    *logger.LoggerWrapper
	tr                     trace.Tracer
	aaaRestClient          client.AaaRestClient
}

func NewEventHandler(commonProjection *CommonProjection, enrichingProjection *EnrichingProjection, rabbitmqEventPublisher *producer.RabbitEventsPublisher, db *db.DB, lgr *logger.LoggerWrapper, aaaRestClient client.AaaRestClient) *EventHandler {
	tr := otel.Tracer("event")

	return &EventHandler{
		commonProjection:       commonProjection,
		enrichingProjection:    enrichingProjection,
		rabbitmqEventPublisher: rabbitmqEventPublisher,
		db:                     db,
		lgr:                    lgr,
		tr:                     tr,
		aaaRestClient:          aaaRestClient,
	}
}

func (m *EventHandler) OnParticipantAdded(ctx context.Context, event *ParticipantsAdded) error {
	eventTypeChatCreated := EventTypeChatCreated
	ctx, chatAddSpan := m.tr.Start(ctx, fmt.Sprintf("chat.%s", eventTypeChatCreated))
	defer chatAddSpan.End()

	userIds := event.GetParticipantIds()
	m.lgr.DebugContext(ctx, "Sending notification about the chat to participants", "event_type", eventTypeChatCreated, "user_ids", userIds)

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

	addedUsersWithAdmins := m.buildUserWithAdminBasedOnParticipantWithAdmin(event.Participants, usersMap)

	eventTypeParticipantAdded := EventTypeParticipantAdded
	ctx, participantAddSpan := m.tr.Start(ctx, fmt.Sprintf("chat.%s", eventTypeParticipantAdded))
	defer participantAddSpan.End()
	m.lgr.DebugContext(ctx, "Sending notification about the participants", "event_type", eventTypeParticipantAdded, "user_ids", userIds)

	// this is an event for ChatParticipantsModal.vue
	err = m.commonProjection.IterateOverChatParticipantIds(ctx, m.db, event.ChatId, nil, func(participantIdsPortion []int64) error {
		// for every participant of chat we send an info about the newly added participants
		for _, participantId := range participantIdsPortion {
			errInn := m.rabbitmqEventPublisher.Publish(ctx, dto.ChatEvent{
				EventType:    eventTypeParticipantAdded,
				UserId:       participantId,
				ChatId:       event.ChatId,
				Participants: &addedUsersWithAdmins,
			})
			if errInn != nil {
				m.lgr.ErrorContext(ctx, "Error during sending to rabbitmq", "err", errInn)
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
	userIds := event.ParticipantIds

	eventTypeParticipantDeleted := EventTypeParticipantDeleted
	ctx, participantAddSpan := m.tr.Start(ctx, fmt.Sprintf("chat.%s", eventTypeParticipantDeleted))
	defer participantAddSpan.End()
	m.lgr.DebugContext(ctx, "Sending notification about the participants", "event_type", eventTypeParticipantDeleted, "user_ids", userIds)

	var pseudoUsers = []*dto.UserWithAdmin{}
	for _, participantIdToRemove := range userIds {
		pseudoUsers = append(pseudoUsers, &dto.UserWithAdmin{
			User: dto.User{Id: participantIdToRemove},
		})
	}

	// this is an event for ChatParticipantsModal.vue
	err := m.commonProjection.IterateOverChatParticipantIds(ctx, m.db, event.ChatId, nil, func(participantIdsPortion []int64) error {
		// for every participant of chat we send an info about the newly added participants
		for _, participantId := range participantIdsPortion {
			errInn := m.rabbitmqEventPublisher.Publish(ctx, dto.ChatEvent{
				EventType:    eventTypeParticipantDeleted,
				UserId:       participantId,
				ChatId:       event.ChatId,
				Participants: &pseudoUsers,
			})
			if errInn != nil {
				m.lgr.ErrorContext(ctx, "Error during sending to rabbitmq", "err", errInn)
			}
		}
		return nil
	})
	if err != nil {
		m.lgr.ErrorContext(ctx, "Error during sending to rabbitmq", "err", err)
	}

	eventType := EventTypeChatDeleted
	ctx, messageSpan := m.tr.Start(ctx, fmt.Sprintf("chat.%s", eventType))
	defer messageSpan.End()
	m.lgr.DebugContext(ctx, "Sending notification about the chat to participants", "event_type", eventType, "user_ids", userIds)

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

func (m *EventHandler) OnParticipantChanged(ctx context.Context, event *ParticipantChanged) error {
	userIds := []int64{event.ParticipantId}

	errp := m.commonProjection.OnParticipantChanged(ctx, event)
	if errp != nil {
		return errp
	}

	usersWithAdmins, err := m.buildUserWithAdminBasedOnUserIds(ctx, userIds, event.ChatId)
	if err != nil {
		return err
	}

	eventTypeParticipantChanged := EventTypeParticipantChanged
	ctx, participantAddSpan := m.tr.Start(ctx, fmt.Sprintf("chat.%s", eventTypeParticipantChanged))
	defer participantAddSpan.End()
	m.lgr.DebugContext(ctx, "Sending notification about the participant", "event_type", eventTypeParticipantChanged, "user_ids", userIds)

	errOuter := m.commonProjection.IterateOverChatParticipantIds(ctx, m.db, event.ChatId, nil, func(participantIdsPortion []int64) error {
		for _, participantId := range participantIdsPortion {
			errInn := m.rabbitmqEventPublisher.Publish(ctx, dto.ChatEvent{
				EventType:    eventTypeParticipantChanged,
				UserId:       participantId,
				ChatId:       event.ChatId,
				Participants: &usersWithAdmins,
			})
			if errInn != nil {
				m.lgr.ErrorContext(ctx, "Error during sending to rabbitmq", "err", errInn)
			}
		}

		return nil
	})
	if errOuter != nil {
		m.lgr.ErrorContext(ctx, "Error during sending to rabbitmq", "err", errOuter)
	}

	return nil
}

func (m *EventHandler) OnChatViewRefreshed(ctx context.Context, event *ChatViewRefreshed) error {
	eventType := EventTypeChatEdited
	eventTypeUnreadMessagesChanged := EventTypeHasUnreadMessagesChanged
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

	var hasUnreadMessages = map[int64]bool{}
	if event.UnreadMessagesAction != 0 {
		hasUnreadMessages, err = m.commonProjection.GetHasUnreadMessages(ctx, userIds)
		if err != nil {
			return err
		}
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

		if event.UnreadMessagesAction != 0 {
			err = m.rabbitmqEventPublisher.Publish(ctx, dto.GlobalUserEvent{
				UserId:    cv.UserId,
				EventType: eventTypeUnreadMessagesChanged,
				HasUnreadMessagesChanged: &dto.HasUnreadMessagesChanged{
					HasUnreadMessages: hasUnreadMessages[cv.UserId],
				},
			})
			if err != nil {
				m.lgr.ErrorContext(ctx, "Error during sending to rabbitmq", "err", err)
			}
		}
	}
	return nil
}

func (m *EventHandler) buildUserWithAdminBasedOnParticipantWithAdmin(participants []ParticipantWithAdmin, usersMap map[int64]*dto.User) []*dto.UserWithAdmin {
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

func (m *EventHandler) buildUserWithAdminBasedOnUserIds(ctx context.Context, userIds []int64, chatId int64) ([]*dto.UserWithAdmin, error) {
	users, err := m.aaaRestClient.GetUsers(ctx, userIds)
	if err != nil {
		m.lgr.WarnContext(ctx, "unable to get users")
	}
	usersMap := utils.ToMap(users)
	areAdmins, err := m.commonProjection.getAreAdminsOfUserIds(ctx, m.db, userIds, chatId)
	if err != nil {
		return nil, err
	}
	usersWithAdmins := make([]*dto.UserWithAdmin, 0, len(userIds))
	for _, participantId := range userIds {
		user := usersMap[participantId]
		if user != nil {
			usersWithAdmins = append(usersWithAdmins, &dto.UserWithAdmin{
				User:      *user,
				ChatAdmin: areAdmins[participantId],
			})
		}
	}
	return usersWithAdmins, nil
}

func (m *EventHandler) OnMessageCreated(ctx context.Context, event *MessageCreated) error {
	err := m.commonProjection.OnMessageCreated(ctx, event)
	if err != nil {
		return err
	}

	eventType := EventTypeMessageCreated

	m.lgr.DebugContext(ctx, "Sending notification about the message to participants", "event_type", eventType, "user_id", event.OwnerId)

	ctx, messageSpan := m.tr.Start(ctx, fmt.Sprintf("message.%s", eventType))
	defer messageSpan.End()

	// TODO NotifyNewMessageBrowserNotification browser_notification_add_message

	errOuter := m.commonProjection.IterateOverChatParticipantIds(ctx, m.db, event.ChatId, nil, func(participantIdsPortion []int64) error {
		messageViews, errInn := m.enrichingProjection.GetMessagesEnriched(ctx, participantIdsPortion, event.ChatId, int32(len(participantIdsPortion)), nil, true, false, dto.NoSearchString, &event.Id)
		if errInn != nil {
			return errInn
		}

		for _, messageView := range messageViews {
			errInn = m.rabbitmqEventPublisher.Publish(ctx, dto.ChatEvent{
				EventType:           eventType,
				UserId:              messageView.UserId,
				ChatId:              event.ChatId,
				MessageNotification: &messageView,
			})
			if errInn != nil {
				m.lgr.ErrorContext(ctx, "Error during sending to rabbitmq", "err", errInn)
			}
		}

		return nil
	})
	if errOuter != nil {
		return errOuter
	}

	return nil
}

func (m *EventHandler) OnMessageEdited(ctx context.Context, event *MessageEdited) error {
	err := m.commonProjection.OnMessageEdited(ctx, event)
	if err != nil {
		return err
	}

	eventType := EventTypeMessageEdited

	m.lgr.DebugContext(ctx, "Sending notification about the message to participants", "event_type", eventType, "user_id", event.BehalfUserId)

	ctx, messageSpan := m.tr.Start(ctx, fmt.Sprintf("message.%s", eventType))
	defer messageSpan.End()

	errOuter := m.commonProjection.IterateOverChatParticipantIds(ctx, m.db, event.ChatId, nil, func(participantIdsPortion []int64) error {
		messageViews, errInn := m.enrichingProjection.GetMessagesEnriched(ctx, participantIdsPortion, event.ChatId, int32(len(participantIdsPortion)), nil, true, false, dto.NoSearchString, &event.Id)
		if errInn != nil {
			return errInn
		}

		for _, messageView := range messageViews {
			errInn = m.rabbitmqEventPublisher.Publish(ctx, dto.ChatEvent{
				EventType:           eventType,
				UserId:              messageView.UserId,
				ChatId:              event.ChatId,
				MessageNotification: &messageView,
			})
			if errInn != nil {
				m.lgr.ErrorContext(ctx, "Error during sending to rabbitmq", "err", errInn)
			}
		}

		return nil
	})
	if errOuter != nil {
		return errOuter
	}

	return nil
}

func (m *EventHandler) OnUnreadMessageReaded(ctx context.Context, event *MessageReaded) error {
	userIds := []int64{event.ParticipantId}

	eventTypeUnreadMessagesChanged := EventTypeHasUnreadMessagesChanged
	eventTypeChatUnreadMessagesChanged := EventTypeChatUnreadMessagesChanged

	err := m.commonProjection.OnUnreadMessageReaded(ctx, event, func(updatedChatsPortion []dto.ChatUnreadResetDto) {
		if event.ReadMessagesAction != ReadMessagesActionAllChats {
			m.lgr.ErrorContext(ctx, "wrong invariant")
			return
		}

		for _, v := range updatedChatsPortion {
			err := m.rabbitmqEventPublisher.Publish(ctx, dto.GlobalUserEvent{
				UserId:    event.ParticipantId,
				EventType: eventTypeChatUnreadMessagesChanged,
				UnreadMessagesNotification: &dto.ChatUnreadMessageChanged{
					ChatId:             v.ChatId,
					UnreadMessages:     0,
					LastUpdateDateTime: v.UpdateDateTime,
				},
			})
			if err != nil {
				m.lgr.ErrorContext(ctx, "Error during sending to rabbitmq", "err", err)
			}
		}
	})
	if err != nil {
		return err
	}

	var hasUnreadMessages = map[int64]bool{}
	hasUnreadMessages, err = m.commonProjection.GetHasUnreadMessages(ctx, userIds)
	if err != nil {
		return err
	}

	if event.ReadMessagesAction == ReadMessagesActionOneMessage || event.ReadMessagesAction == ReadMessagesActionAllMessagesInOneChat {
		// not.NotifyAboutUnreadMessage(ctx, chatId, participantId, unreadMessagesByUserId[participantId], lastUpdated)
		cvb, err := m.commonProjection.GetChatUserViewBasic(ctx, m.db, event.ChatId, event.ParticipantId)
		if err != nil {
			m.lgr.ErrorContext(ctx, "Error during getting chat UserViewBasic", "err", err)
		}

		err = m.rabbitmqEventPublisher.Publish(ctx, dto.GlobalUserEvent{
			UserId:    event.ParticipantId,
			EventType: eventTypeChatUnreadMessagesChanged,
			UnreadMessagesNotification: &dto.ChatUnreadMessageChanged{
				ChatId:             event.ChatId,
				UnreadMessages:     cvb.UnreadMessages,
				LastUpdateDateTime: cvb.UpdateDateTime,
			},
		})
		if err != nil {
			m.lgr.ErrorContext(ctx, "Error during sending to rabbitmq", "err", err)
		}

	} else if event.ReadMessagesAction == ReadMessagesActionAllChats {
		// nothing
	} else {
		return fmt.Errorf("Unknown action: %T", event.ReadMessagesAction)
	}

	err = m.rabbitmqEventPublisher.Publish(ctx, dto.GlobalUserEvent{
		UserId:    event.ParticipantId,
		EventType: eventTypeUnreadMessagesChanged,
		HasUnreadMessagesChanged: &dto.HasUnreadMessagesChanged{
			HasUnreadMessages: hasUnreadMessages[event.ParticipantId],
		},
	})
	if err != nil {
		m.lgr.ErrorContext(ctx, "Error during IterateOverParticipantsChatIds", "err", err)
	}
	return nil
}
