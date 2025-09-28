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
const EventTypeMessageDeleted = "message_deleted"

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
	eventTypeUnreadMessagesChanged := EventTypeHasUnreadMessagesChanged

	eventTypeParticipantAdded := EventTypeParticipantAdded
	ctx, participantAddSpan := m.tr.Start(ctx, fmt.Sprintf("participant.%s", eventTypeParticipantAdded))
	defer participantAddSpan.End()

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

	var hasUnreadMessages = map[int64]bool{}
	hasUnreadMessages, err = m.commonProjection.GetHasUnreadMessages(ctx, userIds)
	if err != nil {
		return err
	}

	for _, cv := range chatViews {
		err = m.rabbitmqEventPublisher.Publish(ctx, event.AdditionalData.GetCorrelationId(), dto.GlobalUserEvent{
			UserId:           cv.UserId,
			EventType:        eventTypeChatCreated,
			ChatNotification: &cv,
		})
		if err != nil {
			m.lgr.ErrorContext(ctx, "Error during sending to rabbitmq", "err", err)
		}

		err = m.rabbitmqEventPublisher.Publish(ctx, event.AdditionalData.GetCorrelationId(), dto.GlobalUserEvent{
			UserId:    cv.UserId,
			EventType: eventTypeUnreadMessagesChanged,
			HasUnreadMessagesChanged: &dto.HasUnreadMessagesChanged{
				HasUnreadMessages: hasUnreadMessages[cv.UserId],
			},
		})
		if err != nil {
			m.lgr.ErrorContext(ctx, "Error during IterateOverParticipantsChatIds", "err", err)
		}
	}

	addedUsersWithAdmins := m.buildUserWithAdminBasedOnParticipantWithAdmin(event.Participants, usersMap)

	m.lgr.DebugContext(ctx, "Sending notification about the participants", "event_type", eventTypeParticipantAdded, "user_ids", userIds)

	// this is an event for ChatParticipantsModal.vue
	err = m.commonProjection.IterateOverChatParticipantIds(ctx, m.db, event.ChatId, nil, func(participantIdsPortion []int64) error {
		// for every participant of chat we send an info about the newly added participants
		for _, participantId := range participantIdsPortion {
			errInn := m.rabbitmqEventPublisher.Publish(ctx, event.AdditionalData.GetCorrelationId(), dto.ChatEvent{
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
	eventTypeParticipantDeleted := EventTypeParticipantDeleted
	ctx, participantSpan := m.tr.Start(ctx, fmt.Sprintf("participant.%s", eventTypeParticipantDeleted))
	defer participantSpan.End()

	if event.GetParticipantsType == GetParticipantsTypeNormal {
		return m.handleParticipantRemoved(ctx, event.AdditionalData, event.ParticipantIds, event.ChatId, event.BehalfUserId)
	} else if event.GetParticipantsType == GetParticipantsTypeAllInChatExcepting {
		return m.commonProjection.IterateOverChatParticipantIds(ctx, m.db, event.ChatId, nil, func(participantIdsPortion []int64) error {
			return m.handleParticipantRemoved(ctx, event.AdditionalData, participantIdsPortion, event.ChatId, event.BehalfUserId)
		})
	} else {
		return fmt.Errorf("Unknown event.GetParticipantsType = %v", event.GetParticipantsType)
	}
}

func (m *EventHandler) handleParticipantRemoved(ctx context.Context, additionalData *AdditionalData, participantIds []int64, chatId int64, behalfUserId int64) error {
	userIds := participantIds

	eventType := EventTypeChatDeleted
	eventTypeParticipantDeleted := EventTypeParticipantDeleted

	eventTypeUnreadMessagesChanged := EventTypeHasUnreadMessagesChanged
	m.lgr.DebugContext(ctx, "Sending notification about the participants", "event_type", eventTypeParticipantDeleted, "user_ids", userIds)

	var pseudoUsers = []*dto.UserWithAdmin{}
	for _, participantIdToRemove := range userIds {
		pseudoUsers = append(pseudoUsers, &dto.UserWithAdmin{
			User: dto.User{Id: participantIdToRemove},
		})
	}

	// this is an event for ChatParticipantsModal.vue
	// we send for all the participant an event about removing those
	err := m.commonProjection.IterateOverChatParticipantIds(ctx, m.db, chatId, nil, func(participantIdsPortion []int64) error {
		// for every participant of chat we send an info about the newly added participants
		for _, participantId := range participantIdsPortion {
			errInn := m.rabbitmqEventPublisher.Publish(ctx, additionalData.GetCorrelationId(), dto.ChatEvent{
				EventType:    eventTypeParticipantDeleted,
				UserId:       participantId,
				ChatId:       chatId,
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

	m.lgr.DebugContext(ctx, "Sending notification about the chat to participants", "event_type", eventType, "user_ids", userIds)

	errp := m.commonProjection.OnParticipantRemoved(ctx, additionalData, userIds, chatId, behalfUserId)
	if errp != nil {
		return errp
	}

	var hasUnreadMessages = map[int64]bool{}
	hasUnreadMessages, err = m.commonProjection.GetHasUnreadMessages(ctx, userIds)
	if err != nil {
		return err
	}

	for _, participantId := range userIds {
		err := m.rabbitmqEventPublisher.Publish(ctx, additionalData.GetCorrelationId(), dto.GlobalUserEvent{
			UserId:         participantId,
			EventType:      eventType,
			ChatDeletedDto: &dto.ChatDeletedDto{Id: chatId},
		})
		if err != nil {
			m.lgr.ErrorContext(ctx, "Error during sending to rabbitmq", "err", err)
		}

		err = m.rabbitmqEventPublisher.Publish(ctx, additionalData.GetCorrelationId(), dto.GlobalUserEvent{
			UserId:    participantId,
			EventType: eventTypeUnreadMessagesChanged,
			HasUnreadMessagesChanged: &dto.HasUnreadMessagesChanged{
				HasUnreadMessages: hasUnreadMessages[participantId],
			},
		})
		if err != nil {
			m.lgr.ErrorContext(ctx, "Error during IterateOverParticipantsChatIds", "err", err)
		}
	}
	return nil
}

func (m *EventHandler) OnParticipantChanged(ctx context.Context, event *ParticipantChanged) error {
	eventTypeParticipantChanged := EventTypeParticipantChanged
	ctx, participantAddSpan := m.tr.Start(ctx, fmt.Sprintf("participant.%s", eventTypeParticipantChanged))
	defer participantAddSpan.End()

	userIds := []int64{event.ParticipantId}

	errp := m.commonProjection.OnParticipantChanged(ctx, event)
	if errp != nil {
		return errp
	}

	usersWithAdmins, err := m.buildUserWithAdminBasedOnUserIds(ctx, userIds, event.ChatId)
	if err != nil {
		return err
	}

	m.lgr.DebugContext(ctx, "Sending notification about the participant", "event_type", eventTypeParticipantChanged, "user_ids", userIds)

	errOuter := m.commonProjection.IterateOverChatParticipantIds(ctx, m.db, event.ChatId, nil, func(participantIdsPortion []int64) error {
		for _, participantId := range participantIdsPortion {
			errInn := m.rabbitmqEventPublisher.Publish(ctx, event.AdditionalData.GetCorrelationId(), dto.ChatEvent{
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

	ctx, messageSpan := m.tr.Start(ctx, fmt.Sprintf("chat.%s", eventType))
	defer messageSpan.End()

	errOuter := m.commonProjection.IterateOverChatParticipantIds(ctx, m.db, event.ChatId, event.AllParticipantIdsExcepting, func(participantIdsPortion []int64) error {
		userIds := participantIdsPortion

		m.lgr.DebugContext(ctx, "Sending notification about the chat to participants", "event_type", eventType, "user_ids", userIds)

		errp := m.commonProjection.OnChatViewRefreshed(ctx, event.AdditionalData, participantIdsPortion, event.ChatId, event.UnreadMessagesAction, event.LastMessageAction, event.ParticipantsAction, event.IncreaseOn, event.OwnerId)
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
			err = m.rabbitmqEventPublisher.Publish(ctx, event.AdditionalData.GetCorrelationId(), dto.GlobalUserEvent{
				UserId:           cv.UserId,
				EventType:        eventType,
				ChatNotification: &cv,
			})
			if err != nil {
				m.lgr.ErrorContext(ctx, "Error during sending to rabbitmq", "err", err)
			}

			if event.UnreadMessagesAction != 0 {
				err = m.rabbitmqEventPublisher.Publish(ctx, event.AdditionalData.GetCorrelationId(), dto.GlobalUserEvent{
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
	})

	return errOuter
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
	eventType := EventTypeMessageCreated

	ctx, messageSpan := m.tr.Start(ctx, fmt.Sprintf("message.%s", eventType))
	defer messageSpan.End()

	err := m.commonProjection.OnMessageCreated(ctx, event)
	if err != nil {
		return err
	}

	m.lgr.DebugContext(ctx, "Sending notification about the message to participants", "event_type", eventType, "user_id", event.OwnerId)

	// TODO NotifyNewMessageBrowserNotification browser_notification_add_message

	errOuter := m.commonProjection.IterateOverChatParticipantIds(ctx, m.db, event.ChatId, nil, func(participantIdsPortion []int64) error {
		messageViews, errInn := m.enrichingProjection.GetMessagesEnriched(ctx, participantIdsPortion, event.ChatId, int32(len(participantIdsPortion)), nil, true, false, dto.NoSearchString, &event.Id)
		if errInn != nil {
			return errInn
		}

		for _, messageView := range messageViews {
			errInn = m.rabbitmqEventPublisher.Publish(ctx, event.AdditionalData.GetCorrelationId(), dto.ChatEvent{
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
	eventType := EventTypeMessageEdited

	ctx, messageSpan := m.tr.Start(ctx, fmt.Sprintf("message.%s", eventType))
	defer messageSpan.End()

	err := m.commonProjection.OnMessageEdited(ctx, event)
	if err != nil {
		return err
	}

	m.lgr.DebugContext(ctx, "Sending notification about the message to participants", "event_type", eventType, "user_id", event.BehalfUserId)

	errOuter := m.commonProjection.IterateOverChatParticipantIds(ctx, m.db, event.ChatId, nil, func(participantIdsPortion []int64) error {
		messageViews, errInn := m.enrichingProjection.GetMessagesEnriched(ctx, participantIdsPortion, event.ChatId, int32(len(participantIdsPortion)), nil, true, false, dto.NoSearchString, &event.Id)
		if errInn != nil {
			return errInn
		}

		for _, messageView := range messageViews {
			errInn = m.rabbitmqEventPublisher.Publish(ctx, event.AdditionalData.GetCorrelationId(), dto.ChatEvent{
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

func (m *EventHandler) OnMessageRemoved(ctx context.Context, event *MessageDeleted) error {
	eventType := EventTypeMessageDeleted

	// mc.notificator.NotifyAboutDeleteMessage(c.Request().Context(), participantIds, chatId, cd)
	ctx, messageSpan := m.tr.Start(ctx, fmt.Sprintf("message.%s", eventType))
	defer messageSpan.End()

	err := m.commonProjection.OnMessageRemoved(ctx, event)
	if err != nil {
		return err
	}

	m.lgr.DebugContext(ctx, "Sending notification about the message to participants", "event_type", eventType, "user_id", event.BehalfUserId)

	errOuter := m.commonProjection.IterateOverChatParticipantIds(ctx, m.db, event.ChatId, nil, func(participantIdsPortion []int64) error {
		for _, participantId := range participantIdsPortion {
			errInn := m.rabbitmqEventPublisher.Publish(ctx, event.AdditionalData.GetCorrelationId(), dto.ChatEvent{
				EventType: eventType,
				UserId:    participantId,
				ChatId:    event.ChatId,
				MessageDeletedNotification: &dto.MessageDeletedDto{
					Id:     event.MessageId,
					ChatId: event.ChatId,
				},
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

	ctx, messageSpan := m.tr.Start(ctx, fmt.Sprintf("message.%s", eventTypeChatUnreadMessagesChanged))
	defer messageSpan.End()

	err := m.commonProjection.OnUnreadMessageReaded(ctx, event, func(updatedChatsPortion []dto.ChatUserViewBasic) {
		if event.ReadMessagesAction != ReadMessagesActionAllChats {
			m.lgr.ErrorContext(ctx, "wrong invariant, an logic error in commonProjection.OnUnreadMessageReaded")
			return
		}

		for _, cvb := range updatedChatsPortion {
			err := m.rabbitmqEventPublisher.Publish(ctx, event.AdditionalData.GetCorrelationId(), dto.GlobalUserEvent{
				UserId:    event.ParticipantId,
				EventType: eventTypeChatUnreadMessagesChanged,
				UnreadMessagesNotification: &dto.ChatUnreadMessageChanged{
					ChatId:             cvb.ChatId,
					UnreadMessages:     cvb.UnreadMessages,
					LastUpdateDateTime: cvb.UpdateDateTime,
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
		} else {
			err = m.rabbitmqEventPublisher.Publish(ctx, event.AdditionalData.GetCorrelationId(), dto.GlobalUserEvent{
				UserId:    event.ParticipantId,
				EventType: eventTypeChatUnreadMessagesChanged,
				UnreadMessagesNotification: &dto.ChatUnreadMessageChanged{
					ChatId:             cvb.ChatId,
					UnreadMessages:     cvb.UnreadMessages,
					LastUpdateDateTime: cvb.UpdateDateTime,
				},
			})
			if err != nil {
				m.lgr.ErrorContext(ctx, "Error during sending to rabbitmq", "err", err)
			}
		}
	} else if event.ReadMessagesAction == ReadMessagesActionAllChats {
		// nothing, see the callback of commonProjection.OnUnreadMessageReaded()
	} else {
		return fmt.Errorf("Unknown action: %T", event.ReadMessagesAction)
	}

	err = m.rabbitmqEventPublisher.Publish(ctx, event.AdditionalData.GetCorrelationId(), dto.GlobalUserEvent{
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
