package cqrs

import (
	"context"
	"fmt"
	"go-cqrs-chat-example/client"
	"go-cqrs-chat-example/config"
	"go-cqrs-chat-example/db"
	"go-cqrs-chat-example/dto"
	"go-cqrs-chat-example/logger"
	"go-cqrs-chat-example/preview"
	"go-cqrs-chat-example/producer"
	"go-cqrs-chat-example/sanitizer"
	"go-cqrs-chat-example/utils"
	"maps"
	"slices"

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
}

func NewEventHandler(commonProjection *CommonProjection, enrichingProjection *EnrichingProjection, rabbitmqEventPublisher *producer.RabbitOutputEventsPublisher, rabbitmqNotificationEventsPublisher *producer.RabbitNotificationEventsPublisher, db *db.DB, lgr *logger.LoggerWrapper, aaaRestClient client.AaaRestClient, cfg *config.AppConfig, stripSourceContent *sanitizer.StripSourcePolicy, stripAllTags *sanitizer.StripTagsPolicy) *EventHandler {
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
	}
}

func (m *EventHandler) OnParticipantAdded(ctx context.Context, event *ParticipantsAdded) error {
	eventTypeChatCreated := dto.EventTypeChatCreated
	eventTypeUnreadMessagesChanged := dto.EventTypeHasUnreadMessagesChanged

	eventTypeParticipantAdded := dto.EventTypeParticipantAdded
	ctx, participantAddSpan := m.tr.Start(ctx, fmt.Sprintf("participant.%s", eventTypeParticipantAdded))
	defer participantAddSpan.End()

	userIds := event.GetParticipantIds()

	adt, err := m.commonProjection.GetChatDataForAuthorization(ctx, m.db, event.AdditionalData.BehalfUserId, event.ChatId)
	if err != nil {
		return err
	}

	if !CanAddParticipant(adt.IsChatAdmin, adt.ChatIsTetATet, event.IsJoining, adt.AvailableToSearch, adt.IsBlog, event.IsChatCreating, adt.IsParticipant, adt.RegularParticipantCanAddParticipants) {
		m.lgr.InfoContext(ctx, "Skipping ParticipantsAdded because there is no authorization to do so", "chat_id", event.ChatId, "user_id", event.AdditionalData.BehalfUserId)
		return nil
	}

	errp := m.commonProjection.OnParticipantAdded(ctx, event)
	if errp != nil {
		return errp
	}

	m.lgr.DebugContext(ctx, "Sending notification about the chat to participants", "event_type", eventTypeChatCreated, "user_ids", userIds)

	// we don't need to change GetChatsEnriched to additionally process [behalf]userIds because we've already added users in our projection and the projection return all the users
	chatViews, _, err := m.enrichingProjection.GetChatsEnriched(ctx, userIds, int32(len(userIds)), nil, true, false, dto.NoSearchString, &event.ChatId, false)
	if err != nil {
		return err
	}

	var hasUnreadMessages = map[int64]bool{}
	hasUnreadMessages, err = m.commonProjection.GetHasUnreadMessages(ctx, userIds)
	if err != nil {
		return err
	}

	for _, cv := range chatViews {
		dt := dto.GlobalUserEvent{
			UserId:           cv.BehalfUserId,
			EventType:        eventTypeChatCreated,
			ChatNotification: &cv,
		}
		err = m.rabbitmqOutputEventPublisher.Publish(ctx, event.AdditionalData.GetCorrelationId(), dt)
		if err != nil {
			m.lgr.ErrorContext(ctx, "Error during sending to rabbitmq", "err", err)
		}

		err = m.rabbitmqOutputEventPublisher.Publish(ctx, event.AdditionalData.GetCorrelationId(), dto.GlobalUserEvent{
			UserId:    cv.BehalfUserId,
			EventType: eventTypeUnreadMessagesChanged,
			HasUnreadMessagesChanged: &dto.HasUnreadMessagesChanged{
				HasUnreadMessages: hasUnreadMessages[cv.BehalfUserId],
			},
		})
		if err != nil {
			m.lgr.ErrorContext(ctx, "Error during IterateOverParticipantsChatIds", "err", err)
		}
	}

	m.lgr.DebugContext(ctx, "Sending notification about the participants", "event_type", eventTypeParticipantAdded, "user_ids", userIds)

	// this is an event for ChatParticipantsModal.vue
	err = m.commonProjection.IterateOverChatParticipantIdsExcepting(ctx, m.db, event.ChatId, nil, func(participantIdsPortion []int64) error {
		participantsByBehalfs, _, errInn := m.enrichingProjection.GetParticipantsEnriched(ctx, participantIdsPortion, event.ChatId, int32(len(userIds)), utils.DefaultOffset, dto.NoSearchString, false, userIds)
		if errInn != nil {
			return errInn
		}

		sortedParticipants := slices.Sorted(maps.Keys(participantsByBehalfs))

		// for every participant of chat we send an info about the newly added participants
		for _, behalfUserId := range sortedParticipants {
			hisParticipantsViews := participantsByBehalfs[behalfUserId]
			errInn = m.rabbitmqOutputEventPublisher.Publish(ctx, event.AdditionalData.GetCorrelationId(), dto.ChatEvent{
				EventType:    eventTypeParticipantAdded,
				UserId:       behalfUserId,
				ChatId:       event.ChatId,
				Participants: &hisParticipantsViews,
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
	eventTypeParticipantDeleted := dto.EventTypeParticipantDeleted
	ctx, participantSpan := m.tr.Start(ctx, fmt.Sprintf("participant.%s", eventTypeParticipantDeleted))
	defer participantSpan.End()

	adt, err := m.commonProjection.GetChatDataForAuthorization(ctx, m.db, event.AdditionalData.BehalfUserId, event.ChatId)
	if err != nil {
		return err
	}

	isChatRemoving := event.IsChatRemoving

	for _, participantId := range event.ParticipantIds {
		if !CanRemoveParticipant(event.AdditionalData.BehalfUserId, adt.IsChatAdmin, adt.ChatIsTetATet, event.IsLeaving, adt.IsParticipant, participantId, isChatRemoving) {
			m.lgr.InfoContext(ctx, "Skipping ParticipantRemoved because there is no authorization to do so", "chat_id", event.ChatId, "user_id", event.AdditionalData.BehalfUserId)
			return nil
		}
	}

	if event.GetParticipantsType == GetParticipantsTypeNormal {
		return m.handleParticipantRemoved(ctx, event.AdditionalData, event.ParticipantIds, event.ChatId, event.AdditionalData.BehalfUserId, event.IsLeaving, isChatRemoving, adt, event.WereRemovedUsersFromAaa)
	} else if event.GetParticipantsType == GetParticipantsTypeAllInChatExcepting { // delete chat
		return m.commonProjection.IterateOverChatParticipantIdsExcepting(ctx, m.db, event.ChatId, event.AllParticipantIdsExcepting, func(participantIdsPortion []int64) error {
			return m.handleParticipantRemoved(ctx, event.AdditionalData, participantIdsPortion, event.ChatId, event.AdditionalData.BehalfUserId, event.IsLeaving, isChatRemoving, adt, event.WereRemovedUsersFromAaa)
		})
	} else {
		return fmt.Errorf("Unknown event.GetParticipantsType = %v", event.GetParticipantsType)
	}
}

func (m *EventHandler) handleParticipantRemoved(ctx context.Context, additionalData *AdditionalData, participantIds []int64, chatId int64, behalfUserId int64, isLeaving bool, isChatRemoving bool, adt dto.ChatAuthorizationData, wereRemovedUsersFromAaa bool) error {
	userIds := participantIds

	eventType := dto.EventTypeChatDeleted

	eventTypeParticipantDeleted := dto.EventTypeParticipantDeleted

	eventTypeUnreadMessagesChanged := dto.EventTypeHasUnreadMessagesChanged

	var pseudoUsers = []*dto.UserViewEnrichedDto{}
	for _, participantIdToRemove := range userIds {
		pseudoUsers = append(pseudoUsers, &dto.UserViewEnrichedDto{
			UserWithAdmin: dto.UserWithAdmin{
				User: dto.User{Id: participantIdToRemove},
			},
		})
	}

	if isChatRemoving {
		m.lgr.DebugContext(ctx, "Sending notification about the participant during chat deletion", "event_type", eventTypeParticipantDeleted, "user_ids", userIds)

		// in case chat removing no sense to send removing events to all the users (m x n), so we send it only to the removee's
		for _, participantId := range participantIds {
			errInn := m.rabbitmqOutputEventPublisher.Publish(ctx, additionalData.GetCorrelationId(), dto.ChatEvent{
				EventType:    eventTypeParticipantDeleted,
				UserId:       participantId,
				ChatId:       chatId,
				Participants: &pseudoUsers,
			})
			if errInn != nil {
				m.lgr.ErrorContext(ctx, "Error during sending to rabbitmq", "err", errInn)
			}
		}
	} else {
		m.lgr.DebugContext(ctx, "Sending notification about the participants", "event_type", eventTypeParticipantDeleted, "user_ids", userIds)

		// this is an event for ChatParticipantsModal.vue
		// we send to all the participant an event about removing removees
		err := m.commonProjection.IterateOverChatParticipantIdsExcepting(ctx, m.db, chatId, nil, func(participantIdsPortion []int64) error {
			// for every participant of chat we send an info about the newly added participants
			for _, participantId := range participantIdsPortion {
				errInn := m.rabbitmqOutputEventPublisher.Publish(ctx, additionalData.GetCorrelationId(), dto.ChatEvent{
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
	}

	m.lgr.DebugContext(ctx, "Sending notification about the chat to participants", "event_type", eventType, "user_ids", userIds)

	isPubliclyAvailable := adt.AvailableToSearch || adt.IsBlog

	if isPubliclyAvailable {
		errOuter := m.commonProjection.IterateOverChatParticipantIdsIncluding(ctx, m.db, chatId, userIds, func(participantIdsPortion []int64) error {
			chatViews, _, err := m.enrichingProjection.GetChatsEnriched(ctx, participantIdsPortion, int32(len(participantIdsPortion)), nil, true, false, dto.NoSearchString, &chatId, true)
			if err != nil {
				return err
			}
			eventTypeRedraw := dto.EventTypeChatRedraw

			for _, cv := range chatViews {
				err = m.rabbitmqOutputEventPublisher.Publish(ctx, additionalData.GetCorrelationId(), dto.GlobalUserEvent{
					UserId:           cv.BehalfUserId,
					EventType:        eventTypeRedraw,
					ChatNotification: &cv,
				})
				if err != nil {
					m.lgr.ErrorContext(ctx, "Error during sending to rabbitmq", "err", err)
				}
			}

			return nil
		})
		if errOuter != nil {
			return errOuter
		}
	}

	errp := m.commonProjection.OnParticipantRemoved(ctx, userIds, chatId, isChatRemoving, wereRemovedUsersFromAaa)
	if errp != nil {
		return errp
	}

	var hasUnreadMessages = map[int64]bool{}
	hasUnreadMessages, err := m.commonProjection.GetHasUnreadMessages(ctx, userIds)
	if err != nil {
		return err
	}

	for _, participantId := range userIds {
		if !isPubliclyAvailable {
			err = m.rabbitmqOutputEventPublisher.Publish(ctx, additionalData.GetCorrelationId(), dto.GlobalUserEvent{
				UserId:         participantId,
				EventType:      eventType,
				ChatDeletedDto: &dto.ChatDeletedDto{Id: chatId},
			})
			if err != nil {
				m.lgr.ErrorContext(ctx, "Error during sending to rabbitmq", "err", err)
			}
		}

		err = m.rabbitmqOutputEventPublisher.Publish(ctx, additionalData.GetCorrelationId(), dto.GlobalUserEvent{
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
	eventTypeParticipantChanged := dto.EventTypeParticipantEdited
	ctx, participantAddSpan := m.tr.Start(ctx, fmt.Sprintf("participant.%s", eventTypeParticipantChanged))
	defer participantAddSpan.End()

	adt, err := m.commonProjection.GetChatDataForAuthorization(ctx, m.db, event.AdditionalData.BehalfUserId, event.ChatId)
	if err != nil {
		return err
	}

	if !CanChangeParticipant(event.AdditionalData.BehalfUserId, adt.IsChatAdmin, adt.ChatIsTetATet, event.ParticipantId) {
		m.lgr.InfoContext(ctx, "Skipping ParticipantChanged because there is no authorization to do so", "chat_id", event.ChatId, "user_id", event.AdditionalData.BehalfUserId)
		return nil
	}

	userIds := []int64{event.ParticipantId}

	participantsAdminsBefore, err := m.commonProjection.getAreAdminsOfUserIds(ctx, m.db, userIds, event.ChatId)
	if err != nil {
		return err
	}

	errp := m.commonProjection.OnParticipantChanged(ctx, event)
	if errp != nil {
		return errp
	}

	m.lgr.DebugContext(ctx, "Sending notification about the participant", "event_type", eventTypeParticipantChanged, "user_ids", userIds)

	errOuter := m.commonProjection.IterateOverChatParticipantIdsExcepting(ctx, m.db, event.ChatId, nil, func(participantIdsPortion []int64) error {
		participantsByBehalfs, _, errInn := m.enrichingProjection.GetParticipantsEnriched(ctx, participantIdsPortion, event.ChatId, int32(len(userIds)), utils.DefaultOffset, dto.NoSearchString, false, userIds)
		if errInn != nil {
			return errInn
		}

		sortedParticipants := slices.Sorted(maps.Keys(participantsByBehalfs))

		for _, behalfUserId := range sortedParticipants {
			hisParticipantsViews := participantsByBehalfs[behalfUserId]
			errInn = m.rabbitmqOutputEventPublisher.Publish(ctx, event.AdditionalData.GetCorrelationId(), dto.ChatEvent{
				EventType:    eventTypeParticipantChanged,
				UserId:       behalfUserId,
				ChatId:       event.ChatId,
				Participants: &hisParticipantsViews,
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

	changedUserIds := []int64{}
	for _, participantId := range userIds {
		isAdminBefore := participantsAdminsBefore[participantId]
		isAdminAfter := event.NewAdmin
		if isChatAdminInternal(isAdminBefore) != isChatAdminInternal(isAdminAfter) {
			changedUserIds = append(changedUserIds, participantId)
		}
	}
	m.notifyMessagesReloadCommand(ctx, event.ChatId, changedUserIds, event.AdditionalData.GetCorrelationId())

	// ParticipantChanged == changing isAdmin, so CanChangeParticipant(), CanDeleteParticipant() are going to yield the different result so we forcibly refresh his ChatParticipantsModal
	m.notifyParticipantsReloadCommand(ctx, event.ChatId, changedUserIds, event.AdditionalData.GetCorrelationId())

	return nil
}

func (m *EventHandler) OnChatCreated(ctx context.Context, event *ChatCreated) error {
	// we don't check authorization for the chat creation
	err := m.commonProjection.OnChatCreated(ctx, event)
	if err != nil {
		return err
	}

	return nil
}

func (m *EventHandler) OnChatEdited(ctx context.Context, event *ChatEdited) error {
	adt, err := m.commonProjection.GetChatDataForAuthorization(ctx, m.db, event.AdditionalData.BehalfUserId, event.ChatId)
	if err != nil {
		return err
	}

	if !CanEditChat(adt.IsChatAdmin, adt.ChatIsTetATet) {
		m.lgr.InfoContext(ctx, "Skipping OnChatEdited because there is no authorization to do so", "chat_id", event.ChatId, "user_id", event.AdditionalData.BehalfUserId)
		return nil
	}

	chatBasicBefore, err := m.commonProjection.GetChatBasic(ctx, m.commonProjection.db, event.ChatId)
	if err != nil {
		return err
	}

	previousBlogAbout, err := m.commonProjection.OnChatEdited(ctx, event)
	if err != nil {
		return err
	}

	if previousBlogAbout != nil {
		errOuter := m.commonProjection.IterateOverChatParticipantIdsExcepting(ctx, m.db, event.ChatId, []int64{}, func(participantIdsPortion []int64) error {
			chatViews, _, err := m.enrichingProjection.GetChatsEnriched(ctx, participantIdsPortion, int32(len(participantIdsPortion)), nil, true, false, dto.NoSearchString, previousBlogAbout, false)
			if err != nil {
				return err
			}
			eventType := dto.EventTypeChatEdited

			for _, cv := range chatViews {
				err = m.rabbitmqOutputEventPublisher.Publish(ctx, event.AdditionalData.GetCorrelationId(), dto.GlobalUserEvent{
					UserId:           cv.BehalfUserId,
					EventType:        eventType,
					ChatNotification: &cv,
				})
				if err != nil {
					m.lgr.ErrorContext(ctx, "Error during sending to rabbitmq", "err", err)
				}
			}

			return nil
		})
		if errOuter != nil {
			return errOuter
		}
	}

	chatBasicAfter, err := m.commonProjection.GetChatBasic(ctx, m.commonProjection.db, event.ChatId)
	if err != nil {
		return err
	}

	// if any of message-related fields were changed we need to reload messages on user's side
	if canPublishMessageInternal(chatBasicBefore.RegularParticipantCanPublishMessage) != canPublishMessageInternal(chatBasicAfter.RegularParticipantCanPublishMessage) ||
		canPinMessageInternal(chatBasicBefore.RegularParticipantCanPinMessage) != canPinMessageInternal(chatBasicAfter.RegularParticipantCanPinMessage) ||
		canWriteMessageInternal(chatBasicBefore.RegularParticipantCanWriteMessage) != canWriteMessageInternal(chatBasicAfter.RegularParticipantCanWriteMessage) ||
		isBlogInternal(chatBasicBefore.IsBlog) != isBlogInternal(chatBasicAfter.IsBlog) {

		errOuter := m.commonProjection.IterateOverChatParticipantIdsExcepting(ctx, m.db, event.ChatId, nil, func(participantIdsPortion []int64) error {
			m.notifyMessagesReloadCommand(ctx, event.ChatId, participantIdsPortion, event.AdditionalData.GetCorrelationId())

			return nil
		})
		if errOuter != nil {
			return errOuter
		}

	}

	return nil
}

func (m *EventHandler) OnChatRemoved(ctx context.Context, event *ChatDeleted) error {

	// we don't check authorization here because the participants already were removed

	err := m.commonProjection.OnChatRemoved(ctx, event)
	if err != nil {
		return err
	}

	return nil
}

func (m *EventHandler) notifyMessagesReloadCommand(ctx context.Context, chatId int64, participantIds []int64, correlationId *string) {
	eventType := dto.EventTypeMessagesReload
	ctx, messageSpan := m.tr.Start(ctx, fmt.Sprintf("chat.%s", eventType))
	defer messageSpan.End()

	for _, participantId := range participantIds {
		err := m.rabbitmqOutputEventPublisher.Publish(ctx, correlationId, dto.ChatEvent{
			EventType: eventType,
			UserId:    participantId,
			ChatId:    chatId,
		})
		if err != nil {
			m.lgr.ErrorContext(ctx, "Error during sending to rabbitmq", "err", err)
		}
	}

}

func (m *EventHandler) notifyParticipantsReloadCommand(ctx context.Context, chatId int64, participantIds []int64, correlationId *string) {
	eventType := dto.EventTypeParticipantsReload
	ctx, messageSpan := m.tr.Start(ctx, fmt.Sprintf("chat.%s", eventType))
	defer messageSpan.End()

	for _, participantId := range participantIds {
		err := m.rabbitmqOutputEventPublisher.Publish(ctx, correlationId, dto.ChatEvent{
			EventType: eventType,
			UserId:    participantId,
			ChatId:    chatId,
		})
		if err != nil {
			m.lgr.ErrorContext(ctx, "Error during sending to rabbitmq", "err", err)
		}
	}
}

func (m *EventHandler) OnChatPinned(ctx context.Context, event *ChatPinned) error {
	// we don't check authorization here because all the participants can pin chat (their chat_user_view)
	err := m.commonProjection.OnChatPinned(ctx, event)
	if err != nil {
		return err
	}

	return nil
}

func (m *EventHandler) OnChatNotificationSettingsSetted(ctx context.Context, event *ChatNotificationSettingsSetted) error {
	// we don't check authorization here because all the participants can change their notification setting
	err := m.commonProjection.OnChatNotificationSettingsSetted(ctx, event)
	if err != nil {
		return err
	}

	d := dto.ChatNotificationSettingsChanged{
		ChatId:                   event.ChatId,
		ConsiderMessagesAsUnread: event.Setted,
	}

	err = m.rabbitmqOutputEventPublisher.Publish(ctx, event.AdditionalData.GetCorrelationId(), dto.GlobalUserEvent{
		UserId:                          event.AdditionalData.BehalfUserId,
		EventType:                       dto.EventTypeChatNotificationSettingsChanged,
		ChatNotificationSettingsChanged: &d,
	})
	if err != nil {
		m.lgr.ErrorContext(ctx, "Error during sending to rabbitmq", "err", err)
	}

	hasUnreadMessages, err := m.commonProjection.GetHasUnreadMessages(ctx, []int64{event.AdditionalData.BehalfUserId})
	if err != nil {
		return err
	}

	err = m.rabbitmqOutputEventPublisher.Publish(ctx, event.AdditionalData.GetCorrelationId(), dto.GlobalUserEvent{
		UserId:    event.AdditionalData.BehalfUserId,
		EventType: dto.EventTypeHasUnreadMessagesChanged,
		HasUnreadMessagesChanged: &dto.HasUnreadMessagesChanged{
			HasUnreadMessages: hasUnreadMessages[event.AdditionalData.BehalfUserId],
		},
	})
	if err != nil {
		m.lgr.ErrorContext(ctx, "Error during sending to rabbitmq", "err", err)
	}

	return nil
}

func (m *EventHandler) OnChatViewRefreshed(ctx context.Context, event *ChatViewRefreshed) error {
	eventType := dto.EventTypeChatEdited
	eventTypeUnreadMessagesChanged := dto.EventTypeHasUnreadMessagesChanged

	ctx, messageSpan := m.tr.Start(ctx, fmt.Sprintf("chat.%s", eventType))
	defer messageSpan.End()

	processParticipantsBatch := func(participantIdsPortion []int64) error {
		userIds := participantIdsPortion

		m.lgr.DebugContext(ctx, "Sending notification about the chat to participants", "event_type", eventType, "user_ids", userIds)

		errp := m.commonProjection.OnChatViewRefreshed(ctx, event.AdditionalData, participantIdsPortion, event.ChatId, event.UnreadMessagesAction, event.LastMessageAction, event.IncreaseOn, event.AdditionalData.BehalfUserId, event.ChatAction)
		if errp != nil {
			return errp
		}

		chatViews, _, err := m.enrichingProjection.GetChatsEnriched(ctx, userIds, int32(len(userIds)), nil, true, false, dto.NoSearchString, &event.ChatId, false)
		if err != nil {
			return err
		}

		var hasUnreadMessages = map[int64]bool{}
		if event.UnreadMessagesAction != UnreadMessagesActionUnspecified {
			hasUnreadMessages, err = m.commonProjection.GetHasUnreadMessages(ctx, userIds)
			if err != nil {
				return err
			}
		}

		for _, cv := range chatViews {
			err = m.rabbitmqOutputEventPublisher.Publish(ctx, event.AdditionalData.GetCorrelationId(), dto.GlobalUserEvent{
				UserId:           cv.BehalfUserId,
				EventType:        eventType,
				ChatNotification: &cv,
			})
			if err != nil {
				m.lgr.ErrorContext(ctx, "Error during sending to rabbitmq", "err", err)
			}

			if event.UnreadMessagesAction != UnreadMessagesActionUnspecified {
				err = m.rabbitmqOutputEventPublisher.Publish(ctx, event.AdditionalData.GetCorrelationId(), dto.GlobalUserEvent{
					UserId:    cv.BehalfUserId,
					EventType: eventTypeUnreadMessagesChanged,
					HasUnreadMessagesChanged: &dto.HasUnreadMessagesChanged{
						HasUnreadMessages: hasUnreadMessages[cv.BehalfUserId],
					},
				})
				if err != nil {
					m.lgr.ErrorContext(ctx, "Error during sending to rabbitmq", "err", err)
				}
			}
		}
		return nil
	}

	switch event.ParticipantsMode {
	case ParticipantsModeAllParticipantIdsExcepting:
		errOuter := m.commonProjection.IterateOverChatParticipantIdsExcepting(ctx, m.db, event.ChatId, event.AllParticipantIdsExcepting, processParticipantsBatch)
		if errOuter != nil {
			return errOuter
		}
	case ParticipantsModeOnlyParticipantIds:
		errOuter := m.commonProjection.IterateOverChatParticipantIdsIncluding(ctx, m.db, event.ChatId, event.OnlyParticipantIds, processParticipantsBatch)
		if errOuter != nil {
			return errOuter
		}
	default:
		return fmt.Errorf("Unknown constant ParticipantsMode = %v", event.ParticipantsMode)
	}

	return nil
}

func (m *EventHandler) OnMessageCreated(ctx context.Context, event *MessageCreated) error {
	eventType := dto.EventTypeMessageCreated

	ctx, messageSpan := m.tr.Start(ctx, fmt.Sprintf("message.%s", eventType))
	defer messageSpan.End()

	adt, err := m.commonProjection.GetMessageDataForAuthorization(ctx, m.db, event.AdditionalData.BehalfUserId, event.MessageCommoned.ChatId, event.MessageCommoned.Id)
	if err != nil {
		return err
	}
	if !CanWriteMessage(adt.IsParticipant, adt.IsChatAdmin, adt.ChatCanWriteMessage) {
		m.lgr.InfoContext(ctx, "Skipping OnMessageCreated because there is no authorization to do so", "chat_id", event.MessageCommoned.ChatId, "user_id", event.AdditionalData.BehalfUserId)
		return nil
	}

	err = m.commonProjection.OnMessageCreated(ctx, event)
	if err != nil {
		return err
	}

	m.lgr.DebugContext(ctx, "Sending notification about the message to participants", "event_type", eventType, "user_id", event.AdditionalData.BehalfUserId)

	chatNotificationTitle, err := m.commonProjection.getChatNameForNotification(ctx, m.db, event.MessageCommoned.ChatId)
	if err != nil {
		m.lgr.WarnContext(ctx, "Unable to get chatNotificationTitle", "chat_id", event.MessageCommoned.ChatId, "err", err)
		// nothing
	}

	newMentionedUserIds, newHasHere, newHasAll, newWithoutAnyHtml, newRepliedUserId := m.getNotificationData(ctx, event.MessageCommoned.Content, event.MessageCommoned.Embed)

	var additionalUserIdToFetch []int64 = []int64{event.AdditionalData.BehalfUserId}

	var oppositeTetATetUserId *int64
	if adt.ChatIsTetATet {
		oppositeTetATetUserId, err = m.enrichingProjection.getTetATetOpposite(ctx, m.db, event.MessageCommoned.ChatId, event.AdditionalData.BehalfUserId)
		if err != nil || oppositeTetATetUserId == nil {
			m.lgr.WarnContext(ctx, "Unable to get opposite", "chat_id", event.MessageCommoned.ChatId, "err", err)
		} else {
			additionalUserIdToFetch = append(additionalUserIdToFetch, *oppositeTetATetUserId)
		}
	}

	// for cache purposes, kinda optimization
	var behalfUserDto *dto.User

	cin, err := m.enrichingProjection.getChatInfoForMessageNotification(ctx, m.db, event.MessageCommoned.ChatId, event.AdditionalData.BehalfUserId)
	if err != nil {
		return err
	}

	errOuter := m.commonProjection.IterateOverChatParticipantIdsExcepting(ctx, m.db, event.MessageCommoned.ChatId, nil, func(participantIdsPortion []int64) error {
		messageViews, _, allPortionUsers, errInn := m.enrichingProjection.GetMessagesEnriched(ctx, participantIdsPortion, false, nil, event.MessageCommoned.ChatId, int32(len(participantIdsPortion)), nil, true, false, dto.NoSearchString, &event.MessageCommoned.Id, additionalUserIdToFetch)
		if errInn != nil {
			return errInn
		}

		allPortionUsersMap := utils.ToMap(allPortionUsers)

		cinp := m.enrichingProjection.patchChatInfoForMessageNotification(ctx, cin, allPortionUsersMap, oppositeTetATetUserId)

		behalfUserDto = allPortionUsersMap[event.AdditionalData.BehalfUserId]

		for _, messageView := range messageViews {
			// frontend event to add the message on the web page
			errInn = m.rabbitmqOutputEventPublisher.Publish(ctx, event.AdditionalData.GetCorrelationId(), dto.ChatEvent{
				EventType:           eventType,
				UserId:              messageView.UserId,
				ChatId:              event.MessageCommoned.ChatId,
				MessageNotification: &messageView,
			})
			if errInn != nil {
				m.lgr.ErrorContext(ctx, "Error during sending to rabbitmq", "err", errInn)
			}

			// notification about the new message (red dot)
			if messageView.UserId != event.AdditionalData.BehalfUserId { // skip myself
				if owner, ok := allPortionUsersMap[messageView.OwnerId]; !ok {
					m.lgr.InfoContext(ctx, "Message owner isn't found", "user_id", messageView.OwnerId)
				} else {
					err = m.rabbitmqOutputEventPublisher.Publish(ctx, event.AdditionalData.GetCorrelationId(), dto.GlobalUserEvent{
						UserId:    messageView.UserId,
						EventType: dto.EventTypeMessageBrowserNotificationAdd,
						BrowserNotification: &dto.BrowserNotification{
							ChatId:      messageView.ChatId,
							ChatName:    cinp.ChatName,
							ChatAvatar:  cinp.ChatAvatar,
							MessageId:   messageView.Id,
							MessageText: newWithoutAnyHtml,
							OwnerId:     owner.Id,
							OwnerLogin:  owner.Login,
						},
					})
					if err != nil {
						m.lgr.ErrorContext(ctx, "Error during sending to rabbitmq", "err", errInn)
					}
				}
			}
		}

		newToSendMentions := m.prepareMentionParticipantIds(ctx, newHasAll, newHasHere, newMentionedUserIds, participantIdsPortion)

		if behalfUserDto == nil {
			m.lgr.InfoContext(ctx, "Unable to get behalf user for mention notification", "user_id", event.AdditionalData.BehalfUserId)
		} else {
			for _, participantId := range newToSendMentions {
				if participantId == event.AdditionalData.BehalfUserId {
					continue // skip myself
				}

				errInn = m.rabbitmqNotificationEventsPublisher.Publish(ctx, event.AdditionalData.GetCorrelationId(), dto.NotificationEvent{
					EventType: dto.EventTypeMentionAdded,
					UserId:    participantId,
					ChatId:    event.MessageCommoned.ChatId,
					MentionNotification: &dto.MentionNotification{
						Id:   event.MessageCommoned.Id,
						Text: newWithoutAnyHtml,
					},
					ByUserId:  behalfUserDto.Id,
					ByLogin:   behalfUserDto.Login,
					ByAvatar:  behalfUserDto.Avatar,
					ChatTitle: chatNotificationTitle,
				})
				if errInn != nil {
					m.lgr.ErrorContext(ctx, "Error during sending to rabbitmq", "err", errInn)
				}
			}
		}

		return nil
	})
	if errOuter != nil {
		return errOuter
	}

	if newRepliedUserId != nil {
		if behalfUserDto == nil {
			m.lgr.InfoContext(ctx, "Unable to get behalf user for reply notification", "user_id", event.AdditionalData.BehalfUserId)
		} else {
			if *newRepliedUserId != event.AdditionalData.BehalfUserId { // skip myself
				err = m.rabbitmqNotificationEventsPublisher.Publish(ctx, event.AdditionalData.GetCorrelationId(), dto.NotificationEvent{
					EventType: dto.EventTypeReplyAdded,
					UserId:    *newRepliedUserId,
					ChatId:    event.MessageCommoned.ChatId,
					ReplyNotification: &dto.ReplyDto{
						MessageId:        event.MessageCommoned.Id,
						ChatId:           event.MessageCommoned.ChatId,
						ReplyableMessage: newWithoutAnyHtml,
					},
					ByUserId:  behalfUserDto.Id,
					ByLogin:   behalfUserDto.Login,
					ByAvatar:  behalfUserDto.Avatar,
					ChatTitle: chatNotificationTitle,
				})
				if err != nil {
					m.lgr.ErrorContext(ctx, "Error during sending to rabbitmq", "err", err)
				}
			}
		}
	}

	return nil
}

func (m *EventHandler) prepareMentionParticipantIds(ctx context.Context, newHasAll, newHasHere bool, newMentionedUserIds []int64, participantIdsPortion []int64) []int64 {
	newToSendMentions := []int64{}

	newMentionedUserIdsMap := utils.SliceToSetMapIdStruct(newMentionedUserIds)

	// see also cqrs/projection_message.go :: parseMentionUserIdsFromMessageHtml()
	if newHasAll {
		newToSendMentions = append(newToSendMentions, participantIdsPortion...)
	} else if newHasHere {
		userOnlines, err := m.aaaRestClient.GetOnlines(ctx, participantIdsPortion) // get online for opposite user
		if err != nil {
			m.lgr.WarnContext(ctx, "Unable to get online for", "user_ids", participantIdsPortion, "err", err)
			// nothing
		}
		for _, uo := range userOnlines {
			newToSendMentions = append(newToSendMentions, uo.Id)
		}
	} else {
		for _, pi := range participantIdsPortion {
			if _, ok := newMentionedUserIdsMap[pi]; ok {
				newToSendMentions = append(newToSendMentions, pi)
			}
		}
	}

	return newToSendMentions
}

func (m *EventHandler) OnMessageEdited(ctx context.Context, event *MessageEdited) error {
	eventType := dto.EventTypeMessageEdited

	ctx, messageSpan := m.tr.Start(ctx, fmt.Sprintf("message.%s", eventType))
	defer messageSpan.End()

	adt, err := m.commonProjection.GetMessageDataForAuthorization(ctx, m.db, event.AdditionalData.BehalfUserId, event.MessageCommoned.ChatId, event.MessageCommoned.Id)
	if err != nil {
		return err
	}

	canWriteMessage := CanWriteMessage(adt.IsParticipant, adt.IsChatAdmin, adt.ChatCanWriteMessage)

	if event.IsEmbedSync {
		if !CanSyncEmbedMessage(event.AdditionalData.BehalfUserId, adt.MessageOwnerId, adt.HasEmbedMessage, canWriteMessage) {
			m.lgr.InfoContext(ctx, "Skipping OnMessageEdited because there is no authorization to do so (sync)", "chat_id", event.MessageCommoned.ChatId, "user_id", event.AdditionalData.BehalfUserId)
			return nil
		}
	} else {
		if !CanEditMessage(event.AdditionalData.BehalfUserId, adt.MessageOwnerId, adt.HasEmbedMessage, adt.EmbedMessageTypeSafe, canWriteMessage) {
			m.lgr.InfoContext(ctx, "Skipping OnMessageEdited because there is no authorization to do so (edit)", "chat_id", event.MessageCommoned.ChatId, "user_id", event.AdditionalData.BehalfUserId)
			return nil
		}
	}

	messageBasicOld, err := m.commonProjection.GetMessageWithEmbed(ctx, m.db, event.MessageCommoned.ChatId, event.MessageCommoned.Id)
	if err != nil {
		return err
	}

	oldMentionedUserIds, oldHasHere, oldHasAll, _, oldRepliedUserId := m.getNotificationData(ctx, messageBasicOld.GetContentOrEmpty(), messageBasicOld.GetEmbed())
	oldMentionedUserIdsMap := utils.SliceToSetMapIdStruct(oldMentionedUserIds)

	err = m.commonProjection.OnMessageEdited(ctx, event)
	if err != nil {
		return err
	}

	m.lgr.DebugContext(ctx, "Sending notification about the message to participants", "event_type", eventType, "user_id", event.AdditionalData.BehalfUserId)

	chatNotificationTitle, err := m.commonProjection.getChatNameForNotification(ctx, m.db, event.MessageCommoned.ChatId)
	if err != nil {
		m.lgr.WarnContext(ctx, "Unable to get chatNotificationTitle", "chat_id", event.MessageCommoned.ChatId, "err", err)
		// nothing
	}

	newMentionedUserIds, newHasHere, newHasAll, newWithoutAnyHtml, newRepliedUserId := m.getNotificationData(ctx, event.MessageCommoned.Content, event.MessageCommoned.Embed)
	newMentionedUserIdsMap := utils.SliceToSetMapIdStruct(newMentionedUserIds)

	// for cache purposes, kinda optimization
	var behalfUserDto *dto.User

	addedMentionedUserIds := []int64{}
	removedMentionedUserIds := []int64{}

	var addedRepliedUserId *int64
	var removedRepliedUserId *int64

	for newUserId := range newMentionedUserIdsMap {
		if _, ok := oldMentionedUserIdsMap[newUserId]; !ok {
			addedMentionedUserIds = append(addedMentionedUserIds, newUserId)
		}
	}

	for oldUserId := range oldMentionedUserIdsMap {
		if _, ok := newMentionedUserIdsMap[oldUserId]; !ok {
			removedMentionedUserIds = append(removedMentionedUserIds, oldUserId)
		}
	}

	if newRepliedUserId != nil && oldRepliedUserId != nil {
		if *newRepliedUserId != *oldRepliedUserId {
			addedRepliedUserId = newRepliedUserId
			removedRepliedUserId = oldRepliedUserId
		}
	} else if newRepliedUserId != nil {
		addedRepliedUserId = newRepliedUserId
	} else if oldRepliedUserId != nil {
		removedRepliedUserId = oldRepliedUserId
	}

	addedHasAll := !oldHasAll && newHasAll
	addedHasHere := !oldHasHere && newHasHere

	removedHasAll := oldHasAll && !newHasAll
	removedHasHere := oldHasHere && !newHasHere

	var additionalUserIdToFetch []int64 = []int64{event.AdditionalData.BehalfUserId}

	errOuter := m.commonProjection.IterateOverChatParticipantIdsExcepting(ctx, m.db, event.MessageCommoned.ChatId, nil, func(participantIdsPortion []int64) error {
		messageViews, _, allPortionUsers, errInn := m.enrichingProjection.GetMessagesEnriched(ctx, participantIdsPortion, false, nil, event.MessageCommoned.ChatId, int32(len(participantIdsPortion)), nil, true, false, dto.NoSearchString, &event.MessageCommoned.Id, additionalUserIdToFetch)
		if errInn != nil {
			return errInn
		}

		allPortionUsersMap := utils.ToMap(allPortionUsers)
		behalfUserDto = allPortionUsersMap[event.AdditionalData.BehalfUserId]

		for _, messageView := range messageViews {
			errInn = m.rabbitmqOutputEventPublisher.Publish(ctx, event.AdditionalData.GetCorrelationId(), dto.ChatEvent{
				EventType:           eventType,
				UserId:              messageView.UserId,
				ChatId:              event.MessageCommoned.ChatId,
				MessageNotification: &messageView,
			})
			if errInn != nil {
				m.lgr.ErrorContext(ctx, "Error during sending to rabbitmq", "err", errInn)
			}
		}

		addedToSendMentions := m.prepareMentionParticipantIds(ctx, addedHasAll, addedHasHere, addedMentionedUserIds, participantIdsPortion)
		removedToSendMentions := m.prepareMentionParticipantIds(ctx, removedHasAll, removedHasHere, removedMentionedUserIds, participantIdsPortion)

		if behalfUserDto == nil {
			m.lgr.InfoContext(ctx, "Unable to get behalf user for mention notification", "user_id", event.AdditionalData.BehalfUserId)
		} else {

			// add notification
			for _, participantId := range addedToSendMentions {
				if participantId == event.AdditionalData.BehalfUserId {
					continue // skip myself
				}

				errInn = m.rabbitmqNotificationEventsPublisher.Publish(ctx, event.AdditionalData.GetCorrelationId(), dto.NotificationEvent{
					EventType: dto.EventTypeMentionAdded,
					UserId:    participantId,
					ChatId:    event.MessageCommoned.ChatId,
					MentionNotification: &dto.MentionNotification{
						Id:   event.MessageCommoned.Id,
						Text: newWithoutAnyHtml,
					},
					ByUserId:  behalfUserDto.Id,
					ByLogin:   behalfUserDto.Login,
					ByAvatar:  behalfUserDto.Avatar,
					ChatTitle: chatNotificationTitle,
				})
				if errInn != nil {
					m.lgr.ErrorContext(ctx, "Error during sending to rabbitmq", "err", errInn)
				}
			}

			// remove notification
			for _, participantId := range removedToSendMentions {
				if participantId == event.AdditionalData.BehalfUserId {
					continue // skip myself
				}

				errInn = m.rabbitmqNotificationEventsPublisher.Publish(ctx, event.AdditionalData.GetCorrelationId(), dto.NotificationEvent{
					EventType: dto.EventTypeMentionDeleted,
					UserId:    participantId,
					ChatId:    event.MessageCommoned.ChatId,
					MentionNotification: &dto.MentionNotification{
						Id: event.MessageCommoned.Id,
					},
				})
				if errInn != nil {
					m.lgr.ErrorContext(ctx, "Error during sending to rabbitmq", "err", errInn)
				}
			}
		}

		return nil
	})
	if errOuter != nil {
		return errOuter
	}

	if addedRepliedUserId != nil {
		if behalfUserDto == nil {
			m.lgr.InfoContext(ctx, "Unable to get behalf user for reply notification", "user_id", event.AdditionalData.BehalfUserId)
		} else {
			if *addedRepliedUserId != event.AdditionalData.BehalfUserId { // skip myself
				err = m.rabbitmqNotificationEventsPublisher.Publish(ctx, event.AdditionalData.GetCorrelationId(), dto.NotificationEvent{
					EventType: dto.EventTypeReplyAdded,
					UserId:    *addedRepliedUserId,
					ChatId:    event.MessageCommoned.ChatId,
					ReplyNotification: &dto.ReplyDto{
						MessageId:        event.MessageCommoned.Id,
						ChatId:           event.MessageCommoned.ChatId,
						ReplyableMessage: newWithoutAnyHtml,
					},
					ByUserId:  behalfUserDto.Id,
					ByLogin:   behalfUserDto.Login,
					ByAvatar:  behalfUserDto.Avatar,
					ChatTitle: chatNotificationTitle,
				})
				if err != nil {
					m.lgr.ErrorContext(ctx, "Error during sending to rabbitmq", "err", err)
				}
			}
		}
	}

	if removedRepliedUserId != nil {
		if behalfUserDto == nil {
			m.lgr.InfoContext(ctx, "Unable to get behalf user for reply notification", "user_id", event.AdditionalData.BehalfUserId)
		} else {

			err = m.rabbitmqNotificationEventsPublisher.Publish(ctx, event.AdditionalData.GetCorrelationId(), dto.NotificationEvent{
				EventType: dto.EventTypeReplyDeleted,
				UserId:    *removedRepliedUserId,
				ChatId:    event.MessageCommoned.ChatId,
				ReplyNotification: &dto.ReplyDto{
					MessageId: event.MessageCommoned.Id,
					ChatId:    event.MessageCommoned.ChatId,
				},
			})
			if err != nil {
				m.lgr.ErrorContext(ctx, "Error during sending to rabbitmq", "err", err)
			}
		}
	}

	return nil
}

func (m *EventHandler) getNotificationData(ctx context.Context, messageHtml string, em dto.Embeddable) ([]int64, bool, bool, string, *int64) {
	newWithoutSourceTags := m.stripSourceContent.Sanitize(messageHtml)
	newMentionedUserIds, newHasHere, newHasAll := m.enrichingProjection.parseMentionUserIdsFromMessageHtml(ctx, newWithoutSourceTags)

	newWithoutAnyHtml := m.stripAllTags.Sanitize(newWithoutSourceTags)
	newWithoutAnyHtml = preview.CreateMessagePreviewWithoutLogin(m.stripAllTags, m.cfg.Message.PreviewMaxTextSize, newWithoutAnyHtml)

	var repliedUserId *int64
	if em != nil {
		if reply, ok := em.(*dto.EmbedReply); ok {
			repliedUserId = &reply.OwnerId
		}
	}

	return newMentionedUserIds, newHasHere, newHasAll, newWithoutAnyHtml, repliedUserId
}

func (m *EventHandler) OnMessageRemoved(ctx context.Context, event *MessageDeleted) error {
	eventType := dto.EventTypeMessageDeleted

	ctx, messageSpan := m.tr.Start(ctx, fmt.Sprintf("message.%s", eventType))
	defer messageSpan.End()

	adt, err := m.commonProjection.GetMessageDataForAuthorization(ctx, m.db, event.AdditionalData.BehalfUserId, event.ChatId, event.MessageId)
	if err != nil {
		return err
	}
	if !CanDeleteMessage(event.AdditionalData.BehalfUserId, adt.MessageOwnerId, adt.ChatCanWriteMessage) {
		m.lgr.InfoContext(ctx, "Skipping OnMessageRemoved because there is no authorization to do so", "chat_id", event.ChatId, "user_id", event.AdditionalData.BehalfUserId)
		return nil
	}

	messageBasic, err := m.commonProjection.GetMessageBasic(ctx, m.db, event.ChatId, event.MessageId)
	if err != nil {
		return err
	}

	reactions, err := m.commonProjection.GetReactionsOnMessage(ctx, m.db, event.ChatId, event.MessageId)
	if err != nil {
		return err
	}

	err = m.commonProjection.OnMessageRemoved(ctx, event)
	if err != nil {
		return err
	}

	m.lgr.DebugContext(ctx, "Sending notification about the message to participants", "event_type", eventType, "user_id", event.AdditionalData.BehalfUserId)

	errOuter := m.commonProjection.IterateOverChatParticipantIdsExcepting(ctx, m.db, event.ChatId, nil, func(participantIdsPortion []int64) error {
		for _, participantId := range participantIdsPortion {
			errInn := m.rabbitmqOutputEventPublisher.Publish(ctx, event.AdditionalData.GetCorrelationId(), dto.ChatEvent{
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

			err = m.rabbitmqNotificationEventsPublisher.Publish(ctx, event.AdditionalData.GetCorrelationId(), dto.NotificationEvent{
				EventType: dto.EventTypeMentionDeleted,
				UserId:    participantId,
				ChatId:    event.ChatId,
				MentionNotification: &dto.MentionNotification{
					Id: event.MessageId,
				},
			})
			if err != nil {
				m.lgr.ErrorContext(ctx, "Error during sending to rabbitmq", "err", err)
			}

			err = m.rabbitmqNotificationEventsPublisher.Publish(ctx, event.AdditionalData.GetCorrelationId(), dto.NotificationEvent{
				EventType: dto.EventTypeReplyDeleted,
				UserId:    participantId,
				ChatId:    event.ChatId,
				ReplyNotification: &dto.ReplyDto{
					MessageId: event.MessageId,
					ChatId:    event.ChatId,
				},
			})
			if err != nil {
				m.lgr.ErrorContext(ctx, "Error during sending to rabbitmq", "err", err)
			}

			for _, reaction := range reactions {
				var messageOwnerId = messageBasic.GetOwnerId()
				if messageOwnerId == dto.NoOwner || messageOwnerId == dto.NoId {
					m.lgr.InfoContext(ctx, "Unable to get message owner for reaction notification")
				} else {
					err = m.rabbitmqNotificationEventsPublisher.Publish(ctx, event.AdditionalData.GetCorrelationId(), dto.NotificationEvent{
						EventType: dto.EventTypeReactionDeleted,
						ReactionEvent: &dto.ReactionEvent{
							Reaction:  reaction,
							MessageId: event.MessageId,
						},
						UserId: messageOwnerId,
						ChatId: event.ChatId,
					})
					if err != nil {
						m.lgr.ErrorContext(ctx, "Error during sending to rabbitmq", "err", err)
					}
				}
			}

			err = m.rabbitmqOutputEventPublisher.Publish(ctx, event.AdditionalData.GetCorrelationId(), dto.GlobalUserEvent{
				UserId:    participantId,
				EventType: dto.EventTypeMessageBrowserNotificationDelete,
				BrowserNotification: &dto.BrowserNotification{
					ChatId:    event.ChatId,
					MessageId: event.MessageId,
					OwnerId:   dto.NonExistentUser,
				},
			})
			if err != nil {
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

func (m *EventHandler) OnMessagePinned(ctx context.Context, event *MessagePinned) error {
	var eventType string
	if event.Pinned {
		eventType = dto.EventTypePinnedMessagePromote
	} else {
		eventType = dto.EventTypePinnedMessageUnpromote
	}

	ctx, messageSpan := m.tr.Start(ctx, fmt.Sprintf("chat.%s", eventType))
	defer messageSpan.End()

	adt, err := m.commonProjection.GetMessageDataForAuthorization(ctx, m.db, event.AdditionalData.BehalfUserId, event.ChatId, event.MessageId)
	if err != nil {
		return err
	}

	if !CanPinMessage(adt.ChatCanPinMessage, adt.IsChatAdmin) {
		m.lgr.InfoContext(ctx, "Skipping OnChatEdited because there is no authorization to do so", "chat_id", event.ChatId, "user_id", event.AdditionalData.BehalfUserId)
		return nil
	}

	promotedMessageId, err := m.commonProjection.OnMessagePinned(ctx, event)
	if err != nil {
		return err
	}

	// send unpromote current message event
	if !event.Pinned {
		err := m.rabbitmqOutputEventPublisher.Publish(ctx, event.AdditionalData.GetCorrelationId(), dto.ChatEvent{
			EventType: eventType,
			PromoteMessageNotification: &dto.PinnedMessageEvent{
				Message: dto.PinnedMessageDto{
					Id:     event.MessageId,
					ChatId: event.ChatId,
				},
				TotalCount: count,
			},
			UserId: participantId,
			ChatId: event.ChatId,
		})
		if err != nil {
			m.lgr.ErrorContext(ctx, "Error during sending to rabbitmq", "err", err)
		}
	}

	// send promote current message event or send promote previous pinned message event
	if promotedMessageId != nil {
		err := m.rabbitmqOutputEventPublisher.Publish(ctx, event.AdditionalData.GetCorrelationId(), dto.ChatEvent{
			EventType: eventType,
			PromoteMessageNotification: &dto.PinnedMessageEvent{
				Message: dto.PinnedMessageDto{
					Id:             *promotedMessageId,
					Text:           dbMessage.Text,
					ChatId:         dbMessage.ChatId,
					OwnerId:        dbMessage.OwnerId,
					Owner:          user,
					PinnedPromoted: dbMessage.PinPromoted,
					CreateDateTime: dbMessage.CreateDateTime,
				},
				TotalCount: count,
			},
			UserId: participantId,
			ChatId: event.ChatId,
		})
		if err != nil {
			m.lgr.ErrorContext(ctx, "Error during sending to rabbitmq", "err", err)
		}
	}

	return nil
}

func (m *EventHandler) OnUnreadMessageReaded(ctx context.Context, event *MessageReaded) error {
	userIds := []int64{event.AdditionalData.BehalfUserId}

	eventTypeUnreadMessagesChanged := dto.EventTypeHasUnreadMessagesChanged
	eventTypeChatUnreadMessagesChanged := dto.EventTypeChatUnreadMessagesChanged

	ctx, messageSpan := m.tr.Start(ctx, fmt.Sprintf("message.%s", eventTypeChatUnreadMessagesChanged))
	defer messageSpan.End()

	if event.ReadMessagesAction == ReadMessagesActionOneMessage || event.ReadMessagesAction == ReadMessagesActionAllMessagesInOneChat {
		adt, err := m.commonProjection.GetMessageDataForAuthorization(ctx, m.db, event.AdditionalData.BehalfUserId, event.ChatId, event.MessageId)
		if err != nil {
			return err
		}

		if !CanReadMessage(adt.IsParticipant) {
			m.lgr.InfoContext(ctx, "Skipping OnUnreadMessageReaded because there is no authorization to do so", "chat_id", event.ChatId, "user_id", event.AdditionalData.BehalfUserId)
			return nil
		}
	}

	err := m.commonProjection.OnUnreadMessageReaded(ctx, event, func(updatedChatsPortion []dto.ChatUserViewBasic) {
		if event.ReadMessagesAction != ReadMessagesActionAllChats {
			m.lgr.ErrorContext(ctx, "wrong invariant: a logical error in commonProjection.OnUnreadMessageReaded")
			return
		}

		for _, cvb := range updatedChatsPortion {
			err := m.rabbitmqOutputEventPublisher.Publish(ctx, event.AdditionalData.GetCorrelationId(), dto.GlobalUserEvent{
				UserId:    event.AdditionalData.BehalfUserId,
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
		cvb, err := m.commonProjection.GetChatUserViewBasic(ctx, m.db, event.ChatId, event.AdditionalData.BehalfUserId)
		if err != nil {
			m.lgr.ErrorContext(ctx, "Error during getting chat UserViewBasic", "err", err)
		} else {
			err = m.rabbitmqOutputEventPublisher.Publish(ctx, event.AdditionalData.GetCorrelationId(), dto.GlobalUserEvent{
				UserId:    event.AdditionalData.BehalfUserId,
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

	err = m.rabbitmqOutputEventPublisher.Publish(ctx, event.AdditionalData.GetCorrelationId(), dto.GlobalUserEvent{
		UserId:    event.AdditionalData.BehalfUserId,
		EventType: eventTypeUnreadMessagesChanged,
		HasUnreadMessagesChanged: &dto.HasUnreadMessagesChanged{
			HasUnreadMessages: hasUnreadMessages[event.AdditionalData.BehalfUserId],
		},
	})
	if err != nil {
		m.lgr.ErrorContext(ctx, "Error during IterateOverParticipantsChatIds", "err", err)
	}

	return nil
}

func (m *EventHandler) OnMessageBlogPostMade(ctx context.Context, event *MessageBlogPostMade) error {
	adt, err := m.commonProjection.GetMessageDataForAuthorization(ctx, m.db, event.AdditionalData.BehalfUserId, event.ChatId, event.MessageId)
	if err != nil {
		return err
	}

	if !CanMakeMessageBlogPost(adt.IsChatAdmin, adt.ChatIsTetATet, adt.IsMessageBlogPost, adt.IsBlog, true) {
		m.lgr.InfoContext(ctx, "Skipping OnMessageBlogPostMade because there is no authorization to do so", "chat_id", event.ChatId, "user_id", event.AdditionalData.BehalfUserId)
		return nil
	}

	currentBlogPost, err := m.commonProjection.GetCurrentBlogPostMessage(ctx, m.db, event.ChatId)
	if err != nil {
		return err
	}

	err = m.commonProjection.OnMessageBlogPostMade(ctx, event)
	if err != nil {
		return err
	}

	eventType := dto.EventTypeMessageEdited

	if currentBlogPost != nil { // here, after OnMessageBlogPostMade() ex. blog post message is no more blog post
		m.lgr.DebugContext(ctx, "Sending notification about the message is no more blog post to participants", "event_type", eventType, "user_id", event.AdditionalData.BehalfUserId)

		errOuter := m.commonProjection.IterateOverChatParticipantIdsExcepting(ctx, m.db, event.ChatId, nil, func(participantIdsPortion []int64) error {
			messageViews, _, _, errInn := m.enrichingProjection.GetMessagesEnriched(ctx, participantIdsPortion, false, nil, event.ChatId, int32(len(participantIdsPortion)), nil, true, false, dto.NoSearchString, currentBlogPost, nil)
			if errInn != nil {
				return errInn
			}

			for _, messageView := range messageViews {
				errInn = m.rabbitmqOutputEventPublisher.Publish(ctx, event.AdditionalData.GetCorrelationId(), dto.ChatEvent{
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
	}

	m.lgr.DebugContext(ctx, "Sending notification about the message become blog post to participants", "event_type", eventType, "user_id", event.AdditionalData.BehalfUserId)

	errOuter := m.commonProjection.IterateOverChatParticipantIdsExcepting(ctx, m.db, event.ChatId, nil, func(participantIdsPortion []int64) error {
		messageViews, _, _, errInn := m.enrichingProjection.GetMessagesEnriched(ctx, participantIdsPortion, false, nil, event.ChatId, int32(len(participantIdsPortion)), nil, true, false, dto.NoSearchString, &event.MessageId, nil)
		if errInn != nil {
			return errInn
		}

		for _, messageView := range messageViews {
			errInn = m.rabbitmqOutputEventPublisher.Publish(ctx, event.AdditionalData.GetCorrelationId(), dto.ChatEvent{
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

func (m *EventHandler) OnMessageReactionFlipped(ctx context.Context, event *MessageReactionFlipped) error {
	ctx, messageSpan := m.tr.Start(ctx, fmt.Sprintf("message.reaction"))
	defer messageSpan.End()

	adt, err := m.commonProjection.GetChatDataForAuthorization(ctx, m.db, event.AdditionalData.BehalfUserId, event.ChatId)
	if err != nil {
		return err
	}

	if !CanReactOnMessage(adt.ChatCanReactOnMessage, adt.IsParticipant) {
		m.lgr.InfoContext(ctx, "Skipping OnMessageReactionFlipped because there is no authorization to do so", "chat_id", event.ChatId, "user_id", event.AdditionalData.BehalfUserId)
		return nil
	}

	wasAdded, err := m.commonProjection.OnMessageReactionFlipped(ctx, event)
	if err != nil {
		return err
	}

	messageBasic, err := m.commonProjection.GetMessageBasic(ctx, m.db, event.ChatId, event.MessageId)
	if err != nil {
		return err
	}

	chatNotificationTitle, err := m.commonProjection.getChatNameForNotification(ctx, m.db, event.ChatId)
	if err != nil {
		m.lgr.WarnContext(ctx, "Unable to get chatNotificationTitle", "chat_id", event.ChatId, "err", err)
		// nothing
	}

	var behalfUserDto *dto.User

	reaction, err := m.commonProjection.GetReaction(ctx, m.db, event.ChatId, event.MessageId, event.Reaction)
	if err != nil {
		m.lgr.ErrorContext(ctx, "Error during IterateOverReactionParticipantsIds", "err", err)
		return nil
	}

	var wasChanged bool
	if reaction.Count > 0 {
		wasChanged = true // false means removed
	}

	toQueryUserIds := []int64{}
	toQueryUserIds = append(toQueryUserIds, event.AdditionalData.BehalfUserId)
	toQueryUserIds = append(toQueryUserIds, reaction.UserIds...)

	users, err := m.aaaRestClient.GetUsers(ctx, toQueryUserIds)
	if err != nil {
		m.lgr.WarnContext(ctx, "unable to get users")
	}
	reactionUserMap := utils.ToMap(users)
	behalfUserDto = reactionUserMap[event.AdditionalData.BehalfUserId]

	reactionUsers := make([]*dto.User, 0)
	for _, userId := range reaction.UserIds {
		user := reactionUserMap[userId]
		if user != nil {
			reactionUsers = append(reactionUsers, user)
		} else {
			reactionUsers = append(reactionUsers, getDeletedUser(userId)) // fallback
		}
	}

	var eventType string
	if wasChanged {
		eventType = dto.EventTypeReactionChanged
	} else {
		eventType = dto.EventTypeReactionRemoved
	}

	aReaction := dto.Reaction{
		Count:    reaction.Count,
		Reaction: reaction.Reaction,
		Users:    reactionUsers,
	}

	reactionChangedEvent := dto.ReactionChangedEvent{
		MessageId: event.MessageId,
		Reaction:  aReaction,
	}

	errOuter := m.commonProjection.IterateOverChatParticipantIdsExcepting(ctx, m.db, event.ChatId, []int64{}, func(participantIds []int64) error {
		for _, participantId := range participantIds {
			errInner := m.rabbitmqOutputEventPublisher.Publish(ctx, event.AdditionalData.GetCorrelationId(), dto.ChatEvent{
				EventType:            eventType,
				ReactionChangedEvent: &reactionChangedEvent,
				UserId:               participantId,
				ChatId:               event.ChatId,
			})
			if errInner != nil {
				m.lgr.ErrorContext(ctx, "Error during sending to rabbitmq", "err", errInner)
			}
		}
		return nil
	})
	if errOuter != nil {
		return errOuter
	}

	var reactionEventType string
	if wasAdded {
		reactionEventType = dto.EventTypeReactionAdded
		re := dto.ReactionEvent{
			UserId:    event.AdditionalData.BehalfUserId,
			Reaction:  event.Reaction,
			MessageId: event.MessageId,
		}

		var messageOwnerId = messageBasic.GetOwnerId()
		if messageOwnerId == dto.NoOwner || messageOwnerId == dto.NoId {
			m.lgr.InfoContext(ctx, "Unable to get message owner for reaction notification")
		} else {
			if behalfUserDto == nil {
				m.lgr.InfoContext(ctx, "Unable to get behalf user for reply notification", "user_id", event.AdditionalData.BehalfUserId)
			} else {
				if messageOwnerId != event.AdditionalData.BehalfUserId { // skip myself
					err = m.rabbitmqNotificationEventsPublisher.Publish(ctx, event.AdditionalData.GetCorrelationId(), dto.NotificationEvent{
						EventType:     reactionEventType,
						ReactionEvent: &re,
						UserId:        messageOwnerId,
						ChatId:        event.ChatId,
						ByUserId:      behalfUserDto.Id,
						ByLogin:       behalfUserDto.Login,
						ByAvatar:      behalfUserDto.Avatar,
						ChatTitle:     chatNotificationTitle,
					})
					if err != nil {
						m.lgr.ErrorContext(ctx, "Error during sending to rabbitmq", "err", err)
					}
				}
			}
		}
	} else {
		reactionEventType = dto.EventTypeReactionDeleted
		re := dto.ReactionEvent{
			Reaction:  event.Reaction,
			MessageId: event.MessageId,
		}

		var messageOwnerId = messageBasic.GetOwnerId()
		if messageOwnerId == dto.NoOwner || messageOwnerId == dto.NoId {
			m.lgr.InfoContext(ctx, "Unable to get message owner for reaction notification")
		} else {
			if behalfUserDto == nil {
				m.lgr.InfoContext(ctx, "Unable to get behalf user for reply notification", "user_id", event.AdditionalData.BehalfUserId)
			} else {
				err = m.rabbitmqNotificationEventsPublisher.Publish(ctx, event.AdditionalData.GetCorrelationId(), dto.NotificationEvent{
					EventType:     reactionEventType,
					ReactionEvent: &re,
					UserId:        messageOwnerId,
					ChatId:        event.ChatId,
					ByUserId:      behalfUserDto.Id,
					ByLogin:       behalfUserDto.Login,
					ByAvatar:      behalfUserDto.Avatar,
					ChatTitle:     chatNotificationTitle,
				})
				if err != nil {
					m.lgr.ErrorContext(ctx, "Error during sending to rabbitmq", "err", err)
				}
			}
		}
	}

	return nil
}
