package cqrs

import (
	"context"
	"fmt"
	"go-cqrs-chat-example/client"
	"go-cqrs-chat-example/config"
	"go-cqrs-chat-example/db"
	"go-cqrs-chat-example/dto"
	"go-cqrs-chat-example/logger"
	"go-cqrs-chat-example/producer"
	"go-cqrs-chat-example/utils"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"
	"maps"
	"slices"
)

// performs Authorization,
// sending before events,
// mutations via delegating to projection
// sending after events
type EventHandler struct {
	commonProjection             *CommonProjection
	enrichingProjection          *EnrichingProjection
	rabbitmqOutputEventPublisher *producer.RabbitOutputEventsPublisher
	db                           *db.DB
	lgr                          *logger.LoggerWrapper
	tr                           trace.Tracer
	aaaRestClient                client.AaaRestClient
	chatUserViewConfig           *config.ChatUserViewConfig
}

func NewEventHandler(commonProjection *CommonProjection, enrichingProjection *EnrichingProjection, rabbitmqEventPublisher *producer.RabbitOutputEventsPublisher, db *db.DB, lgr *logger.LoggerWrapper, aaaRestClient client.AaaRestClient, cfg *config.AppConfig) *EventHandler {
	tr := otel.Tracer("event")

	return &EventHandler{
		commonProjection:             commonProjection,
		enrichingProjection:          enrichingProjection,
		rabbitmqOutputEventPublisher: rabbitmqEventPublisher,
		db:                           db,
		lgr:                          lgr,
		tr:                           tr,
		aaaRestClient:                aaaRestClient,
		chatUserViewConfig:           &cfg.Cqrs.Projections.ChatUserView,
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

	if !CanAddParticipant(adt.IsChatAdmin, adt.ChatIsTetATet, event.IsJoining, adt.AvailableToSearch, adt.IsBlog, event.IsChatCreating) {
		m.lgr.InfoContext(ctx, "Skipping ParticipantsAdded because there is no authorization to do so", "chat_id", event.ChatId, "user_id", event.AdditionalData.BehalfUserId)
		return nil
	}

	errp := m.commonProjection.OnParticipantAdded(ctx, event)
	if errp != nil {
		return errp
	}

	m.lgr.DebugContext(ctx, "Sending notification about the chat to participants", "event_type", eventTypeChatCreated, "user_ids", userIds)

	// we don't need to change GetChatsEnriched to additionally process [behalf]userIds because we've already added users in our projection and the projection return all the users
	chatViews, _, err := m.enrichingProjection.GetChatsEnriched(ctx, userIds, int32(len(userIds)), nil, true, false, dto.NoSearchString, &event.ChatId)
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
		if event.IsJoining && cv.BehalfUserId == event.AdditionalData.BehalfUserId {
			dt.EventType = dto.EventTypeChatEdited
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
		return m.handleParticipantRemoved(ctx, event.AdditionalData, event.ParticipantIds, event.ChatId, event.AdditionalData.BehalfUserId, event.IsLeaving, false)
	} else if event.GetParticipantsType == GetParticipantsTypeAllInChatExcepting { // delete chat
		return m.commonProjection.IterateOverChatParticipantIdsExcepting(ctx, m.db, event.ChatId, event.AllParticipantIdsExcepting, func(participantIdsPortion []int64) error {
			return m.handleParticipantRemoved(ctx, event.AdditionalData, participantIdsPortion, event.ChatId, event.AdditionalData.BehalfUserId, event.IsLeaving, isChatRemoving)
		})
	} else {
		return fmt.Errorf("Unknown event.GetParticipantsType = %v", event.GetParticipantsType)
	}
}

func (m *EventHandler) handleParticipantRemoved(ctx context.Context, additionalData *AdditionalData, participantIds []int64, chatId int64, behalfUserId int64, isLeaving bool, isChatRemoving bool) error {
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

	errp := m.commonProjection.OnParticipantRemoved(ctx, additionalData, userIds, chatId, behalfUserId, isLeaving, isChatRemoving)
	if errp != nil {
		return errp
	}

	var hasUnreadMessages = map[int64]bool{}
	hasUnreadMessages, err := m.commonProjection.GetHasUnreadMessages(ctx, userIds)
	if err != nil {
		return err
	}

	for _, participantId := range userIds {
		err = m.rabbitmqOutputEventPublisher.Publish(ctx, additionalData.GetCorrelationId(), dto.GlobalUserEvent{
			UserId:         participantId,
			EventType:      eventType,
			ChatDeletedDto: &dto.ChatDeletedDto{Id: chatId},
		})
		if err != nil {
			m.lgr.ErrorContext(ctx, "Error during sending to rabbitmq", "err", err)
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

	err = m.commonProjection.OnChatEdited(ctx, event)
	if err != nil {
		return err
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

		chatViews, _, err := m.enrichingProjection.GetChatsEnriched(ctx, userIds, int32(len(userIds)), nil, true, false, dto.NoSearchString, &event.ChatId)
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

	// TODO NotifyNewMessageBrowserNotification browser_notification_add_message

	errOuter := m.commonProjection.IterateOverChatParticipantIdsExcepting(ctx, m.db, event.MessageCommoned.ChatId, nil, func(participantIdsPortion []int64) error {
		messageViews, _, errInn := m.enrichingProjection.GetMessagesEnriched(ctx, participantIdsPortion, false, nil, event.MessageCommoned.ChatId, int32(len(participantIdsPortion)), nil, true, false, dto.NoSearchString, &event.MessageCommoned.Id)
		if errInn != nil {
			return errInn
		}

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

		return nil
	})
	if errOuter != nil {
		return errOuter
	}

	return nil
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

	err = m.commonProjection.OnMessageEdited(ctx, event)
	if err != nil {
		return err
	}

	m.lgr.DebugContext(ctx, "Sending notification about the message to participants", "event_type", eventType, "user_id", event.AdditionalData.BehalfUserId)

	errOuter := m.commonProjection.IterateOverChatParticipantIdsExcepting(ctx, m.db, event.MessageCommoned.ChatId, nil, func(participantIdsPortion []int64) error {
		messageViews, _, errInn := m.enrichingProjection.GetMessagesEnriched(ctx, participantIdsPortion, false, nil, event.MessageCommoned.ChatId, int32(len(participantIdsPortion)), nil, true, false, dto.NoSearchString, &event.MessageCommoned.Id)
		if errInn != nil {
			return errInn
		}

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

		return nil
	})
	if errOuter != nil {
		return errOuter
	}

	return nil
}

func (m *EventHandler) OnMessageRemoved(ctx context.Context, event *MessageDeleted) error {
	eventType := dto.EventTypeMessageDeleted

	ctx, messageSpan := m.tr.Start(ctx, fmt.Sprintf("message.%s", eventType))
	defer messageSpan.End()

	err := m.commonProjection.OnMessageRemoved(ctx, event)
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
		}

		return nil
	})
	if errOuter != nil {
		return errOuter
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
			messageViews, _, errInn := m.enrichingProjection.GetMessagesEnriched(ctx, participantIdsPortion, false, nil, event.ChatId, int32(len(participantIdsPortion)), nil, true, false, dto.NoSearchString, currentBlogPost)
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
		messageViews, _, errInn := m.enrichingProjection.GetMessagesEnriched(ctx, participantIdsPortion, false, nil, event.ChatId, int32(len(participantIdsPortion)), nil, true, false, dto.NoSearchString, &event.MessageId)
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

	err = m.commonProjection.OnMessageReactionFlipped(ctx, event)
	if err != nil {
		return err
	}

	reaction, err := m.commonProjection.GetReaction(ctx, m.db, event.ChatId, event.MessageId, event.Reaction)
	if err != nil {
		m.lgr.ErrorContext(ctx, "Error during IterateOverReactionParticipantsIds", "err", err)
		return nil
	}

	var wasChanged bool
	if reaction.Count > 0 {
		wasChanged = true // false means removed
	}

	users, err := m.aaaRestClient.GetUsers(ctx, reaction.UserIds)
	if err != nil {
		m.lgr.WarnContext(ctx, "unable to get users")
	}
	reactionUserMap := utils.ToMap(users)

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
			err := m.rabbitmqOutputEventPublisher.Publish(ctx, event.AdditionalData.GetCorrelationId(), dto.ChatEvent{
				EventType:            eventType,
				ReactionChangedEvent: &reactionChangedEvent,
				UserId:               participantId,
				ChatId:               event.ChatId,
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
	return nil
}
