package cqrs

import (
	"context"
	"go-cqrs-chat-example/dto"
	"go-cqrs-chat-example/utils"
	"maps"
	"slices"
)

func (m *EventHandler) OnUserChatViewCreated(ctx context.Context, event *UserChatViewCreated) error {
	eventTypeParticipantAdded := dto.EventTypeParticipantAdded

	userIds := []int64{event.UserId}

	err := m.commonProjection.OnUserChatViewCreated(ctx, userIds, event.ChatId, event.AdditionalData)
	if err != nil {
		return err
	}

	eventTypeChatCreated := dto.EventTypeChatCreated
	eventTypeUnreadMessagesChanged := dto.EventTypeHasUnreadMessagesChanged

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

		if event.TetATet {
			err = m.rabbitmqOutputEventPublisher.Publish(ctx, event.AdditionalData.GetCorrelationId(), dto.GlobalUserEvent{
				UserId:    cv.BehalfUserId,
				EventType: dto.EventTypeChatTetATetUpserted,
				ChatTetATetUpsertedDto: &dto.ChatTetATetUpsertedDto{
					ChatId: cv.Id,
				},
			})
			if err != nil {
				m.lgr.ErrorContext(ctx, "Error during sending to rabbitmq", "err", err)
			}

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
