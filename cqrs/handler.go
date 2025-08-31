package cqrs

import (
	"context"
	"fmt"
	"go-cqrs-chat-example/db"
	"go-cqrs-chat-example/dto"
	"go-cqrs-chat-example/producer"
	"go-cqrs-chat-example/utils"
)

type EventHandler struct {
	commonProjection       *CommonProjection
	rabbitmqEventPublisher *producer.RabbitEventsPublisher
	db                     *db.DB
}

func NewEventHandler(commonProjection *CommonProjection, rabbitmqEventPublisher *producer.RabbitEventsPublisher, db *db.DB) *EventHandler {
	return &EventHandler{
		commonProjection:       commonProjection,
		rabbitmqEventPublisher: rabbitmqEventPublisher,
		db:                     db,
	}
}

func (m *EventHandler) OnChatCreated(ctx context.Context, event *ChatCreated) error {
	err := m.commonProjection.OnChatCreated(ctx, event)
	if err != nil {
		return err
	}

	errOuter := db.Transact(ctx, m.db, func(tx *db.Tx) error {
		// TODO взять персональный chat_user_view
		//  и поверх обогатить инфой canDelete, canEdit...
		chatDto, err := ch.getChatWithoutPersonalization(ctx, tx, event.ChatId, 0, 0)
		if err != nil {
			return err
		}

		err = tx.IterateOverChatParticipantIds(ctx, event.ChatId, func(participantIds []int64) error {
			areAdmins, err := getAreAdminsOfUserIds(ctx, tx, participantIds, event.ChatId)
			if err != nil {
				return err
			}

			m.NotifyAboutNewChat(ctx, chatDto, participantIds, len(chatDto.ParticipantIds) == 1, true, tx, areAdmins)
			return nil
		})
		if err != nil {
			return err
		}
		return nil
	})
	return errOuter
}

func (not *EventHandler) NotifyAboutNewChat(ctx context.Context, newChatDto *dto.ChatDto, userIds []int64, isSingleTetATetParticipant bool, overrideIsParticipant bool, tx *db.Tx, areAdminsMap map[int64]bool) {
	not.chatNotifyCommon(ctx, userIds, newChatDto, "chat_created", isSingleTetATetParticipant, overrideIsParticipant, tx, areAdminsMap)
}

/**
 * isSingleParticipant should be taken from responseDto or count. using len(participants) where participants are a portion from Iterate...() is incorrect because we can get only one user in the last iteration
 */
func (not *EventHandler) chatNotifyCommon(ctx context.Context, userIds []int64, newChatDto *dto.ChatDto, eventType string, isSingleTetATetParticipant bool, overrideIsParticipant bool, tx *db.Tx, areAdminsMap map[int64]bool) {
	not.lgr.WithTracing(ctx).Debugf("Sending notification about %v the chat to participants: %v", eventType, userIds)

	ctx, messageSpan := not.tr.Start(ctx, fmt.Sprintf("chat.%s", eventType))
	defer messageSpan.End()

	if eventType == "chat_deleted" {
		for _, participantId := range userIds {
			err := not.rabbitEventPublisher.Publish(ctx, dto.GlobalUserEvent{
				UserId:         participantId,
				EventType:      eventType,
				ChatDeletedDto: &dto.ChatDeletedDto{Id: newChatDto.Id},
			})
			if err != nil {
				not.lgr.WithTracing(ctx).Errorf("Error during sending to rabbitmq : %s", err)
			}
		}
	} else {
		unreadMessages, err := tx.GetUnreadMessagesCountBatchByParticipants(ctx, userIds, newChatDto.Id)
		if err != nil {
			not.lgr.WithTracing(ctx).Errorf("error during get unread messages: %v", err)
			return
		}

		isChatPinnedMap, err := tx.IsChatPinnedBatch(ctx, userIds, newChatDto.Id)
		if err != nil {
			not.lgr.WithTracing(ctx).Errorf("error during get pinned: %v", err)
			return
		}

		participantsOnlineForTetATetMap, err := not.getParticipantsOnlineForTetATetMap(ctx, newChatDto.IsTetATet, newChatDto.ParticipantIds)
		if err != nil {
			not.lgr.WithTracing(ctx).Warnf("error during get user onlines: %v", err)
		}

		for _, participantId := range userIds {
			var copied *dto.ChatDto = &dto.ChatDto{}
			if err := deepcopy.Copy(copied, newChatDto); err != nil {
				not.lgr.WithTracing(ctx).Errorf("error during performing deep copy: %s", err)
				continue
			}

			// see also handlers/chat.go:199 convertToDto()
			// override pinned personally for participantId
			copied.SetPersonalizedFields(areAdminsMap[participantId], unreadMessages[participantId], overrideIsParticipant, isChatPinnedMap[participantId])

			// set chat name and avatar for tet-a-tet
			for _, participant := range copied.Participants {
				utils.ReplaceForTetATet(copied, participantsOnlineForTetATetMap, participant, participantId, isSingleTetATetParticipant)
			}

			err = not.rabbitEventPublisher.Publish(ctx, dto.GlobalUserEvent{
				UserId:           participantId,
				EventType:        eventType,
				ChatNotification: copied,
			})
			if err != nil {
				not.lgr.WithTracing(ctx).Errorf("Error during sending to rabbitmq : %s", err)
			}
		}
	}
}
