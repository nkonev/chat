package cqrs

import (
	"context"
	"fmt"
	"go-cqrs-chat-example/db"
)

type UnauthorizedError struct {
	info string
}

func NewUnauthorizedError(info string) *UnauthorizedError {
	return &UnauthorizedError{info: info}
}

func (u *UnauthorizedError) Error() string {
	return u.info
}

type ChatCreate struct {
	AdditionalData *AdditionalData
	Title          string
	ParticipantIds []int64
}

type ChatEdit struct {
	ChatId              int64
	AdditionalData      *AdditionalData
	Title               string
	ParticipantIdsToAdd []int64
	Blog                bool // desired state
}

type ChatDelete struct {
	ChatId         int64
	AdditionalData *AdditionalData
}

type ParticipantAdd struct {
	AdditionalData *AdditionalData
	ChatId         int64
	ParticipantIds []int64
}

type ParticipantDelete struct {
	AdditionalData *AdditionalData
	ChatId         int64
	ParticipantIds []int64
}

type ParticipantChange struct {
	AdditionalData *AdditionalData
	ChatId         int64
	ParticipantId  int64
	NewAdmin       bool
	BehalfUserId   int64
}

type MessageCreate struct {
	AdditionalData *AdditionalData
	ChatId         int64
	OwnerId        int64
	Content        string
}

type MessageEdit struct {
	AdditionalData *AdditionalData
	ChatId         int64
	MessageId      int64
	Content        string
}

type MessageDelete struct {
	AdditionalData *AdditionalData
	ChatId         int64
	MessageId      int64
}

type ChatPin struct {
	AdditionalData *AdditionalData
	ChatId         int64
	Pin            bool
	ParticipantId  int64
}

type MessageRead struct {
	AdditionalData *AdditionalData
	ChatId         int64
	MessageId      int64
	ParticipantId  int64
}

type MakeMessageBlogPost struct {
	AdditionalData *AdditionalData
	ChatId         int64
	MessageId      int64
	BlogPost       bool
}

func (s *ChatCreate) Handle(ctx context.Context, behalfUserId int64, eventBus EventBusInterface, dba *db.DB, commonProjection *CommonProjection) (int64, error) {
	chatId, err := db.TransactWithResult(ctx, dba, func(tx *db.Tx) (int64, error) {
		return commonProjection.GetNextChatId(ctx, tx)
	})

	cc := &ChatCreated{
		AdditionalData: s.AdditionalData,
		ChatId:         chatId,
		Title:          s.Title,
	}
	err = eventBus.Publish(ctx, cc)
	if err != nil {
		return 0, err
	}

	pa := &ParticipantsAdded{
		AdditionalData: s.AdditionalData,
		ChatId:         chatId,
		Participants:   make([]ParticipantWithAdmin, 0),
	}
	for _, participantId := range s.ParticipantIds {
		pa.Participants = append(pa.Participants, ParticipantWithAdmin{
			ParticipantId: participantId,
			ChatAdmin:     participantId == behalfUserId,
		})
	}

	err = eventBus.Publish(ctx, pa)
	if err != nil {
		return 0, err
	}

	return chatId, nil
}

func (s *ChatEdit) Handle(ctx context.Context, eventBus EventBusInterface, dba *db.DB, commonProjection *CommonProjection) error {
	admin, err := commonProjection.IsChatAdmin(ctx, dba, s.BehalfUserId, s.ChatId)
	if err != nil {
		return err
	}
	if !admin {
		return NewUnauthorizedError(fmt.Sprintf("user %v is not admin of chat %v", s.BehalfUserId, s.ChatId))
	}

	cc := &ChatEdited{
		AdditionalData: s.AdditionalData,
		ChatId:         s.ChatId,
		Title:          s.Title,
		Blog:           s.Blog,
	}
	err = eventBus.Publish(ctx, cc)
	if err != nil {
		return err
	}

	if len(s.ParticipantIdsToAdd) > 0 {
		pa := &ParticipantsAdded{
			AdditionalData: s.AdditionalData,
			ChatId:         s.ChatId,
		}
		for _, participantId := range s.ParticipantIdsToAdd {
			pa.Participants = append(pa.Participants, ParticipantWithAdmin{
				ParticipantId: participantId,
				ChatAdmin:     false,
			})
		}

		err = eventBus.Publish(ctx, pa)
		if err != nil {
			return err
		}
	}

	errOuter := commonProjection.IterateOverChatParticipantIds(ctx, dba, s.ChatId, nil, func(participantIdsPortion []int64) error {
		ui := &ChatViewRefreshed{
			AdditionalData:   s.AdditionalData,
			ParticipantIds:   participantIdsPortion,
			ChatId:           s.ChatId,
			ChatCommonAction: ChatCommonActionRefresh,
			Title:            s.Title,
		}

		if len(s.ParticipantIdsToAdd) > 0 {
			ui.ParticipantsAction = ParticipantsActionRefresh
		}

		errInner := eventBus.Publish(ctx, ui)
		if errInner != nil {
			return errInner
		}
		return nil
	})

	return errOuter
}

func (s *ChatDelete) Handle(ctx context.Context, eventBus EventBusInterface, dba *db.DB, commonProjection *CommonProjection) error {
	admin, err := commonProjection.IsChatAdmin(ctx, dba, s.BehalfUserId, s.ChatId)
	if err != nil {
		return err
	}
	if !admin {
		return NewUnauthorizedError(fmt.Sprintf("user %v is not admin of chat %v", s.BehalfUserId, s.ChatId))
	}

	errOuter := commonProjection.IterateOverChatParticipantIds(ctx, dba, s.ChatId, nil, func(participantIdsPortion []int64) error {
		pa := &ParticipantDeleted{
			AdditionalData: s.AdditionalData,
			ParticipantIds: participantIdsPortion,
			ChatId:         s.ChatId,
		}
		errInner := eventBus.Publish(ctx, pa)
		return errInner

	})
	if errOuter != nil {
		return errOuter
	}

	cc := &ChatDeleted{
		AdditionalData: s.AdditionalData,
		ChatId:         s.ChatId,
	}
	err = eventBus.Publish(ctx, cc)
	if err != nil {
		return err
	}
	return nil
}

func (s *ParticipantAdd) Handle(ctx context.Context, eventBus EventBusInterface, dba *db.DB, commonProjection *CommonProjection) error {
	admin, err := commonProjection.IsChatAdmin(ctx, dba, s.BehalfUserId, s.ChatId)
	if err != nil {
		return err
	}
	if !admin {
		return NewUnauthorizedError(fmt.Sprintf("user %v is not admin of chat %v", s.BehalfUserId, s.ChatId))
	}

	pa := &ParticipantsAdded{
		AdditionalData: s.AdditionalData,
		ChatId:         s.ChatId,
	}
	for _, participantId := range s.ParticipantIds {
		pa.Participants = append(pa.Participants, ParticipantWithAdmin{
			ParticipantId: participantId,
			ChatAdmin:     false,
		})
	}
	err = eventBus.Publish(ctx, pa)
	if err != nil {
		return err
	}

	// excluding => s.ParticipantIds is an optimization in order not to re-refresh views for the recently added
	errOuter := commonProjection.IterateOverChatParticipantIds(ctx, dba, s.ChatId, s.ParticipantIds, func(participantIdsPortion []int64) error {
		if len(participantIdsPortion) > 0 {
			ui := &ChatViewRefreshed{
				AdditionalData:     s.AdditionalData,
				ParticipantIds:     participantIdsPortion, // chat_user_views for newly added participants will be created from scratch including already added, see ParticipantsAdded handler
				ChatId:             s.ChatId,
				ParticipantsAction: ParticipantsActionRefresh,
			}
			errInner := eventBus.Publish(ctx, ui)
			if errInner != nil {
				return errInner
			}
		}
		return nil
	})

	return errOuter
}

func (s *ParticipantDelete) Handle(ctx context.Context, eventBus EventBusInterface, dba *db.DB, commonProjection *CommonProjection) error {
	admin, err := commonProjection.IsChatAdmin(ctx, dba, s.BehalfUserId, s.ChatId)
	if err != nil {
		return err
	}
	if !admin {
		return NewUnauthorizedError(fmt.Sprintf("user %v is not admin of chat %v", s.BehalfUserId, s.ChatId))
	}

	pa := &ParticipantDeleted{
		AdditionalData: s.AdditionalData,
		ParticipantIds: s.ParticipantIds,
		ChatId:         s.ChatId,
	}
	err = eventBus.Publish(ctx, pa)
	if err != nil {
		return err
	}

	// excluding => s.ParticipantIds is an optimization - we don't need to refresh views for deleted participants
	errOuter := commonProjection.IterateOverChatParticipantIds(ctx, dba, s.ChatId, s.ParticipantIds, func(participantIdsPortion []int64) error {
		if len(participantIdsPortion) > 0 {
			ui := &ChatViewRefreshed{
				AdditionalData:     s.AdditionalData,
				ParticipantIds:     participantIdsPortion,
				ChatId:             s.ChatId,
				ParticipantsAction: ParticipantsActionRefresh,
			}
			errInner := eventBus.Publish(ctx, ui)
			if errInner != nil {
				return errInner
			}
			return nil
		}
		return nil
	})

	return errOuter
}

func (s *ParticipantChange) Handle(ctx context.Context, eventBus EventBusInterface, dba *db.DB, commonProjection *CommonProjection) error {
	admin, err := commonProjection.IsChatAdmin(ctx, dba, s.BehalfUserId, s.ChatId)
	if err != nil {
		return err
	}
	if !admin {
		return NewUnauthorizedError(fmt.Sprintf("user %v is not admin of chat %v", s.BehalfUserId, s.ChatId))
	}

	pa := &ParticipantChanged{
		AdditionalData: s.AdditionalData,
		ParticipantId:  s.ParticipantId,
		ChatId:         s.ChatId,
		BehalfUserId:   s.BehalfUserId,
		NewAdmin:       s.NewAdmin,
	}
	err = eventBus.Publish(ctx, pa)
	if err != nil {
		return err
	}

	return nil
}

func (s *ChatPin) Handle(ctx context.Context, eventBus EventBusInterface) error {
	cp := &ChatPinned{
		AdditionalData: s.AdditionalData,
		ParticipantId:  s.ParticipantId,
		ChatId:         s.ChatId,
		Pinned:         s.Pin,
	}
	return eventBus.Publish(ctx, cp)
}

func (s *MessageCreate) Handle(ctx context.Context, eventBus EventBusInterface, dba *db.DB, commonProjection *CommonProjection) (int64, bool, error) {
	messageId, err := db.TransactWithResult(ctx, dba, func(tx *db.Tx) (int64, error) {
		return commonProjection.GetNextMessageId(ctx, tx, s.ChatId)
	})
	if err != nil {
		return 0, false, err
	}

	if messageId == ChatStillNotExists {
		return 0, false, nil
	}

	mc := &MessageCreated{
		AdditionalData: s.AdditionalData,
		Id:             messageId,
		OwnerId:        s.OwnerId,
		ChatId:         s.ChatId,
		Content:        s.Content,
	}

	err = eventBus.Publish(ctx, mc)
	if err != nil {
		return 0, false, err
	}

	errOuter := commonProjection.IterateOverChatParticipantIds(ctx, dba, s.ChatId, nil, func(participantIdsPortion []int64) error {
		ui := &ChatViewRefreshed{
			AdditionalData:       s.AdditionalData,
			ParticipantIds:       participantIdsPortion,
			ChatId:               s.ChatId,
			UnreadMessagesAction: UnreadMessagesActionIncrease,
			IncreaseOn:           1,
			OwnerId:              s.OwnerId,
			LastMessageAction:    LastMessageActionRefresh,
		}

		errInner := eventBus.Publish(ctx, ui)
		if errInner != nil {
			return errInner
		}
		return nil
	})

	if errOuter != nil {
		return 0, false, errOuter
	}

	return messageId, true, nil
}

func (s *MessageRead) Handle(ctx context.Context, eventBus EventBusInterface, commonProjection *CommonProjection) error {

	lastMessageReadedId, lastMessgeReadedExists, maxMessageId, err := commonProjection.GetLastMessageReaded(ctx, s.ChatId, s.ParticipantId)
	if err != nil {
		return err
	}

	messageIdToMark := s.MessageId

	if s.MessageId > maxMessageId {
		messageIdToMark = maxMessageId
	}

	if (lastMessgeReadedExists && messageIdToMark > lastMessageReadedId) || (!lastMessgeReadedExists && lastMessageReadedId == 0) {
		cp := &MessageReaded{
			AdditionalData: s.AdditionalData,
			ParticipantId:  s.ParticipantId,
			ChatId:         s.ChatId,
			MessageId:      messageIdToMark,
		}
		return eventBus.Publish(ctx, cp)
	}

	return nil
}

func (s *MakeMessageBlogPost) Handle(ctx context.Context, eventBus EventBusInterface) error {
	ev := MessageBlogPostMade{
		AdditionalData: s.AdditionalData,
		ChatId:         s.ChatId,
		MessageId:      s.MessageId,
		BlogPost:       s.BlogPost,
	}

	return eventBus.Publish(ctx, &ev)
}

func (s *MessageDelete) Handle(ctx context.Context, eventBus EventBusInterface, dba *db.DB, commonProjection *CommonProjection, userId int64) error {

	ownerId, err := commonProjection.GetMessageOwner(ctx, s.ChatId, s.MessageId)
	if err != nil {
		return err
	}

	if ownerId != userId {
		return fmt.Errorf("User %v is not an owner of message %v in chat %v", userId, s.MessageId, s.ChatId)
	}

	cp := &MessageDeleted{
		AdditionalData: s.AdditionalData,
		ChatId:         s.ChatId,
		MessageId:      s.MessageId,
	}
	err = eventBus.Publish(ctx, cp)
	if err != nil {
		return err
	}

	errOuter := commonProjection.IterateOverChatParticipantIds(ctx, dba, s.ChatId, nil, func(participantIdsPortion []int64) error {
		ui := &ChatViewRefreshed{
			AdditionalData:       s.AdditionalData,
			ParticipantIds:       participantIdsPortion,
			ChatId:               s.ChatId,
			UnreadMessagesAction: UnreadMessagesActionRefresh,
			OwnerId:              userId,
			LastMessageAction:    LastMessageActionRefresh,
		}

		errInner := eventBus.Publish(ctx, ui)
		if errInner != nil {
			return errInner
		}
		return nil
	})

	return errOuter
}

func (s *MessageEdit) Handle(ctx context.Context, eventBus EventBusInterface, dba *db.DB, commonProjection *CommonProjection, userId int64) error {
	ownerId, err := commonProjection.GetMessageOwner(ctx, s.ChatId, s.MessageId)
	if err != nil {
		return err
	}

	if ownerId != userId {
		return fmt.Errorf("User %v is not an owner of message %v in chat %v", userId, s.MessageId, s.ChatId)
	}

	cp := &MessageEdited{
		AdditionalData: s.AdditionalData,
		ChatId:         s.ChatId,
		Id:             s.MessageId,
		Content:        s.Content,
	}
	err = eventBus.Publish(ctx, cp)
	if err != nil {
		return err
	}

	lastMessageId, err := commonProjection.GetLastMessageId(ctx, s.ChatId)
	if lastMessageId == s.MessageId {
		// if it's the last chat message then update ChatView
		errOuter := commonProjection.IterateOverChatParticipantIds(ctx, dba, s.ChatId, nil, func(participantIdsPortion []int64) error {
			ui := &ChatViewRefreshed{
				AdditionalData:    s.AdditionalData,
				ParticipantIds:    participantIdsPortion,
				ChatId:            s.ChatId,
				LastMessageAction: LastMessageActionRefresh,
			}

			errInner := eventBus.Publish(ctx, ui)
			if errInner != nil {
				return errInner
			}
			return nil
		})
		if errOuter != nil {
			return errOuter
		}
	}
	return nil
}
