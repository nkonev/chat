package cqrs

// In general, to avoid race conditions, we should avoid relying on database here, in command_handler.
// Invoking db here, we can get an old data and make wrong decisions.
// The best place to perform checks against database is the projection side.
// In sake optimization here we have as an exception a few db calls.
// See comments about it in TestUnreads()
// Also, in order to keep these command's response times fast we should avoid iterations over db rows here. The best place for it is event_handler, projection.

import (
	"context"
	"errors"
	"fmt"
	validation "github.com/go-ozzo/ozzo-validation/v4"
	"github.com/qdm12/reprint"
	"go-cqrs-chat-example/config"
	"go-cqrs-chat-example/db"
	"go-cqrs-chat-example/dto"
	"go-cqrs-chat-example/logger"
	"go-cqrs-chat-example/sanitizer"
	"slices"
)

const minChatNameLen = 1
const maxChatNameLen = 256

const maxMessageLen = 1024 * 1024
const minMessageLen = 1

type UnauthorizedError struct {
	info string
}

func NewUnauthorizedError(info string) *UnauthorizedError {
	return &UnauthorizedError{info: info}
}

func (u *UnauthorizedError) Error() string {
	return u.info
}

type ValidationError struct {
	info string
}

func (u *ValidationError) Error() string {
	return u.info
}

func NewValidationError(info string) *ValidationError {
	return &ValidationError{info: info}
}

type ChatStillNotExistsError struct {
	info string
}

func (u *ChatStillNotExistsError) Error() string {
	return u.info
}

func NewChatStillNotExistsError(info string) *ChatStillNotExistsError {
	return &ChatStillNotExistsError{info: info}
}

type MessageStillNotExistsError struct {
	info string
}

func (u *MessageStillNotExistsError) Error() string {
	return u.info
}

func NewMessageStillNotExistsError(info string) *MessageStillNotExistsError {
	return &MessageStillNotExistsError{info: info}
}

type ParticipantsError struct {
	info string
}

func (u *ParticipantsError) Error() string {
	return u.info
}

func NewParticipantsError(info string) *ParticipantsError {
	return &ParticipantsError{info: info}
}

type ChatCreate struct {
	AdditionalData                      *AdditionalData
	Title                               string
	ParticipantIds                      []int64
	TetATet                             bool
	Blog                                bool
	Avatar                              *string
	AvatarBig                           *string
	CanResend                           bool
	CanReact                            bool
	AvailableToSearch                   bool
	RegularParticipantCanPublishMessage bool
	RegularParticipantCanPinMessage     bool
	RegularParticipantCanWriteMessage   bool
}

type ChatEdit struct {
	AdditionalData                      *AdditionalData
	ChatId                              int64
	Title                               string
	ParticipantIdsToAdd                 []int64
	Blog                                bool // desired state
	Avatar                              *string
	AvatarBig                           *string
	CanResend                           bool
	CanReact                            bool
	AvailableToSearch                   bool
	RegularParticipantCanPublishMessage bool
	RegularParticipantCanPinMessage     bool
	RegularParticipantCanWriteMessage   bool
}

func (cc *ChatEdit) IsValidatabale() bool {
	return true
}

func (a *ChatEdit) Validate() error {
	return validation.ValidateStruct(a,
		validation.Field(&a.Title, validation.Required, validation.Length(minChatNameLen, maxChatNameLen), validation.NotIn(dto.ReservedPublicallyAvailableForSearchChats)),
		validation.Field(&a.ChatId, validation.Required),
	)
}

func (cc *ChatCreate) IsValidatabale() bool {
	return true
}

func (a *ChatCreate) Validate() error {
	return validation.ValidateStruct(a,
		validation.Field(&a.Title, validation.Required, validation.Length(minChatNameLen, maxChatNameLen), validation.NotIn(dto.ReservedPublicallyAvailableForSearchChats)),
	)
}

type ChatDelete struct {
	ChatId         int64
	AdditionalData *AdditionalData
}

type ParticipantAdd struct {
	AdditionalData *AdditionalData
	ChatId         int64
	ParticipantIds []int64
	IsJoining      bool
}

type ParticipantDelete struct {
	AdditionalData *AdditionalData
	ChatId         int64
	ParticipantIds []int64
	IsLeaving      bool
}

type ParticipantChange struct {
	AdditionalData *AdditionalData
	ChatId         int64
	ParticipantId  int64
	NewAdmin       bool
}

type EmbedMessage struct {
	Id        int64
	ChatId    int64
	EmbedType string
}

type MessageCreate struct {
	AdditionalData *AdditionalData
	ChatId         int64
	Content        string
	EmbedMessage   *EmbedMessage
}

type MessageEdit struct {
	AdditionalData *AdditionalData
	ChatId         int64
	MessageId      int64
	Content        string
	EmbedMessage   *EmbedMessage
}

func (a *MessageCreate) Validate() error {
	return validation.ValidateStruct(a,
		validation.Field(&a.Content, validation.Required, validation.Length(minMessageLen, maxMessageLen)),
	)
}

func (mcd *MessageCreate) IsValidatabale() bool {
	return mcd.EmbedMessage == nil || (mcd.EmbedMessage != nil && mcd.EmbedMessage.EmbedType == dto.EmbedMessageTypeReply)
}

func (a *MessageEdit) Validate() error {
	return validation.ValidateStruct(a,
		validation.Field(&a.Content, validation.Required, validation.Length(minMessageLen, maxMessageLen)),
		validation.Field(&a.MessageId, validation.Required),
	)
}

func (mcd *MessageEdit) IsValidatabale() bool {
	return true
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
}

type ChatNotificationSettingsSet struct {
	AdditionalData *AdditionalData
	ChatId         int64
	Set            bool
}

type MessageRead struct {
	AdditionalData     *AdditionalData
	ChatId             int64
	MessageId          int64
	ReadMessagesAction ReadMessagesAction
}

type MakeMessageBlogPost struct {
	AdditionalData *AdditionalData
	ChatId         int64
	MessageId      int64
	BlogPost       bool
}

type MessageReactionFlip struct {
	AdditionalData *AdditionalData
	ChatId         int64
	MessageId      int64
	Reaction       string
}

func (sp *ChatCreate) Handle(ctx context.Context, eventBus EventBusInterface, dba *db.DB, commonProjection *CommonProjection, stripTagsPolicy *sanitizer.StripTagsPolicy, cfg *config.AppConfig) (int64, error) {
	var copyCommand *ChatCreate
	err := reprint.FromTo(&sp, &copyCommand)
	if err != nil {
		return 0, err
	}

	if !slices.Contains(copyCommand.ParticipantIds, copyCommand.AdditionalData.BehalfUserId) {
		copyCommand.ParticipantIds = append(copyCommand.ParticipantIds, copyCommand.AdditionalData.BehalfUserId)
	}

	if int32(len(copyCommand.ParticipantIds)) > cfg.Cqrs.Commands.MaxParticipantsPerSingleCommand {
		return 0, fmt.Errorf("Max allowed participants %d, got %d", cfg.Cqrs.Commands.MaxParticipantsPerSingleCommand, copyCommand.ParticipantIds)
	}

	copyCommand.Title = sanitizer.TrimAmdSanitizeChatTitle(stripTagsPolicy, copyCommand.Title)

	if copyCommand.IsValidatabale() {
		if err = copyCommand.Validate(); err != nil {
			return 0, NewValidationError(fmt.Sprintf("Error during validation: %v", err))
		}
	}

	if copyCommand.TetATet {
		if len(copyCommand.ParticipantIds) != 2 {
			return 0, NewValidationError("Error during validation: tet-a-tet chat doesn't have 2 participants")
		}
		if copyCommand.ParticipantIds[0] == copyCommand.ParticipantIds[1] {
			return 0, NewValidationError("Error during validation: tet-a-tet should have different participants")
		}
		if copyCommand.Blog {
			return 0, NewValidationError("Error during validation: tet-a-tet cannot be blog")
		}
	}

	chatId, err := db.TransactWithResult(ctx, dba, func(tx *db.Tx) (int64, error) {
		return commonProjection.GetNextChatId(ctx, tx)
	})
	if err != nil {
		return 0, err
	}

	cc := &ChatCreated{
		AdditionalData:                      copyCommand.AdditionalData,
		ChatId:                              chatId,
		Title:                               copyCommand.Title,
		TetATet:                             copyCommand.TetATet,
		Blog:                                copyCommand.Blog,
		Avatar:                              copyCommand.Avatar,
		AvatarBig:                           copyCommand.AvatarBig,
		CanResend:                           copyCommand.CanResend,
		CanReact:                            copyCommand.CanReact,
		AvailableToSearch:                   copyCommand.AvailableToSearch,
		RegularParticipantCanPublishMessage: copyCommand.RegularParticipantCanPublishMessage,
		RegularParticipantCanPinMessage:     copyCommand.RegularParticipantCanPinMessage,
		RegularParticipantCanWriteMessage:   copyCommand.RegularParticipantCanWriteMessage,
	}
	err = eventBus.Publish(ctx, cc)
	if err != nil {
		return 0, err
	}

	pa := &ParticipantsAdded{
		AdditionalData: copyCommand.AdditionalData,
		ChatId:         chatId,
		Participants:   make([]ParticipantWithAdmin, 0),
		AreFirstUsers:  true,
	}
	for _, participantId := range copyCommand.ParticipantIds {
		pa.Participants = append(pa.Participants, ParticipantWithAdmin{
			ParticipantId: participantId,
			ChatAdmin:     participantId == copyCommand.AdditionalData.BehalfUserId || copyCommand.TetATet,
		})
	}

	if len(pa.Participants) == 0 {
		return dto.NoId, NewParticipantsError("Cannot add 0 participants")
	}

	err = eventBus.Publish(ctx, pa)
	if err != nil {
		return 0, err
	}

	return chatId, nil
}

func (sp *ChatEdit) Handle(ctx context.Context, eventBus EventBusInterface, dba *db.DB, commonProjection *CommonProjection, stripTagsPolicy *sanitizer.StripTagsPolicy, cfg *config.AppConfig) error {
	var copyCommand *ChatEdit
	err := reprint.FromTo(&sp, &copyCommand)
	if err != nil {
		return err
	}

	admin, err := commonProjection.IsChatAdmin(ctx, dba, copyCommand.AdditionalData.BehalfUserId, copyCommand.ChatId)
	if err != nil {
		return err
	}
	if !admin {
		return NewUnauthorizedError(fmt.Sprintf("user %v is not admin of chat %v", copyCommand.AdditionalData.BehalfUserId, copyCommand.ChatId))
	}

	if int32(len(copyCommand.ParticipantIdsToAdd)) > cfg.Cqrs.Commands.MaxParticipantsPerSingleCommand {
		return fmt.Errorf("Max allowed participants %d, got %d", cfg.Cqrs.Commands.MaxParticipantsPerSingleCommand, copyCommand.ParticipantIdsToAdd)
	}

	copyCommand.Title = sanitizer.TrimAmdSanitizeChatTitle(stripTagsPolicy, copyCommand.Title)

	if copyCommand.IsValidatabale() {
		if err = copyCommand.Validate(); err != nil {
			return NewValidationError(fmt.Sprintf("Error during validation: %v", err))
		}
	}

	cb, err := commonProjection.GetChatBasic(ctx, dba, copyCommand.ChatId)
	if err != nil {
		return err
	}

	if cb == nil {
		return NewChatStillNotExistsError(fmt.Sprintf("chat %d still does not exist", copyCommand.ChatId))
	}

	if cb.TetATet {
		return NewValidationError("Error during validation: tet-a-tet chat cannot be changed")
	}

	cc := &ChatEdited{
		AdditionalData:                      copyCommand.AdditionalData,
		ChatId:                              copyCommand.ChatId,
		Title:                               copyCommand.Title,
		Blog:                                copyCommand.Blog,
		Avatar:                              copyCommand.Avatar,
		AvatarBig:                           copyCommand.AvatarBig,
		CanResend:                           copyCommand.CanResend,
		CanReact:                            copyCommand.CanReact,
		AvailableToSearch:                   copyCommand.AvailableToSearch,
		RegularParticipantCanPublishMessage: copyCommand.RegularParticipantCanPublishMessage,
		RegularParticipantCanPinMessage:     copyCommand.RegularParticipantCanPinMessage,
		RegularParticipantCanWriteMessage:   copyCommand.RegularParticipantCanWriteMessage,
	}
	err = eventBus.Publish(ctx, cc)
	if err != nil {
		return err
	}

	if len(copyCommand.ParticipantIdsToAdd) > 0 {
		pa := &ParticipantsAdded{
			AdditionalData: copyCommand.AdditionalData,
			ChatId:         copyCommand.ChatId,
		}
		for _, participantId := range copyCommand.ParticipantIdsToAdd {
			pa.Participants = append(pa.Participants, ParticipantWithAdmin{
				ParticipantId: participantId,
				ChatAdmin:     false,
			})
		}
		if len(pa.Participants) == 0 {
			return NewParticipantsError("Cannot add 0 participants")
		}

		err = eventBus.Publish(ctx, pa)
		if err != nil {
			return err
		}
	}

	ui := &ChatViewRefreshed{
		AdditionalData:   copyCommand.AdditionalData,
		ParticipantsMode: ParticipantsModeAllParticipantIdsExcepting,
		// excluding => s.ParticipantIds is an optimization in order not to re-refresh views for the recently added
		AllParticipantIdsExcepting: copyCommand.ParticipantIdsToAdd,
		ChatId:                     copyCommand.ChatId,
		ChatAction:                 ChatActionRefresh,
	}

	errInner := eventBus.Publish(ctx, ui)
	if errInner != nil {
		return errInner
	}

	return nil
}

func (s *ChatDelete) Handle(ctx context.Context, eventBus EventBusInterface, dba *db.DB, commonProjection *CommonProjection) error {
	admin, err := commonProjection.IsChatAdmin(ctx, dba, s.AdditionalData.BehalfUserId, s.ChatId)
	if err != nil {
		return err
	}
	if !admin {
		return NewUnauthorizedError(fmt.Sprintf("user %v is not admin of chat %v", s.AdditionalData.BehalfUserId, s.ChatId))
	}

	pa := &ParticipantDeleted{
		AdditionalData:             s.AdditionalData,
		GetParticipantsType:        GetParticipantsTypeAllInChatExcepting,
		AllParticipantIdsExcepting: []int64{},
		ChatId:                     s.ChatId,
	}
	errInner := eventBus.Publish(ctx, pa)
	if errInner != nil {
		return errInner
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

func (s *ParticipantAdd) Handle(ctx context.Context, eventBus EventBusInterface, dba *db.DB, commonProjection *CommonProjection, cfg *config.AppConfig) error {
	basic, err := commonProjection.GetChatBasic(ctx, dba, s.ChatId)
	if err != nil {
		return err
	}

	if basic == nil {
		return NewChatStillNotExistsError(fmt.Sprintf("chat %d still does not exist", s.ChatId))
	}

	if int32(len(s.ParticipantIds)) > cfg.Cqrs.Commands.MaxParticipantsPerSingleCommand {
		return fmt.Errorf("Max allowed participants %d, got %d", cfg.Cqrs.Commands.MaxParticipantsPerSingleCommand, s.ParticipantIds)
	}

	admin, err := commonProjection.IsChatAdmin(ctx, dba, s.AdditionalData.BehalfUserId, s.ChatId)
	if err != nil {
		return err
	}
	if !admin {
		if s.IsJoining {
			if !basic.AvailableToSearch && !basic.IsBlog {
				return NewUnauthorizedError(fmt.Sprintf("user %v is not allowed to join to chat %v", s.AdditionalData.BehalfUserId, s.ChatId))
			}
		} else {
			return NewUnauthorizedError(fmt.Sprintf("user %v is not admin of chat %v", s.AdditionalData.BehalfUserId, s.ChatId))
		}
	}

	pa := &ParticipantsAdded{
		AdditionalData: s.AdditionalData,
		ChatId:         s.ChatId,
		IsJoining:      s.IsJoining,
	}
	for _, participantId := range s.ParticipantIds {
		pa.Participants = append(pa.Participants, ParticipantWithAdmin{
			ParticipantId: participantId,
			ChatAdmin:     false,
		})
	}
	if len(pa.Participants) == 0 {
		return NewParticipantsError("Cannot add 0 participants")
	}

	err = eventBus.Publish(ctx, pa)
	if err != nil {
		return err
	}

	ui := &ChatViewRefreshed{
		AdditionalData:   s.AdditionalData,
		ParticipantsMode: ParticipantsModeAllParticipantIdsExcepting,
		// chat_user_views for newly added participants will be created from scratch including already added, see ParticipantsAdded handler
		// actually don't need to send an event for newly added, their last_updated will be updated in OnParticipantAdded()
		AllParticipantIdsExcepting: s.ParticipantIds,
		ChatId:                     s.ChatId,
		ChatAction:                 ChatActionRefresh,
	}
	errInner := eventBus.Publish(ctx, ui)
	if errInner != nil {
		return errInner
	}

	return nil
}

func (s *ParticipantDelete) Handle(ctx context.Context, eventBus EventBusInterface, dba *db.DB, commonProjection *CommonProjection, cfg *config.AppConfig) error {
	admin, err := commonProjection.IsChatAdmin(ctx, dba, s.AdditionalData.BehalfUserId, s.ChatId)
	if err != nil {
		return err
	}
	if !s.IsLeaving && !admin {
		return NewUnauthorizedError(fmt.Sprintf("user %v is not admin of chat %v", s.AdditionalData.BehalfUserId, s.ChatId))
	}

	if int32(len(s.ParticipantIds)) > cfg.Cqrs.Commands.MaxParticipantsPerSingleCommand {
		return fmt.Errorf("Max allowed participants %d, got %d", cfg.Cqrs.Commands.MaxParticipantsPerSingleCommand, s.ParticipantIds)
	}

	pa := &ParticipantDeleted{
		AdditionalData:      s.AdditionalData,
		ParticipantIds:      s.ParticipantIds,
		GetParticipantsType: GetParticipantsTypeNormal,
		ChatId:              s.ChatId,
		IsLeaving:           s.IsLeaving,
	}
	err = eventBus.Publish(ctx, pa)
	if err != nil {
		return err
	}

	// excluding => s.ParticipantIds is an optimization - we don't need to refresh views for deleted participants
	if len(s.ParticipantIds) > 0 {
		ui := &ChatViewRefreshed{
			AdditionalData:             s.AdditionalData,
			ParticipantsMode:           ParticipantsModeAllParticipantIdsExcepting,
			AllParticipantIdsExcepting: s.ParticipantIds,
			ChatId:                     s.ChatId,
			ChatAction:                 ChatActionRefresh,
		}
		errInner := eventBus.Publish(ctx, ui)
		if errInner != nil {
			return errInner
		}
		return nil
	}

	return nil
}

func (s *ParticipantChange) Handle(ctx context.Context, eventBus EventBusInterface, dba *db.DB, commonProjection *CommonProjection) error {
	admin, err := commonProjection.IsChatAdmin(ctx, dba, s.AdditionalData.BehalfUserId, s.ChatId)
	if err != nil {
		return err
	}
	if !admin {
		return NewUnauthorizedError(fmt.Sprintf("user %v is not admin of chat %v", s.AdditionalData.BehalfUserId, s.ChatId))
	}

	pa := &ParticipantChanged{
		AdditionalData: s.AdditionalData,
		ParticipantId:  s.ParticipantId,
		ChatId:         s.ChatId,
		NewAdmin:       s.NewAdmin,
	}
	err = eventBus.Publish(ctx, pa)
	if err != nil {
		return err
	}

	ui := &ChatViewRefreshed{
		AdditionalData:             s.AdditionalData,
		ParticipantsMode:           ParticipantsModeAllParticipantIdsExcepting,
		AllParticipantIdsExcepting: []int64{},
		ChatId:                     s.ChatId,
		// here we don't add ParticipantsAction == ParticipantsActionRefresh because changing a participant (e. g. making him admin) shouldn't change chat_user_view
	}
	errInner := eventBus.Publish(ctx, ui)
	if errInner != nil {
		return errInner
	}
	return nil
}

func (s *ChatPin) Handle(ctx context.Context, eventBus EventBusInterface) error {
	cp := &ChatPinned{
		AdditionalData: s.AdditionalData,
		ChatId:         s.ChatId,
		Pinned:         s.Pin,
	}
	err := eventBus.Publish(ctx, cp)
	if err != nil {
		return err
	}

	ui := &ChatViewRefreshed{
		AdditionalData:     s.AdditionalData,
		ParticipantsMode:   ParticipantsModeOnlyParticipantIds,
		OnlyParticipantIds: []int64{s.AdditionalData.BehalfUserId},
		ChatId:             s.ChatId,
		ChatAction:         ChatActionRefresh,
	}

	errInner := eventBus.Publish(ctx, ui)
	if errInner != nil {
		return errInner
	}

	return nil
}

func (s *ChatNotificationSettingsSet) Handle(ctx context.Context, eventBus EventBusInterface) error {
	cp := &ChatNotificationSettingsSetted{
		AdditionalData: s.AdditionalData,
		ChatId:         s.ChatId,
		Setted:         s.Set,
	}
	return eventBus.Publish(ctx, cp)
}

func (sp *MessageCreate) Handle(ctx context.Context, eventBus EventBusInterface, dba *db.DB, commonProjection *CommonProjection, cfg *config.AppConfig, lgr *logger.LoggerWrapper, policy *sanitizer.SanitizerPolicy) (int64, error) {
	var copyCommand *MessageCreate
	err := reprint.FromTo(&sp, &copyCommand)
	if err != nil {
		return 0, err
	}

	participant, err := commonProjection.IsParticipant(ctx, dba, sp.AdditionalData.BehalfUserId, sp.ChatId)
	if err != nil {
		return 0, err
	}

	if !participant {
		return 0, NewUnauthorizedError(fmt.Sprintf("user %v is not a participant of chat %v", sp.AdditionalData.BehalfUserId, sp.ChatId))
	}

	trimmedAndSanitized, err := sanitizer.TrimAmdSanitizeMessage(ctx, cfg, lgr, policy, copyCommand.Content)
	if err != nil {
		return 0, err
	}
	copyCommand.Content = trimmedAndSanitized

	if copyCommand.IsValidatabale() {
		if err = copyCommand.Validate(); err != nil {
			return 0, NewValidationError(fmt.Sprintf("Error during validation: %v", err))
		}
	}

	mc := &MessageCreated{
		MessageCommoned: MessageCommoned{
			ChatId:  copyCommand.ChatId,
			Content: copyCommand.Content,
		},
		AdditionalData: copyCommand.AdditionalData,
	}

	err = validateAndSetEmbedFieldsEmbedMessage(ctx, dba, commonProjection, copyCommand.EmbedMessage, &mc.MessageCommoned)
	if err != nil {
		return 0, err
	}

	messageId, err := commonProjection.GetNextMessageId(ctx, dba, copyCommand.ChatId)
	if err != nil {
		return 0, err
	}

	if messageId == ChatStillNotExists {
		return 0, NewChatStillNotExistsError(fmt.Sprintf("chat %d still does not exist", copyCommand.ChatId))
	}

	mc.MessageCommoned.Id = messageId

	err = eventBus.Publish(ctx, mc)
	if err != nil {
		return 0, err
	}

	ui := &ChatViewRefreshed{
		AdditionalData:             copyCommand.AdditionalData,
		ParticipantsMode:           ParticipantsModeAllParticipantIdsExcepting,
		AllParticipantIdsExcepting: []int64{},
		ChatId:                     copyCommand.ChatId,
		UnreadMessagesAction:       UnreadMessagesActionIncrease,
		IncreaseOn:                 1,
		LastMessageAction:          LastMessageActionRefresh,
	}

	errInner := eventBus.Publish(ctx, ui)
	if errInner != nil {
		return 0, errInner
	}

	return messageId, nil
}

func (s *MessageRead) Handle(ctx context.Context, eventBus EventBusInterface, commonProjection *CommonProjection, dba *db.DB) error {
	if s.ReadMessagesAction == ReadMessagesActionAllMessagesInOneChat {
		cp := &MessageReaded{
			AdditionalData:     s.AdditionalData,
			ReadMessagesAction: ReadMessagesActionAllMessagesInOneChat,
			ChatId:             s.ChatId,
		}
		err := eventBus.Publish(ctx, cp)
		if err != nil {
			return err
		}
		return nil
	} else if s.ReadMessagesAction == ReadMessagesActionAllChats {
		cp := &MessageReaded{
			AdditionalData:     s.AdditionalData,
			ReadMessagesAction: ReadMessagesActionAllChats,
		}
		err := eventBus.Publish(ctx, cp)
		if err != nil {
			return err
		}
		return nil
	} else if s.ReadMessagesAction == ReadMessagesActionOneMessage {
		participant, err := commonProjection.IsParticipant(ctx, dba, s.AdditionalData.BehalfUserId, s.ChatId)
		if err != nil {
			return err
		}

		if !participant {
			return NewUnauthorizedError(fmt.Sprintf("user %v is not a participant of chat %v", s.AdditionalData.BehalfUserId, s.ChatId))
		}

		lastMessageReadedId, lastMessgeReadedExists, maxMessageId, err := commonProjection.GetLastMessageReaded(ctx, s.ChatId, s.AdditionalData.BehalfUserId)
		if err != nil {
			return err
		}
		messageIdToMark := s.MessageId
		if s.MessageId > maxMessageId {
			messageIdToMark = maxMessageId
		}
		// Optimizations in order to not send useless messages in Kafka
		if (lastMessgeReadedExists && messageIdToMark > lastMessageReadedId) || (!lastMessgeReadedExists && lastMessageReadedId == 0) {
			cp := &MessageReaded{
				AdditionalData:     s.AdditionalData,
				ChatId:             s.ChatId,
				MessageId:          messageIdToMark,
				ReadMessagesAction: ReadMessagesActionOneMessage,
			}
			err = eventBus.Publish(ctx, cp)
			if err != nil {
				return err
			}
		}
		return nil
	} else {
		return fmt.Errorf("Unknown action: %T", s.ReadMessagesAction)
	}
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

func (s *MessageDelete) Handle(ctx context.Context, eventBus EventBusInterface, dba *db.DB, commonProjection *CommonProjection) error {
	participant, err := commonProjection.IsParticipant(ctx, dba, s.AdditionalData.BehalfUserId, s.ChatId)
	if err != nil {
		return err
	}
	if !participant {
		return NewUnauthorizedError(fmt.Sprintf("user %v is not a participant of chat %v", s.AdditionalData.BehalfUserId, s.ChatId))
	}

	ownerId, err := commonProjection.GetMessageOwner(ctx, s.ChatId, s.MessageId)
	if err != nil {
		return err
	}

	if ownerId != s.AdditionalData.BehalfUserId {
		return fmt.Errorf("User %v is not an owner of message %v in chat %v", s.AdditionalData.BehalfUserId, s.MessageId, s.ChatId)
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

	ui := &ChatViewRefreshed{
		AdditionalData:             s.AdditionalData,
		ParticipantsMode:           ParticipantsModeAllParticipantIdsExcepting,
		AllParticipantIdsExcepting: []int64{},
		ChatId:                     s.ChatId,
		UnreadMessagesAction:       UnreadMessagesActionRefresh,
		LastMessageAction:          LastMessageActionRefresh,
	}

	errInner := eventBus.Publish(ctx, ui)
	if errInner != nil {
		return errInner
	}

	return nil
}

func (sp *MessageEdit) Handle(ctx context.Context, eventBus EventBusInterface, dba *db.DB, commonProjection *CommonProjection, cfg *config.AppConfig, lgr *logger.LoggerWrapper, policy *sanitizer.SanitizerPolicy) error {
	var copyCommand *MessageEdit
	err := reprint.FromTo(&sp, &copyCommand)
	if err != nil {
		return err
	}

	participant, err := commonProjection.IsParticipant(ctx, dba, copyCommand.AdditionalData.BehalfUserId, sp.ChatId)
	if err != nil {
		return err
	}
	if !participant {
		return NewUnauthorizedError(fmt.Sprintf("user %v is not a participant of chat %v", copyCommand.AdditionalData.BehalfUserId, sp.ChatId))
	}

	ownerId, err := commonProjection.GetMessageOwner(ctx, copyCommand.ChatId, copyCommand.MessageId)
	if err != nil {
		return err
	}

	if ownerId != copyCommand.AdditionalData.BehalfUserId {
		return NewUnauthorizedError(fmt.Sprintf("User %v is not an owner of message %v in chat %v", copyCommand.AdditionalData.BehalfUserId, copyCommand.MessageId, copyCommand.ChatId))
	}

	trimmedAndSanitized, err := sanitizer.TrimAmdSanitizeMessage(ctx, cfg, lgr, policy, copyCommand.Content)
	if err != nil {
		return err
	}
	copyCommand.Content = trimmedAndSanitized

	if copyCommand.IsValidatabale() {
		if err = copyCommand.Validate(); err != nil {
			return NewValidationError(fmt.Sprintf("Error during validation: %v", err))
		}
	}

	cp := &MessageEdited{
		MessageCommoned: MessageCommoned{
			Id:      copyCommand.MessageId,
			ChatId:  copyCommand.ChatId,
			Content: copyCommand.Content,
		},
		AdditionalData: copyCommand.AdditionalData,
	}

	err = validateAndSetEmbedFieldsEmbedMessage(ctx, dba, commonProjection, copyCommand.EmbedMessage, &cp.MessageCommoned)
	if err != nil {
		return err
	}

	err = eventBus.Publish(ctx, cp)
	if err != nil {
		return err
	}

	lastMessageId, err := commonProjection.GetLastMessageId(ctx, copyCommand.ChatId)
	if lastMessageId == copyCommand.MessageId {
		ui := &ChatViewRefreshed{
			AdditionalData:             copyCommand.AdditionalData,
			ParticipantsMode:           ParticipantsModeAllParticipantIdsExcepting,
			AllParticipantIdsExcepting: []int64{},
			ChatId:                     copyCommand.ChatId,
			LastMessageAction:          LastMessageActionRefresh,
		}

		errInner := eventBus.Publish(ctx, ui)
		if errInner != nil {
			return errInner
		}
		return nil
	}
	return nil
}

func (s *MessageReactionFlip) Handle(ctx context.Context, eventBus EventBusInterface, dba *db.DB, commonProjection *CommonProjection, policy *sanitizer.SanitizerPolicy) error {
	participant, err := commonProjection.IsParticipant(ctx, dba, s.AdditionalData.BehalfUserId, s.ChatId)
	if err != nil {
		return err
	}

	if !participant {
		return NewUnauthorizedError(fmt.Sprintf("user %v is not a participant of chat %v", s.AdditionalData.BehalfUserId, s.ChatId))
	}

	sanitizedReaction := sanitizer.TrimAmdSanitize(policy, s.Reaction)

	if len([]rune(sanitizedReaction)) > 4 || len([]rune(sanitizedReaction)) < 1 {
		return NewValidationError("Wrong length of reaction")
	}

	cp := &MessageReactionFlipped{
		AdditionalData: s.AdditionalData,
		ChatId:         s.ChatId,
		MessageId:      s.MessageId,
		Reaction:       sanitizedReaction,
	}

	err = eventBus.Publish(ctx, cp)
	if err != nil {
		return err
	}

	return nil
}

func validateAndSetEmbedFieldsEmbedMessage(ctx context.Context, dba *db.DB, commonProjection *CommonProjection, embedMessageRequest *EmbedMessage, receiver *MessageCommoned) error {
	if embedMessageRequest != nil {
		if embedMessageRequest.Id == 0 {
			return errors.New("Missed embed message id")
		}
		if embedMessageRequest.EmbedType == "" {
			return errors.New("Missed embedMessageType")
		} else {
			if embedMessageRequest.EmbedType != dto.EmbedMessageTypeReply && embedMessageRequest.EmbedType != dto.EmbedMessageTypeResend {
				return errors.New("Wrong embedMessageType")
			}
			if embedMessageRequest.EmbedType == dto.EmbedMessageTypeResend && embedMessageRequest.ChatId == 0 {
				return errors.New("Missed embedChatId for EmbedMessageTypeResend")
			}
		}

		if embedMessageRequest.EmbedType == dto.EmbedMessageTypeReply {
			receiver.EmbedMessageId = &embedMessageRequest.Id
			receiver.EmbedMessageType = &embedMessageRequest.EmbedType
			return nil
		} else if embedMessageRequest.EmbedType == dto.EmbedMessageTypeResend {
			receiver.EmbedMessageId = &embedMessageRequest.Id
			receiver.EmbedMessageType = &embedMessageRequest.EmbedType

			chat, err := commonProjection.GetChatBasic(ctx, dba, embedMessageRequest.ChatId)
			if err != nil {
				return err
			}
			if !chat.CanResend {
				return errors.New("Resending is forbidden for this chat")
			}
			m, err := commonProjection.GetMessageBasic(ctx, dba, embedMessageRequest.ChatId, embedMessageRequest.Id)
			if err != nil {
				return err
			}
			if m == nil {
				return errors.New("Missing the message")
			}
			receiver.Content = m.Content
			receiver.EmbedMessageOwnerId = &m.OwnerId
			receiver.EmbedMessageChatId = &embedMessageRequest.ChatId
			return nil
		}
		return fmt.Errorf("Unexpected embed type '%v'", embedMessageRequest.EmbedType)
	}

	return nil
}
