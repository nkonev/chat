package cqrs

import (
	"context"
	"errors"
	"fmt"
	"github.com/PuerkitoBio/goquery"
	validation "github.com/go-ozzo/ozzo-validation/v4"
	"github.com/qdm12/reprint"
	"go-cqrs-chat-example/config"
	"go-cqrs-chat-example/db"
	"go-cqrs-chat-example/dto"
	"go-cqrs-chat-example/logger"
	"go-cqrs-chat-example/services"
	"go-cqrs-chat-example/utils"
	"net/url"
	"slices"
	"strings"
)

const ReservedPublicallyAvailableForSearchChats = "__AVAILABLE_FOR_SEARCH"

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
	BehalfUserId        int64
}

func (cc *ChatEdit) IsValidatabale() bool {
	return true
}

func (a *ChatEdit) Validate() error {
	return validation.ValidateStruct(a,
		validation.Field(&a.Title, validation.Required, validation.Length(minChatNameLen, maxChatNameLen), validation.NotIn(ReservedPublicallyAvailableForSearchChats)),
		validation.Field(&a.ChatId, validation.Required),
	)
}

func (cc *ChatCreate) IsValidatabale() bool {
	return true
}

func (a *ChatCreate) Validate() error {
	return validation.ValidateStruct(a,
		validation.Field(&a.Title, validation.Required, validation.Length(minChatNameLen, maxChatNameLen), validation.NotIn(ReservedPublicallyAvailableForSearchChats)),
	)
}

type ChatDelete struct {
	ChatId         int64
	AdditionalData *AdditionalData
	BehalfUserId   int64
}

type ParticipantAdd struct {
	AdditionalData *AdditionalData
	ChatId         int64
	ParticipantIds []int64
	BehalfUserId   int64
}

type ParticipantDelete struct {
	AdditionalData *AdditionalData
	ChatId         int64
	ParticipantIds []int64
	BehalfUserId   int64
}

type ParticipantChange struct {
	AdditionalData *AdditionalData
	ChatId         int64
	ParticipantId  int64
	NewAdmin       bool
	BehalfUserId   int64
}

type EmbedMessage struct {
	Id        int64
	ChatId    int64
	EmbedType string
}

type MessageCreate struct {
	AdditionalData *AdditionalData
	ChatId         int64
	OwnerId        int64
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
	BehalfUserId   int64
}

func (sp *ChatCreate) Handle(ctx context.Context, behalfUserId int64, eventBus EventBusInterface, dba *db.DB, commonProjection *CommonProjection, stripTagsPolicy *services.StripTagsPolicy) (int64, error) {
	var copyCommand *ChatCreate
	err := reprint.FromTo(sp, copyCommand)
	if err != nil {
		return 0, err
	}

	if !slices.Contains(copyCommand.ParticipantIds, behalfUserId) {
		copyCommand.ParticipantIds = append(copyCommand.ParticipantIds, behalfUserId)
	}

	copyCommand.Title = TrimAmdSanitizeChatTitle(stripTagsPolicy, copyCommand.Title)

	if copyCommand.IsValidatabale() {
		if err = copyCommand.Validate(); err != nil {
			return 0, NewValidationError(fmt.Sprintf("Error during validation: %v", err))
		}
	}

	chatId, err := db.TransactWithResult(ctx, dba, func(tx *db.Tx) (int64, error) {
		return commonProjection.GetNextChatId(ctx, tx)
	})
	if err != nil {
		return 0, err
	}

	cc := &ChatCreated{
		AdditionalData: copyCommand.AdditionalData,
		ChatId:         chatId,
		Title:          copyCommand.Title,
	}
	err = eventBus.Publish(ctx, cc)
	if err != nil {
		return 0, err
	}

	pa := &ParticipantsAdded{
		AdditionalData:     copyCommand.AdditionalData,
		ChatId:             chatId,
		Participants:       make([]ParticipantWithAdmin, 0),
		BehalfUserId:       behalfUserId,
		SkipChatAdminCheck: true,
	}
	for _, participantId := range copyCommand.ParticipantIds {
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

func (sp *ChatEdit) Handle(ctx context.Context, eventBus EventBusInterface, dba *db.DB, commonProjection *CommonProjection, stripTagsPolicy *services.StripTagsPolicy) error {
	var copyCommand *ChatEdit
	err := reprint.FromTo(sp, copyCommand)
	if err != nil {
		return err
	}

	admin, err := commonProjection.IsChatAdmin(ctx, dba, copyCommand.BehalfUserId, copyCommand.ChatId)
	if err != nil {
		return err
	}
	if !admin {
		return NewUnauthorizedError(fmt.Sprintf("user %v is not admin of chat %v", copyCommand.BehalfUserId, copyCommand.ChatId))
	}

	copyCommand.Title = TrimAmdSanitizeChatTitle(stripTagsPolicy, copyCommand.Title)

	if copyCommand.IsValidatabale() {
		if err = copyCommand.Validate(); err != nil {
			return NewValidationError(fmt.Sprintf("Error during validation: %v", err))
		}
	}

	cc := &ChatEdited{
		AdditionalData: copyCommand.AdditionalData,
		ChatId:         copyCommand.ChatId,
		Title:          copyCommand.Title,
		Blog:           copyCommand.Blog,
		BehalfUserId:   copyCommand.BehalfUserId,
	}
	err = eventBus.Publish(ctx, cc)
	if err != nil {
		return err
	}

	if len(copyCommand.ParticipantIdsToAdd) > 0 {
		pa := &ParticipantsAdded{
			AdditionalData: copyCommand.AdditionalData,
			ChatId:         copyCommand.ChatId,
			BehalfUserId:   copyCommand.BehalfUserId,
		}
		for _, participantId := range copyCommand.ParticipantIdsToAdd {
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

	errOuter := commonProjection.IterateOverChatParticipantIds(ctx, dba, copyCommand.ChatId, nil, func(participantIdsPortion []int64) error {
		ui := &ChatViewRefreshed{
			AdditionalData:   copyCommand.AdditionalData,
			ParticipantIds:   participantIdsPortion,
			ChatId:           copyCommand.ChatId,
			ChatCommonAction: ChatCommonActionRefresh,
			Title:            copyCommand.Title,
		}

		if len(copyCommand.ParticipantIdsToAdd) > 0 {
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
			BehalfUserId:   s.BehalfUserId,
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
		BehalfUserId:   s.BehalfUserId,
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
		BehalfUserId:   s.BehalfUserId,
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
		BehalfUserId:   s.BehalfUserId,
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

func (sp *MessageCreate) Handle(ctx context.Context, eventBus EventBusInterface, dba *db.DB, commonProjection *CommonProjection, cfg *config.AppConfig, lgr *logger.LoggerWrapper, policy *services.SanitizerPolicy) (int64, error) {
	var copyCommand *MessageCreate
	err := reprint.FromTo(sp, copyCommand)
	if err != nil {
		return 0, err
	}

	trimmedAndSanitized, err := TrimAmdSanitizeMessage(ctx, cfg, lgr, policy, copyCommand.Content)
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
		OwnerId:        copyCommand.OwnerId,
	}

	err = validateAndSetEmbedFieldsEmbedMessage(ctx, dba, commonProjection, copyCommand.EmbedMessage, &mc.MessageCommoned)
	if err != nil {
		return 0, err
	}

	messageId, err := db.TransactWithResult(ctx, dba, func(tx *db.Tx) (int64, error) {
		return commonProjection.GetNextMessageId(ctx, tx, copyCommand.ChatId)
	})
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

	errOuter := commonProjection.IterateOverChatParticipantIds(ctx, dba, copyCommand.ChatId, nil, func(participantIdsPortion []int64) error {
		ui := &ChatViewRefreshed{
			AdditionalData:       copyCommand.AdditionalData,
			ParticipantIds:       participantIdsPortion,
			ChatId:               copyCommand.ChatId,
			UnreadMessagesAction: UnreadMessagesActionIncrease,
			IncreaseOn:           1,
			OwnerId:              copyCommand.OwnerId,
			LastMessageAction:    LastMessageActionRefresh,
		}

		errInner := eventBus.Publish(ctx, ui)
		if errInner != nil {
			return errInner
		}
		return nil
	})

	if errOuter != nil {
		return 0, errOuter
	}

	return messageId, nil
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
		BehalfUserId:   s.BehalfUserId,
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

func (sp *MessageEdit) Handle(ctx context.Context, eventBus EventBusInterface, dba *db.DB, commonProjection *CommonProjection, userId int64, cfg *config.AppConfig, lgr *logger.LoggerWrapper, policy *services.SanitizerPolicy) error {
	var copyCommand *MessageEdit
	err := reprint.FromTo(sp, copyCommand)
	if err != nil {
		return err
	}

	ownerId, err := commonProjection.GetMessageOwner(ctx, copyCommand.ChatId, copyCommand.MessageId)
	if err != nil {
		return err
	}

	if ownerId != userId {
		return NewUnauthorizedError(fmt.Sprintf("User %v is not an owner of message %v in chat %v", userId, copyCommand.MessageId, copyCommand.ChatId))
	}

	trimmedAndSanitized, err := TrimAmdSanitizeMessage(ctx, cfg, lgr, policy, copyCommand.Content)
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
		// if it's the last chat message then update ChatView
		errOuter := commonProjection.IterateOverChatParticipantIds(ctx, dba, copyCommand.ChatId, nil, func(participantIdsPortion []int64) error {
			ui := &ChatViewRefreshed{
				AdditionalData:    copyCommand.AdditionalData,
				ParticipantIds:    participantIdsPortion,
				ChatId:            copyCommand.ChatId,
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
			// check if this input.EmbedChatId resendable
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

func TrimAmdSanitizeChatTitle(policy *services.StripTagsPolicy, title string) string {
	t := Trim(policy.Sanitize(title))
	return t
}

func Trim(str string) string {
	return strings.TrimSpace(str)
}

func SanitizeMessage(policy *services.SanitizerPolicy, input string) string {
	return policy.Sanitize(input)
}

func TrimAmdSanitizeMessage(ctx context.Context, cfg *config.AppConfig, lgr *logger.LoggerWrapper, policy *services.SanitizerPolicy, input string) (string, error) {
	sanitizedHtml := Trim(SanitizeMessage(policy, input))

	whitelist := cfg.MessageConfig.AllowedMediaUrls
	wlArr := strings.Split(whitelist, ",")
	frontendUrl := cfg.FrontendUrl
	wlArr = append(wlArr, frontendUrl)
	wlArr = append(wlArr, "") // storage urls without protocol://host:port

	iframeWhitelist := cfg.MessageConfig.AllowedIframeUrls
	iframeWlArr := strings.Split(iframeWhitelist, ",")

	// Load the HTML document
	doc, err := goquery.NewDocumentFromReader(strings.NewReader(sanitizedHtml))
	if err != nil {
		lgr.WarnContext(ctx, "Unable to read html", "err", err)
		return "", errors.New("Unable to read html")
	}

	var retErr error
	maxMediasCount := cfg.MessageConfig.MaxMedias
	mediaCount := 0

	doc.Find("img").Each(func(i int, s *goquery.Selection) {
		maybeImage := s.First()
		if maybeImage != nil {
			src, exists := maybeImage.Attr("src")
			if exists && !utils.ContainsUrl(ctx, lgr, wlArr, src) {
				lgr.InfoContext(ctx, "Filtered not allowed url in image src", "src", src)
				retErr = &MediaUrlErr{src, "image src"}
			}
			if exists {
				fixedSrc, err := removeProtocolHostPortIfNeed(src, frontendUrl)
				if err != nil {
					retErr = err
				}
				maybeImage.SetAttr("src", fixedSrc)
			}

			original, originalExists := maybeImage.Attr("data-original")
			if originalExists && (!utils.ContainsUrl(ctx, lgr, wlArr, original) && !utils.ContainsUrl(ctx, lgr, iframeWlArr, original)) {
				lgr.InfoContext(ctx, "Filtered not allowed url in image src", "src", original)
				retErr = &MediaUrlErr{original, "image src"}
			}
			if originalExists {
				fixedSrc, err := removeProtocolHostPortIfNeed(original, frontendUrl)
				if err != nil {
					retErr = err
				}
				maybeImage.SetAttr("data-original", fixedSrc)
			}

			mediaCount++
		}
	})
	if retErr != nil {
		return "", retErr
	}

	if mediaCount > maxMediasCount {
		retErr = &MediaOverflowErr{maxMediasCount, mediaCount}
		return "", retErr
	}

	doc.Find("a").Each(func(i int, s *goquery.Selection) {
		maybeA := s.First()
		if maybeA != nil {
			src, exists := maybeA.Attr("href")
			if exists {
				fixedSrc, err := removeProtocolHostPortIfNeed(src, frontendUrl)
				if err != nil {
					retErr = err
				}
				maybeA.SetAttr("href", fixedSrc)
			}
		}
	})
	if retErr != nil {
		return "", retErr
	}

	ret, err := doc.Find("html").Find("body").Html()
	if err != nil {
		lgr.WarnContext(ctx, "Unable to write html", "err", err)
		return "", err
	}

	return ret, nil
}

type MediaUrlErr struct {
	url   string
	where string
}

func (s *MediaUrlErr) Error() string {
	return fmt.Sprintf("Media url is not allowed in %v: %v", s.where, s.url)
}

type MediaOverflowErr struct {
	allowed int
	given   int
}

func (s *MediaOverflowErr) Error() string {
	return fmt.Sprintf("Too many medias: allowed %v, given %v", s.allowed, s.given)
}

func removeProtocolHostPortIfNeed(src, frontendUrl string) (string, error) {
	parsed, err := url.Parse(src)
	if err != nil {
		return "", err
	}

	parsedAllowedUrl, err := url.Parse(frontendUrl)
	if err != nil {
		return "", err
	}

	if utils.ContainUrl(parsed, parsedAllowedUrl) {
		parsed.Host = ""
		parsed.Scheme = ""
		parsed.User = nil
	}
	return parsed.String(), nil
}
