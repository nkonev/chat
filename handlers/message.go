package handlers

import (
	"errors"
	"fmt"
	"go-cqrs-chat-example/config"
	"go-cqrs-chat-example/cqrs"
	"go-cqrs-chat-example/db"
	"go-cqrs-chat-example/dto"
	"go-cqrs-chat-example/logger"
	"go-cqrs-chat-example/sanitizer"
	"go-cqrs-chat-example/services"
	"go-cqrs-chat-example/utils"
	"net/http"

	"github.com/gin-gonic/gin"
)

const badMediaUrl = "BAD_MEDIA_URL"

type MessageHandler struct {
	lgr                 *logger.LoggerWrapper
	eventBus            *cqrs.PartitionAwareEventBus
	dbWrapper           *db.DB
	commonProjection    *cqrs.CommonProjection
	policy              *sanitizer.SanitizerPolicy
	cfg                 *config.AppConfig
	enrichingProjection *cqrs.EnrichingProjection
	asyncMessageService *services.AsyncMessageService
}

func NewMessageHandler(
	lgr *logger.LoggerWrapper,
	eventBus *cqrs.PartitionAwareEventBus,
	dbWrapper *db.DB,
	commonProjection *cqrs.CommonProjection,
	policy *sanitizer.SanitizerPolicy,
	cfg *config.AppConfig,
	enrichingProjection *cqrs.EnrichingProjection,
	messageService *services.AsyncMessageService, // we use async message service in order not to perform potentially heavyweight iterations in user-facing handles
) *MessageHandler {
	return &MessageHandler{
		lgr:                 lgr,
		eventBus:            eventBus,
		dbWrapper:           dbWrapper,
		commonProjection:    commonProjection,
		policy:              policy,
		cfg:                 cfg,
		enrichingProjection: enrichingProjection,
		asyncMessageService: messageService,
	}
}

func (mc *MessageHandler) CreateMessage(g *gin.Context) {
	cid := g.Param(dto.ChatIdParam)

	chatId, err := utils.ParseInt64(cid)
	if err != nil {
		mc.lgr.ErrorContext(g.Request.Context(), "Error binding chatId", "err", err)
		g.Status(http.StatusInternalServerError)
		return
	}

	userId, err := getUserId(g)
	if err != nil {
		mc.lgr.ErrorContext(g.Request.Context(), "Error parsing UserId", "err", err)
		g.Status(http.StatusInternalServerError)
		return
	}

	mcd := new(dto.MessageCreateDto)

	err = g.Bind(mcd)
	if err != nil {
		mc.lgr.ErrorContext(g.Request.Context(), "Error binding MessageCreateDto", "err", err)
		g.Status(http.StatusInternalServerError)
		return
	}

	cc := cqrs.MessageCreate{
		AdditionalData: cqrs.GenerateMessageAdditionalData(getCorrelationId(g), userId),
		ChatId:         chatId,
		Content:        mcd.Content,
	}
	if mcd.EmbedMessageRequest != nil {
		cc.EmbedMessage = &cqrs.EmbedMessage{
			Id:        mcd.EmbedMessageRequest.Id,
			ChatId:    mcd.EmbedMessageRequest.ChatId,
			EmbedType: mcd.EmbedMessageRequest.EmbedType,
		}
	}

	mid, err := cc.Handle(g.Request.Context(), mc.eventBus, mc.dbWrapper, mc.commonProjection, mc.cfg, mc.lgr, mc.policy)
	if err != nil {
		if translateMessageError(g, err) {
			return
		}

		mc.lgr.ErrorContext(g.Request.Context(), "Error sending MessageCreate command", "err", err)
		g.Status(http.StatusInternalServerError)
		return
	}

	m := dto.IdResponse{Id: mid}

	g.JSON(http.StatusOK, m)
}

func (mc *MessageHandler) EditMessage(g *gin.Context) {
	cid := g.Param(dto.ChatIdParam)
	chatId, err := utils.ParseInt64(cid)
	if err != nil {
		mc.lgr.ErrorContext(g.Request.Context(), "Error binding chatId", "err", err)
		g.Status(http.StatusInternalServerError)
		return
	}

	userId, err := getUserId(g)
	if err != nil {
		mc.lgr.ErrorContext(g.Request.Context(), "Error parsing UserId", "err", err)
		g.Status(http.StatusInternalServerError)
		return
	}

	ccd := new(dto.MessageEditDto)

	err = g.Bind(ccd)
	if err != nil {
		mc.lgr.ErrorContext(g.Request.Context(), "Error binding MessageEditDto", "err", err)
		g.Status(http.StatusInternalServerError)
		return
	}

	cc := cqrs.MessageEdit{
		AdditionalData: cqrs.GenerateMessageAdditionalData(getCorrelationId(g), userId),
		MessageId:      ccd.Id,
		ChatId:         chatId,
		Content:        ccd.Content,
	}
	if ccd.EmbedMessageRequest != nil {
		cc.EmbedMessage = &cqrs.EmbedMessage{
			Id:        ccd.EmbedMessageRequest.Id,
			ChatId:    ccd.EmbedMessageRequest.ChatId,
			EmbedType: ccd.EmbedMessageRequest.EmbedType,
		}
	}

	err = cc.Handle(g.Request.Context(), mc.eventBus, mc.dbWrapper, mc.commonProjection, mc.cfg, mc.lgr, mc.policy)
	if err != nil {
		if translateMessageError(g, err) {
			return
		}

		mc.lgr.ErrorContext(g.Request.Context(), "Error sending MessageEdit command", "err", err)
		g.Status(http.StatusInternalServerError)
		return
	}

	g.Status(http.StatusOK)
}

func (mc *MessageHandler) DeleteMessage(g *gin.Context) {
	cid := g.Param(dto.ChatIdParam)
	chatId, err := utils.ParseInt64(cid)
	if err != nil {
		mc.lgr.ErrorContext(g.Request.Context(), "Error binding chatId", "err", err)
		g.Status(http.StatusInternalServerError)
		return
	}

	mid := g.Param(dto.MessageIdParam)
	messageId, err := utils.ParseInt64(mid)
	if err != nil {
		mc.lgr.ErrorContext(g.Request.Context(), "Error binding messageId", "err", err)
		g.Status(http.StatusInternalServerError)
		return
	}

	userId, err := getUserId(g)
	if err != nil {
		mc.lgr.ErrorContext(g.Request.Context(), "Error parsing UserId", "err", err)
		g.Status(http.StatusInternalServerError)
		return
	}

	cc := cqrs.MessageDelete{
		AdditionalData: cqrs.GenerateMessageAdditionalData(getCorrelationId(g), userId),
		MessageId:      messageId,
		ChatId:         chatId,
	}

	err = cc.Handle(g.Request.Context(), mc.eventBus, mc.dbWrapper, mc.commonProjection)
	if err != nil {
		if translateMessageError(g, err) {
			return
		}

		mc.lgr.ErrorContext(g.Request.Context(), "Error sending MessageDelete command", "err", err)
		g.Status(http.StatusInternalServerError)
		return
	}

	g.Status(http.StatusOK)
}

func (mc *MessageHandler) ReadMessage(g *gin.Context) {
	cid := g.Param(dto.ChatIdParam)

	chatId, err := utils.ParseInt64(cid)
	if err != nil {
		mc.lgr.ErrorContext(g.Request.Context(), "Error binding chatId", "err", err)
		g.Status(http.StatusInternalServerError)
		return
	}

	mid := g.Param(dto.MessageIdParam)

	messageId, err := utils.ParseInt64(mid)
	if err != nil {
		mc.lgr.ErrorContext(g.Request.Context(), "Error binding messageId", "err", err)
		g.Status(http.StatusInternalServerError)
		return
	}

	userId, err := getUserId(g)
	if err != nil {
		mc.lgr.ErrorContext(g.Request.Context(), "Error parsing UserId", "err", err)
		g.Status(http.StatusInternalServerError)
		return
	}

	mr := cqrs.MessageRead{
		AdditionalData:     cqrs.GenerateMessageAdditionalData(getCorrelationId(g), userId),
		ChatId:             chatId,
		MessageId:          messageId,
		ReadMessagesAction: cqrs.ReadMessagesActionOneMessage,
	}

	err = mr.Handle(g.Request.Context(), mc.eventBus, mc.commonProjection, mc.dbWrapper)
	if err != nil {
		if translateMessageError(g, err) {
			return
		}

		mc.lgr.ErrorContext(g.Request.Context(), "Error sending MessageRead command", "err", err)
		g.Status(http.StatusInternalServerError)
		return
	}

	g.Status(http.StatusOK)
}

func (mc *MessageHandler) MarkChatAsRead(g *gin.Context) {
	cid := g.Param(dto.ChatIdParam)

	chatId, err := utils.ParseInt64(cid)
	if err != nil {
		mc.lgr.ErrorContext(g.Request.Context(), "Error binding chatId", "err", err)
		g.Status(http.StatusInternalServerError)
		return
	}

	userId, err := getUserId(g)
	if err != nil {
		mc.lgr.ErrorContext(g.Request.Context(), "Error parsing UserId", "err", err)
		g.Status(http.StatusInternalServerError)
		return
	}

	mr := cqrs.MessageRead{
		AdditionalData:     cqrs.GenerateMessageAdditionalData(getCorrelationId(g), userId),
		ReadMessagesAction: cqrs.ReadMessagesActionAllMessagesInOneChat,
		ChatId:             chatId,
	}

	err = mr.Handle(g.Request.Context(), mc.eventBus, mc.commonProjection, mc.dbWrapper)
	if err != nil {
		if translateMessageError(g, err) {
			return
		}

		mc.lgr.ErrorContext(g.Request.Context(), "Error sending MessageRead command", "err", err)
		g.Status(http.StatusInternalServerError)
		return
	}

	g.Status(http.StatusOK)
}

func (mc *MessageHandler) MarkAsReadAllChats(g *gin.Context) {

	userId, err := getUserId(g)
	if err != nil {
		mc.lgr.ErrorContext(g.Request.Context(), "Error parsing UserId", "err", err)
		g.Status(http.StatusInternalServerError)
		return
	}

	mr := cqrs.MessageRead{
		AdditionalData:     cqrs.GenerateMessageAdditionalData(getCorrelationId(g), userId),
		ReadMessagesAction: cqrs.ReadMessagesActionAllChats,
	}

	err = mr.Handle(g.Request.Context(), mc.eventBus, mc.commonProjection, mc.dbWrapper)
	if err != nil {
		if translateMessageError(g, err) {
			return
		}

		mc.lgr.ErrorContext(g.Request.Context(), "Error sending MessageRead command", "err", err)
		g.Status(http.StatusInternalServerError)
		return
	}

	g.Status(http.StatusOK)
}

func (mc *MessageHandler) ReactionMessage(g *gin.Context) {
	userId, err := getUserId(g)
	if err != nil {
		mc.lgr.ErrorContext(g.Request.Context(), "Error parsing UserId", "err", err)
		g.Status(http.StatusInternalServerError)
		return
	}

	cid := g.Param(dto.ChatIdParam)
	chatId, err := utils.ParseInt64(cid)
	if err != nil {
		mc.lgr.ErrorContext(g.Request.Context(), "Error binding chatId", "err", err)
		g.Status(http.StatusInternalServerError)
		return
	}

	mid := g.Param(dto.MessageIdParam)

	messageId, err := utils.ParseInt64(mid)
	if err != nil {
		mc.lgr.ErrorContext(g.Request.Context(), "Error binding messageId", "err", err)
		g.Status(http.StatusInternalServerError)
		return
	}

	ccd := new(dto.ReactionPutDto)

	err = g.Bind(ccd)
	if err != nil {
		mc.lgr.ErrorContext(g.Request.Context(), "Error binding ReactionPutDto", "err", err)
		g.Status(http.StatusInternalServerError)
		return
	}

	mr := cqrs.MessageReactionFlip{
		AdditionalData: cqrs.GenerateMessageAdditionalData(getCorrelationId(g), userId),
		ChatId:         chatId,
		MessageId:      messageId,
		Reaction:       ccd.Reaction,
	}

	err = mr.Handle(g.Request.Context(), mc.eventBus, mc.dbWrapper, mc.commonProjection)
	if err != nil {
		if translateMessageError(g, err) {
			return
		}

		mc.lgr.ErrorContext(g.Request.Context(), "Error sending MessageReactionFlip command", "err", err)
		g.Status(http.StatusInternalServerError)
		return
	}

	g.Status(http.StatusOK)
}

func (mc *MessageHandler) TypeMessage(g *gin.Context) {
	userId, err := getUserId(g)
	if err != nil {
		mc.lgr.ErrorContext(g.Request.Context(), "Error parsing UserId", "err", err)
		g.Status(http.StatusInternalServerError)
		return
	}

	userLogin, err := getUserLogin(g)
	if err != nil {
		mc.lgr.ErrorContext(g.Request.Context(), "Error parsing userLogin", "err", err)
		g.Status(http.StatusInternalServerError)
		return
	}

	cid := g.Param(dto.ChatIdParam)
	chatId, err := utils.ParseInt64(cid)
	if err != nil {
		mc.lgr.ErrorContext(g.Request.Context(), "Error binding chatId", "err", err)
		g.Status(http.StatusInternalServerError)
		return
	}

	participant, err := mc.commonProjection.IsParticipant(g.Request.Context(), mc.dbWrapper, userId, chatId)
	if err != nil {
		mc.lgr.ErrorContext(g.Request.Context(), "Error checking is participant", "err", err)
		g.Status(http.StatusInternalServerError)
		return
	}
	if !participant {
		mc.lgr.InfoContext(g.Request.Context(), fmt.Sprintf("User %v is not participant of chat %v, skipping", userId, chatId))
		g.Status(http.StatusOK)
		return
	}

	d := new(dto.BroadcastDto)

	err = g.Bind(d)
	if err != nil {
		mc.lgr.ErrorContext(g.Request.Context(), "Error binding BroadcastDto", "err", err)
		g.Status(http.StatusInternalServerError)
		return
	}

	mc.asyncMessageService.TypeMessage(g.Request.Context(), chatId, userId, userLogin)

	g.Status(http.StatusOK)
	return
}

func (mc *MessageHandler) BroadcastMessage(g *gin.Context) {
	userId, err := getUserId(g)
	if err != nil {
		mc.lgr.ErrorContext(g.Request.Context(), "Error parsing UserId", "err", err)
		g.Status(http.StatusInternalServerError)
		return
	}

	userLogin, err := getUserLogin(g)
	if err != nil {
		mc.lgr.ErrorContext(g.Request.Context(), "Error parsing userLogin", "err", err)
		g.Status(http.StatusInternalServerError)
		return
	}

	cid := g.Param(dto.ChatIdParam)
	chatId, err := utils.ParseInt64(cid)
	if err != nil {
		mc.lgr.ErrorContext(g.Request.Context(), "Error binding chatId", "err", err)
		g.Status(http.StatusInternalServerError)
		return
	}

	participant, err := mc.commonProjection.IsParticipant(g.Request.Context(), mc.dbWrapper, userId, chatId)
	if err != nil {
		mc.lgr.ErrorContext(g.Request.Context(), "Error checking is participant", "err", err)
		g.Status(http.StatusInternalServerError)
		return
	}
	if !participant {
		mc.lgr.InfoContext(g.Request.Context(), fmt.Sprintf("User %v is not participant of chat %v, skipping", userId, chatId))
		g.Status(http.StatusOK)
		return
	}

	d := new(dto.BroadcastDto)

	err = g.Bind(d)
	if err != nil {
		mc.lgr.ErrorContext(g.Request.Context(), "Error binding BroadcastDto", "err", err)
		g.Status(http.StatusInternalServerError)
		return
	}

	mc.asyncMessageService.BroadcastMessage(g.Request.Context(), d.Text, chatId, userId, userLogin)

	g.Status(http.StatusOK)
	return
}

func (mc *MessageHandler) PinPromoted(g *gin.Context) {
	g.Status(http.StatusOK) // TODO implement pinned
	return
}

func (mc *MessageHandler) MessagesFresh(g *gin.Context) {
	g.JSON(http.StatusOK, dto.FreshDto{ // TODO implement fresh
		Ok: true,
	})
	return
}

func (mc *MessageHandler) MessagesFilter(g *gin.Context) {
	g.JSON(http.StatusOK, dto.FilterDto{ // TODO implement filter
		Found: true,
	})
	return
}

func (mc *MessageHandler) MakeBlogPost(g *gin.Context) {
	userId, err := getUserId(g)
	if err != nil {
		mc.lgr.ErrorContext(g.Request.Context(), "Error parsing UserId", "err", err)
		g.Status(http.StatusInternalServerError)
		return
	}

	cid := g.Param(dto.ChatIdParam)
	chatId, err := utils.ParseInt64(cid)
	if err != nil {
		mc.lgr.ErrorContext(g.Request.Context(), "Error binding chatId", "err", err)
		g.Status(http.StatusInternalServerError)
		return
	}

	mid := g.Param(dto.MessageIdParam)

	messageId, err := utils.ParseInt64(mid)
	if err != nil {
		mc.lgr.ErrorContext(g.Request.Context(), "Error binding messageId", "err", err)
		g.Status(http.StatusInternalServerError)
		return
	}

	mr := cqrs.MakeMessageBlogPost{
		AdditionalData: cqrs.GenerateMessageAdditionalData(getCorrelationId(g), userId),
		ChatId:         chatId,
		MessageId:      messageId,
		BlogPost:       true,
	}

	err = mr.Handle(g.Request.Context(), mc.eventBus)
	if err != nil {
		if translateMessageError(g, err) {
			return
		}

		mc.lgr.ErrorContext(g.Request.Context(), "Error sending MakeMessageBlogPost command", "err", err)
		g.Status(http.StatusInternalServerError)
		return
	}

	g.Status(http.StatusOK)
}

func (mc *MessageHandler) SearchMessages(g *gin.Context) {
	userId, err := getUserId(g)
	if err != nil {
		mc.lgr.ErrorContext(g.Request.Context(), "Error parsing UserId", "err", err)
		g.Status(http.StatusInternalServerError)
		return
	}

	cid := g.Param(dto.ChatIdParam)

	chatId, err := utils.ParseInt64(cid)
	if err != nil {
		mc.lgr.ErrorContext(g.Request.Context(), "Error binding chatId", "err", err)
		g.Status(http.StatusInternalServerError)
		return
	}

	size := utils.FixSizeString(g.Query(dto.SizeParam))
	reverse := utils.GetBoolean(g.Query(dto.ReverseParam))
	startingFromItemIdString := g.Query(dto.StartingFromItemId)
	var startingFromItemId *int64
	if startingFromItemIdString != "" {
		startingFromItemId2, err := utils.ParseInt64(startingFromItemIdString) // exclusive
		if err != nil {
			mc.lgr.ErrorContext(g.Request.Context(), "Error parsing startingFromItemId", "err", err)
			g.Status(http.StatusInternalServerError)
			return
		}
		startingFromItemId = &startingFromItemId2
	}
	includeStartingFrom := utils.GetBoolean(g.Query(dto.IncludeStartingFromParam))
	searchString := g.Query(dto.SearchStringParam)

	messages, err := mc.enrichingProjection.GetMessagesEnriched(g.Request.Context(), []int64{userId}, true, &userId, chatId, size, startingFromItemId, includeStartingFrom, reverse, searchString, nil)
	if err != nil {
		if translateMessageError(g, err) {
			return
		}

		mc.lgr.ErrorContext(g.Request.Context(), "Error getting messages", "err", err)
		g.Status(http.StatusInternalServerError)
		return
	}

	g.JSON(http.StatusOK, dto.MessagesResponseDto{
		Items:   messages,
		HasNext: int32(len(messages)) == size,
	})
}

// returns should exit
func translateMessageError(g *gin.Context, err error) bool {
	if err == nil {
		return false
	}
	var mediaError *sanitizer.MediaUrlErr
	var mediaOverflowError *sanitizer.MediaOverflowErr
	var validationError *cqrs.ValidationError
	var chatStillNotExistsError *cqrs.ChatStillNotExistsError
	var unauthError *cqrs.UnauthorizedError
	if errors.As(err, &mediaError) {
		g.JSON(http.StatusBadRequest, &utils.H{"message": mediaError.Error(), "businessErrorCode": badMediaUrl})
		return true
	} else if errors.As(err, &mediaOverflowError) {
		g.JSON(http.StatusBadRequest, &dto.ErrorMessageDto{mediaOverflowError.Error()})
		return true
	} else if errors.As(err, &validationError) {
		g.JSON(http.StatusBadRequest, &dto.ErrorMessageDto{validationError.Error()})
		return true
	} else if errors.As(err, &chatStillNotExistsError) {
		g.Status(http.StatusTeapot)
		return true
	} else if errors.As(err, &unauthError) {
		g.JSON(http.StatusUnauthorized, &dto.ErrorMessageDto{unauthError.Error()})
		return true
	}
	return false
}
