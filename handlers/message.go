package handlers

import (
	"errors"
	"go-cqrs-chat-example/config"
	"go-cqrs-chat-example/cqrs"
	"go-cqrs-chat-example/db"
	"go-cqrs-chat-example/dto"
	"go-cqrs-chat-example/logger"
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
	policy              *services.SanitizerPolicy
	cfg                 *config.AppConfig
	enrichingProjection *cqrs.EnrichingProjection
}

func NewMessageHandler(
	lgr *logger.LoggerWrapper,
	eventBus *cqrs.PartitionAwareEventBus,
	dbWrapper *db.DB,
	commonProjection *cqrs.CommonProjection,
	policy *services.SanitizerPolicy,
	cfg *config.AppConfig,
	enrichingProjection *cqrs.EnrichingProjection,
) *MessageHandler {
	return &MessageHandler{
		lgr:                 lgr,
		eventBus:            eventBus,
		dbWrapper:           dbWrapper,
		commonProjection:    commonProjection,
		policy:              policy,
		cfg:                 cfg,
		enrichingProjection: enrichingProjection,
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
		AdditionalData: cqrs.GenerateMessageAdditionalData(getCorrelationId(g)),
		ChatId:         chatId,
		Content:        mcd.Content,
		OwnerId:        userId,
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
		AdditionalData: cqrs.GenerateMessageAdditionalData(getCorrelationId(g)),
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

	err = cc.Handle(g.Request.Context(), mc.eventBus, mc.dbWrapper, mc.commonProjection, userId, mc.cfg, mc.lgr, mc.policy)
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
		AdditionalData: cqrs.GenerateMessageAdditionalData(getCorrelationId(g)),
		MessageId:      messageId,
		ChatId:         chatId,
	}

	err = cc.Handle(g.Request.Context(), mc.eventBus, mc.dbWrapper, mc.commonProjection, userId)
	if err != nil {
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
		AdditionalData: cqrs.GenerateMessageAdditionalData(getCorrelationId(g)),
		ChatId:         chatId,
		MessageId:      messageId,
		ParticipantId:  userId,
		ReadAll:        false,
	}

	err = mr.Handle(g.Request.Context(), mc.eventBus, mc.commonProjection, mc.dbWrapper)
	if err != nil {
		mc.lgr.ErrorContext(g.Request.Context(), "Error sending MessageRead command", "err", err)
		g.Status(http.StatusInternalServerError)
		return
	}

	g.Status(http.StatusOK)
}

func (mc *MessageHandler) MarkAsReadAll(g *gin.Context) {

	userId, err := getUserId(g)
	if err != nil {
		mc.lgr.ErrorContext(g.Request.Context(), "Error parsing UserId", "err", err)
		g.Status(http.StatusInternalServerError)
		return
	}

	mr := cqrs.MessageRead{
		AdditionalData: cqrs.GenerateMessageAdditionalData(getCorrelationId(g)),
		ParticipantId:  userId,
		ReadAll:        true,
	}

	err = mr.Handle(g.Request.Context(), mc.eventBus, mc.commonProjection, mc.dbWrapper)
	if err != nil {
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

	mr := cqrs.ReactOnMessage{
		AdditionalData: cqrs.GenerateMessageAdditionalData(getCorrelationId(g)),
		ChatId:         chatId,
		MessageId:      messageId,
		BehalfUserId:   userId,
		Reaction:       ccd.Reaction,
	}

	err = mr.Handle(g.Request.Context(), mc.eventBus, mc.dbWrapper, mc.commonProjection)
	if err != nil {
		mc.lgr.ErrorContext(g.Request.Context(), "Error sending ReactOnMessage command", "err", err)
		g.Status(http.StatusInternalServerError)
		return
	}

	g.Status(http.StatusOK)
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
		AdditionalData: cqrs.GenerateMessageAdditionalData(getCorrelationId(g)),
		ChatId:         chatId,
		MessageId:      messageId,
		BlogPost:       true,
		BehalfUserId:   userId,
	}

	err = mr.Handle(g.Request.Context(), mc.eventBus)
	if err != nil {
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

	messages, err := mc.enrichingProjection.GetMessagesEnriched(g.Request.Context(), userId, chatId, size, startingFromItemId, includeStartingFrom, reverse)
	if err != nil {
		mc.lgr.ErrorContext(g.Request.Context(), "Error getting messages", "err", err)
		g.Status(http.StatusInternalServerError)
		return
	}

	g.JSON(http.StatusOK, messages)
}

// returns should exit
func translateMessageError(g *gin.Context, err error) bool {
	if err == nil {
		return false
	}
	var mediaError *cqrs.MediaUrlErr
	var mediaOverflowError *cqrs.MediaOverflowErr
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
