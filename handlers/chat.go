package handlers

import (
	"errors"
	"go-cqrs-chat-example/cqrs"
	"go-cqrs-chat-example/db"
	"go-cqrs-chat-example/dto"
	"go-cqrs-chat-example/logger"
	"go-cqrs-chat-example/services"
	"go-cqrs-chat-example/utils"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
)

type ChatHandler struct {
	lgr                 *logger.LoggerWrapper
	eventBus            *cqrs.PartitionAwareEventBus
	dbWrapper           *db.DB
	commonProjection    *cqrs.CommonProjection
	stripTagsPolicy     *services.StripTagsPolicy
	enrichingProjection *cqrs.EnrichingProjection
}

func NewChatHandler(
	lgr *logger.LoggerWrapper,
	eventBus *cqrs.PartitionAwareEventBus,
	dbWrapper *db.DB,
	commonProjection *cqrs.CommonProjection,
	stripTagsPolicy *services.StripTagsPolicy,
	enrichingProjection *cqrs.EnrichingProjection,
) *ChatHandler {
	return &ChatHandler{
		lgr:                 lgr,
		eventBus:            eventBus,
		dbWrapper:           dbWrapper,
		commonProjection:    commonProjection,
		stripTagsPolicy:     stripTagsPolicy,
		enrichingProjection: enrichingProjection,
	}
}

func (ch *ChatHandler) CreateChat(g *gin.Context) {
	ccd := new(dto.ChatCreateDto)

	err := g.Bind(ccd)
	if err != nil {
		ch.lgr.ErrorContext(g.Request.Context(), "Error binding ChatCreateDto", "err", err)
		g.Status(http.StatusInternalServerError)
		return
	}

	userId, err := getUserId(g)
	if err != nil {
		ch.lgr.ErrorContext(g.Request.Context(), "Error parsing UserId", "err", err)
		g.Status(http.StatusInternalServerError)
		return
	}

	cc := cqrs.ChatCreate{
		AdditionalData: cqrs.GenerateMessageAdditionalData(),
		Title:          ccd.Title,
		ParticipantIds: ccd.ParticipantIds,
	}

	chatId, err := cc.Handle(g.Request.Context(), userId, ch.eventBus, ch.dbWrapper, ch.commonProjection, ch.stripTagsPolicy)
	if err != nil {
		if translateChatError(g, err) {
			return
		}

		ch.lgr.ErrorContext(g.Request.Context(), "Error sending ChatCreate command", "err", err)
		g.Status(http.StatusInternalServerError)
		return
	}

	ch.lgr.InfoContext(g.Request.Context(), "created the chat", "chat_id", chatId)

	m := dto.IdResponse{Id: chatId}

	g.JSON(http.StatusOK, m)
}

func (ch *ChatHandler) EditChat(g *gin.Context) {

	userId, err := getUserId(g)
	if err != nil {
		ch.lgr.ErrorContext(g.Request.Context(), "Error parsing UserId", "err", err)
		g.Status(http.StatusInternalServerError)
		return
	}

	ccd := new(dto.ChatEditDto)

	err = g.Bind(ccd)
	if err != nil {
		ch.lgr.ErrorContext(g.Request.Context(), "Error binding ChatEditDto", "err", err)
		g.Status(http.StatusInternalServerError)
		return
	}

	cc := cqrs.ChatEdit{
		AdditionalData:      cqrs.GenerateMessageAdditionalData(),
		ChatId:              ccd.Id,
		Title:               ccd.Title,
		ParticipantIdsToAdd: ccd.ParticipantIds,
		Blog:                ccd.Blog,
		BehalfUserId:        userId,
	}

	err = cc.Handle(g.Request.Context(), ch.eventBus, ch.dbWrapper, ch.commonProjection, ch.stripTagsPolicy)
	if err != nil {
		if translateChatError(g, err) {
			return
		}

		ch.lgr.ErrorContext(g.Request.Context(), "Error sending ChatEdit command", "err", err)
		g.Status(http.StatusInternalServerError)
		return
	}

	g.Status(http.StatusOK)
}

func (ch *ChatHandler) DeleteChat(g *gin.Context) {
	userId, err := getUserId(g)
	if err != nil {
		ch.lgr.ErrorContext(g.Request.Context(), "Error parsing UserId", "err", err)
		g.Status(http.StatusInternalServerError)
		return
	}

	cid := g.Param(dto.ChatIdParam)

	chatId, err := utils.ParseInt64(cid)
	if err != nil {
		ch.lgr.ErrorContext(g.Request.Context(), "Error binding chatId", "err", err)
		g.Status(http.StatusInternalServerError)
		return
	}

	cc := cqrs.ChatDelete{
		AdditionalData: cqrs.GenerateMessageAdditionalData(),
		ChatId:         chatId,
		BehalfUserId:   userId,
	}

	err = cc.Handle(g.Request.Context(), ch.eventBus, ch.dbWrapper, ch.commonProjection)
	if err != nil {
		if translateChatError(g, err) {
			return
		}

		ch.lgr.ErrorContext(g.Request.Context(), "Error sending ChatDelete command", "err", err)
		g.Status(http.StatusInternalServerError)
		return
	}

	g.Status(http.StatusOK)
}

func (ch *ChatHandler) PinChat(g *gin.Context) {
	cid := g.Param(dto.ChatIdParam)

	chatId, err := utils.ParseInt64(cid)
	if err != nil {
		ch.lgr.ErrorContext(g.Request.Context(), "Error binding chatId", "err", err)
		g.Status(http.StatusInternalServerError)
		return
	}

	p := g.Query(dto.PinParam)

	pin := utils.GetBoolean(p)

	userId, err := getUserId(g)
	if err != nil {
		ch.lgr.ErrorContext(g.Request.Context(), "Error parsing UserId", "err", err)
		g.Status(http.StatusInternalServerError)
		return
	}

	cc := cqrs.ChatPin{
		AdditionalData: cqrs.GenerateMessageAdditionalData(),
		ChatId:         chatId,
		Pin:            pin,
		ParticipantId:  userId,
	}

	err = cc.Handle(g.Request.Context(), ch.eventBus)
	if err != nil {
		ch.lgr.ErrorContext(g.Request.Context(), "Error sending ChatPin command", "err", err)
		g.Status(http.StatusInternalServerError)
		return
	}

	g.Status(http.StatusOK)
}

func (ch *ChatHandler) SearchChats(g *gin.Context) {
	userId, err := getUserId(g)
	if err != nil {
		ch.lgr.ErrorContext(g.Request.Context(), "Error parsing UserId", "err", err)
		g.Status(http.StatusInternalServerError)
		return
	}

	size := utils.FixSizeString(g.Query(dto.SizeParam))
	reverse := utils.GetBoolean(g.Query(dto.ReverseParam))

	pinned := utils.GetBooleanNullable(g.Query(dto.PinnedParam))
	lastUpdateDateTime := utils.GetTimeNullable(g.Query(dto.LastUpdateDateTimeParam))
	id := utils.ParseInt64Nullable(g.Query(dto.ChatIdParam))
	startingFromItemId := ch.convertChatId(pinned, lastUpdateDateTime, id)

	includeStartingFrom := utils.GetBoolean(g.Query(dto.IncludeStartingFromParam))

	chats, err := ch.enrichingProjection.GetChatsEnriched(g.Request.Context(), userId, size, startingFromItemId, includeStartingFrom, reverse)
	if err != nil {
		ch.lgr.ErrorContext(g.Request.Context(), "Error getting chats", "err", err)
		g.Status(http.StatusInternalServerError)
		return
	}

	g.JSON(http.StatusOK, chats)
}

func (ch *ChatHandler) convertChatId(pinned *bool, lastUpdateDateTime *time.Time, id *int64) *dto.ChatId {
	if pinned == nil || lastUpdateDateTime == nil || id == nil {
		return nil
	}
	return &dto.ChatId{
		Pinned:             *pinned,
		LastUpdateDateTime: *lastUpdateDateTime,
		Id:                 *id,
	}
}

// returns should exit
func translateChatError(g *gin.Context, err error) bool {
	if err == nil {
		return false
	}
	var validationError *cqrs.ValidationError
	var unauthError *cqrs.UnauthorizedError
	if errors.As(err, &validationError) {
		g.JSON(http.StatusBadRequest, &utils.H{"message": validationError.Error()})
		return true
	} else if errors.As(err, &unauthError) {
		g.JSON(http.StatusUnauthorized, dto.ErrorMessageDto{unauthError.Error()})
		return true
	}
	return false
}
