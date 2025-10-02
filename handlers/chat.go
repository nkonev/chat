package handlers

import (
	"errors"
	"fmt"
	"go-cqrs-chat-example/config"
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
	cfg                 *config.AppConfig
}

func NewChatHandler(
	lgr *logger.LoggerWrapper,
	eventBus *cqrs.PartitionAwareEventBus,
	dbWrapper *db.DB,
	commonProjection *cqrs.CommonProjection,
	stripTagsPolicy *services.StripTagsPolicy,
	enrichingProjection *cqrs.EnrichingProjection,
	cfg *config.AppConfig,
) *ChatHandler {
	return &ChatHandler{
		lgr:                 lgr,
		eventBus:            eventBus,
		dbWrapper:           dbWrapper,
		commonProjection:    commonProjection,
		stripTagsPolicy:     stripTagsPolicy,
		enrichingProjection: enrichingProjection,
		cfg:                 cfg,
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
		AdditionalData: cqrs.GenerateMessageAdditionalData(getCorrelationId(g), userId),
		Title:          ccd.Title,
		ParticipantIds: ccd.ParticipantIds,
		CanResend:      ccd.CanResend,
		Blog:           ccd.Blog,
		Avatar:         ccd.Avatar,
		AvatarBig:      ccd.AvatarBig,
	}

	chatId, err := cc.Handle(g.Request.Context(), ch.eventBus, ch.dbWrapper, ch.commonProjection, ch.stripTagsPolicy, ch.cfg)
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

func (ch *ChatHandler) CreateTetAChat(g *gin.Context) {

	oppositeUserId, err := utils.ParseInt64(g.Param(dto.ParticipantIdParam))
	if err != nil {
		ch.lgr.ErrorContext(g.Request.Context(), "Error binding participantId", "err", err)
		g.Status(http.StatusInternalServerError)
		return
	}

	userId, err := getUserId(g)
	if err != nil {
		ch.lgr.ErrorContext(g.Request.Context(), "Error parsing UserId", "err", err)
		g.Status(http.StatusInternalServerError)
		return
	}

	tetATetChatName := fmt.Sprintf("tet_a_tet_%v_%v", userId, oppositeUserId)

	cc := cqrs.ChatCreate{
		AdditionalData: cqrs.GenerateMessageAdditionalData(getCorrelationId(g), userId),
		Title:          tetATetChatName,
		ParticipantIds: []int64{oppositeUserId},
		TetATet:        true,
	}

	chatId, err := cc.Handle(g.Request.Context(), ch.eventBus, ch.dbWrapper, ch.commonProjection, ch.stripTagsPolicy, ch.cfg)
	if err != nil {
		if translateChatError(g, err) {
			return
		}

		ch.lgr.ErrorContext(g.Request.Context(), "Error sending ChatCreate command", "err", err)
		g.Status(http.StatusInternalServerError)
		return
	}

	ch.lgr.InfoContext(g.Request.Context(), "created the tet-a-tet chat", "chat_id", chatId)

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
		AdditionalData:      cqrs.GenerateMessageAdditionalData(getCorrelationId(g), userId),
		ChatId:              ccd.Id,
		Title:               ccd.Title,
		ParticipantIdsToAdd: ccd.ParticipantIds,
		Blog:                ccd.Blog,
		CanResend:           ccd.CanResend,
		Avatar:              ccd.Avatar,
		AvatarBig:           ccd.AvatarBig,
	}

	err = cc.Handle(g.Request.Context(), ch.eventBus, ch.dbWrapper, ch.commonProjection, ch.stripTagsPolicy, ch.cfg)
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
		AdditionalData: cqrs.GenerateMessageAdditionalData(getCorrelationId(g), userId),
		ChatId:         chatId,
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
		AdditionalData: cqrs.GenerateMessageAdditionalData(getCorrelationId(g), userId),
		ChatId:         chatId,
		Pin:            pin,
	}

	err = cc.Handle(g.Request.Context(), ch.eventBus)
	if err != nil {
		if translateChatError(g, err) {
			return
		}

		ch.lgr.ErrorContext(g.Request.Context(), "Error sending ChatPin command", "err", err)
		g.Status(http.StatusInternalServerError)
		return
	}

	g.Status(http.StatusOK)
}

func (ch *ChatHandler) PutUserChatNotificationSettings(g *gin.Context) {
	cid := g.Param(dto.ChatIdParam)

	chatId, err := utils.ParseInt64(cid)
	if err != nil {
		ch.lgr.ErrorContext(g.Request.Context(), "Error binding chatId", "err", err)
		g.Status(http.StatusInternalServerError)
		return
	}

	req := dto.PutChatNotificationSettingsDto{}
	err = g.Bind(&req)
	if err != nil {
		ch.lgr.ErrorContext(g.Request.Context(), "Error binding considerMessagesOfThisChatAsUnread", "err", err)
		g.Status(http.StatusInternalServerError)
		return
	}

	userId, err := getUserId(g)
	if err != nil {
		ch.lgr.ErrorContext(g.Request.Context(), "Error parsing UserId", "err", err)
		g.Status(http.StatusInternalServerError)
		return
	}

	cc := cqrs.ChatNotificationSettingsSet{
		AdditionalData: cqrs.GenerateMessageAdditionalData(getCorrelationId(g), userId),
		ChatId:         chatId,
		Set:            req.ConsiderMessagesOfThisChatAsUnread,
	}

	err = cc.Handle(g.Request.Context(), ch.eventBus)
	if err != nil {
		if translateChatError(g, err) {
			return
		}

		ch.lgr.ErrorContext(g.Request.Context(), "Error sending ChatPin command", "err", err)
		g.Status(http.StatusInternalServerError)
		return
	}

	g.Status(http.StatusOK)
}

func (ch *ChatHandler) GetUserChatNotificationSettings(g *gin.Context) {
	cid := g.Param(dto.ChatIdParam)

	chatId, err := utils.ParseInt64(cid)
	if err != nil {
		ch.lgr.ErrorContext(g.Request.Context(), "Error binding chatId", "err", err)
		g.Status(http.StatusInternalServerError)
		return
	}

	userId, err := getUserId(g)
	if err != nil {
		ch.lgr.ErrorContext(g.Request.Context(), "Error parsing UserId", "err", err)
		g.Status(http.StatusInternalServerError)
		return
	}

	cns, err := ch.commonProjection.GetChatNotificationSettings(g.Request.Context(), userId, chatId)
	if err != nil {
		ch.lgr.ErrorContext(g.Request.Context(), "Error getting chat notification settings", "err", err)
		g.Status(http.StatusInternalServerError)
		return
	}

	g.JSON(http.StatusOK, cns)
}

func (ch *ChatHandler) HasNewMessages(g *gin.Context) {
	userId, err := getUserId(g)
	if err != nil {
		ch.lgr.ErrorContext(g.Request.Context(), "Error parsing UserId", "err", err)
		g.Status(http.StatusInternalServerError)
		return
	}

	has, err := ch.commonProjection.GetHasUnreadMessages(g.Request.Context(), []int64{userId})
	if err != nil {
		ch.lgr.ErrorContext(g.Request.Context(), "Error getting HasNewMessages", "err", err)
		g.Status(http.StatusInternalServerError)
		return
	}

	g.JSON(http.StatusOK, &dto.HasUnreadMessages{
		HasUnreadMessages: has[userId],
	})
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

	searchString := g.Query(dto.SearchStringParam)

	chats, _, err := ch.enrichingProjection.GetChatsEnriched(g.Request.Context(), []int64{userId}, size, startingFromItemId, includeStartingFrom, reverse, searchString, nil)
	if err != nil {
		if translateChatError(g, err) {
			return
		}

		ch.lgr.ErrorContext(g.Request.Context(), "Error getting chats", "err", err)
		g.Status(http.StatusInternalServerError)
		return
	}

	g.JSON(http.StatusOK, dto.GetChatsResponseDto{
		Items:   chats,
		HasNext: int32(len(chats)) == size,
	})
}

func (ch *ChatHandler) GetChat(g *gin.Context) {
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

	size := int32(1)
	reverse := false

	var startingFromItemId *dto.ChatId = nil
	includeStartingFrom := true
	searchString := ""

	chats, _, err := ch.enrichingProjection.GetChatsEnriched(g.Request.Context(), []int64{userId}, size, startingFromItemId, includeStartingFrom, reverse, searchString, &chatId)
	if err != nil {
		if translateChatError(g, err) {
			return
		}

		ch.lgr.ErrorContext(g.Request.Context(), "Error getting chats", "err", err)
		g.Status(http.StatusInternalServerError)
		return
	}

	if len(chats) == 0 {
		g.Status(http.StatusNoContent)
		return
	} else if len(chats) > 1 {
		ch.lgr.ErrorContext(g.Request.Context(), "Wrong invariant - More than 1 chats got")
		g.Status(http.StatusInternalServerError)
		return
	}

	chat := chats[0]

	g.JSON(http.StatusOK, chat)
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
	var chatStillNotExistsError *cqrs.ChatStillNotExistsError
	if errors.As(err, &validationError) {
		g.JSON(http.StatusBadRequest, &dto.ErrorMessageDto{validationError.Error()})
		return true
	} else if errors.As(err, &unauthError) {
		g.JSON(http.StatusUnauthorized, &dto.ErrorMessageDto{unauthError.Error()})
		return true
	} else if errors.As(err, &chatStillNotExistsError) {
		g.Status(http.StatusTeapot)
		return true
	}
	return false
}
