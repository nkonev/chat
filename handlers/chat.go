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
	"strings"
	"time"

	"github.com/gin-gonic/gin"
)

type ChatHandler struct {
	lgr                 *logger.LoggerWrapper
	eventBus            *cqrs.PartitionAwareEventBus
	dbWrapper           *db.DB
	commonProjection    *cqrs.CommonProjection
	stripTagsPolicy     *sanitizer.StripTagsPolicy
	enrichingProjection *cqrs.EnrichingProjection
	cfg                 *config.AppConfig
	authorizeService    *services.AuthorizationService
}

func NewChatHandler(
	lgr *logger.LoggerWrapper,
	eventBus *cqrs.PartitionAwareEventBus,
	dbWrapper *db.DB,
	commonProjection *cqrs.CommonProjection,
	stripTagsPolicy *sanitizer.StripTagsPolicy,
	enrichingProjection *cqrs.EnrichingProjection,
	cfg *config.AppConfig,
	authorizeService *services.AuthorizationService,
) *ChatHandler {
	return &ChatHandler{
		lgr:                 lgr,
		eventBus:            eventBus,
		dbWrapper:           dbWrapper,
		commonProjection:    commonProjection,
		stripTagsPolicy:     stripTagsPolicy,
		enrichingProjection: enrichingProjection,
		cfg:                 cfg,
		authorizeService:    authorizeService,
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
		AdditionalData:                      cqrs.GenerateMessageAdditionalData(getCorrelationId(g), userId),
		Title:                               ccd.Title,
		ParticipantIds:                      ccd.ParticipantIds,
		TetATet:                             false,
		Blog:                                ccd.Blog,
		BlogAbout:                           ccd.BlogAbout,
		Avatar:                              ccd.Avatar,
		AvatarBig:                           ccd.AvatarBig,
		CanResend:                           utils.GetNullableBooleanOr(ccd.CanResend, dto.DefaultCanResend),
		CanReact:                            utils.GetNullableBooleanOr(ccd.CanReact, dto.DefaultCanReact),
		AvailableToSearch:                   utils.GetNullableBooleanOr(ccd.AvailableToSearch, dto.DefaultAvailableToSearch),
		RegularParticipantCanPublishMessage: utils.GetNullableBooleanOr(ccd.RegularParticipantCanPublishMessage, dto.DefaultRegularParticipantCanPublishMessage),
		RegularParticipantCanPinMessage:     utils.GetNullableBooleanOr(ccd.RegularParticipantCanPinMessage, dto.DefaultRegularParticipantCanPinMessage),
		RegularParticipantCanWriteMessage:   utils.GetNullableBooleanOr(ccd.RegularParticipantCanWriteMessage, dto.DefaultRegularParticipantCanWriteMessage),
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
		Blog:           false,
		CanResend:      ch.cfg.Chat.TetATet.CanResend,
		CanReact:       ch.cfg.Chat.TetATet.CanReact,
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
		AdditionalData:                      cqrs.GenerateMessageAdditionalData(getCorrelationId(g), userId),
		ChatId:                              ccd.Id,
		Title:                               ccd.Title,
		ParticipantIdsToAdd:                 ccd.ParticipantIds,
		Blog:                                ccd.Blog,
		BlogAbout:                           ccd.BlogAbout,
		Avatar:                              ccd.Avatar,
		AvatarBig:                           ccd.AvatarBig,
		CanResend:                           utils.GetNullableBooleanOr(ccd.CanResend, dto.DefaultCanResend),
		CanReact:                            utils.GetNullableBooleanOr(ccd.CanReact, dto.DefaultCanReact),
		AvailableToSearch:                   utils.GetNullableBooleanOr(ccd.AvailableToSearch, dto.DefaultAvailableToSearch),
		RegularParticipantCanPublishMessage: utils.GetNullableBooleanOr(ccd.RegularParticipantCanPublishMessage, dto.DefaultRegularParticipantCanPublishMessage),
		RegularParticipantCanPinMessage:     utils.GetNullableBooleanOr(ccd.RegularParticipantCanPinMessage, dto.DefaultRegularParticipantCanPinMessage),
		RegularParticipantCanWriteMessage:   utils.GetNullableBooleanOr(ccd.RegularParticipantCanWriteMessage, dto.DefaultRegularParticipantCanWriteMessage),
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

	chats, _, err := ch.enrichingProjection.GetChatsEnriched(g.Request.Context(), []int64{userId}, size, startingFromItemId, includeStartingFrom, reverse, searchString, nil, false)
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

	chat, shouldJoin, err := ch.enrichingProjection.GetChat(g.Request.Context(), userId, chatId)
	if err != nil {
		if translateChatError(g, err) {
			return
		}

		ch.lgr.ErrorContext(g.Request.Context(), "Error getting chats", "err", err)
		g.Status(http.StatusInternalServerError)
		return
	}

	if shouldJoin {
		g.Status(http.StatusResetContent)
		return
	}

	if chat != nil {
		g.JSON(http.StatusOK, chat)
		return
	} else {
		g.Status(http.StatusNoContent)
		return
	}
}

func (ch *ChatHandler) ChatsFresh(g *gin.Context) {
	userId, err := getUserId(g)
	if err != nil {
		ch.lgr.ErrorContext(g.Request.Context(), "Error parsing UserId", "err", err)
		g.Status(http.StatusInternalServerError)
		return
	}

	size := utils.FixSizeString(g.Query(dto.SizeParam))
	reverse := false

	var startingFromItemId *dto.ChatId = nil

	includeStartingFrom := false

	searchString := g.Query(dto.SearchStringParam)

	var bindTo = make([]*dto.ChatViewEnrichedDto, 0)
	if err = g.Bind(&bindTo); err != nil {
		ch.lgr.ErrorContext(g.Request.Context(), "Error during binding to dto", "err", err)
		g.Status(http.StatusInternalServerError)
		return
	}

	chatDtos, _, err := ch.enrichingProjection.GetChatsEnriched(g.Request.Context(), []int64{userId}, size, startingFromItemId, includeStartingFrom, reverse, searchString, nil, false)
	if err != nil {
		if translateChatError(g, err) {
			return
		}

		ch.lgr.ErrorContext(g.Request.Context(), "Error getting chats", "err", err)
		g.Status(http.StatusInternalServerError)
		return
	}

	edge := true

	aLen := min(len(chatDtos), len(bindTo))
	if len(bindTo) == 0 && len(chatDtos) != 0 {
		edge = false
	}

	for i := range aLen {
		currentChat := chatDtos[i]
		gottenChat := bindTo[i]
		if currentChat.Id != gottenChat.Id {
			edge = false
			break
		}
		if currentChat.Title != gottenChat.Title {
			edge = false
			break
		}
		if currentChat.UnreadMessages != gottenChat.UnreadMessages {
			edge = false
			break
		}
		if !utils.ComparePointers(currentChat.UpdateDateTime, gottenChat.UpdateDateTime) {
			edge = false
			break
		}
		if !utils.ComparePointers(currentChat.LastMessagePreview, gottenChat.LastMessagePreview) {
			edge = false
			break
		}
		if !utils.ComparePointers(currentChat.Avatar, gottenChat.Avatar) {
			edge = false
			break
		}
		if !utils.ComparePointers(currentChat.LoginColor, gottenChat.LoginColor) {
			edge = false
			break
		}
		if currentChat.TetATet != gottenChat.TetATet {
			edge = false
			break
		}
		if currentChat.Blog != gottenChat.Blog {
			edge = false
			break
		}

		if currentChat.ParticipantsCount != gottenChat.ParticipantsCount {
			edge = false
			break
		}
	}

	g.JSON(http.StatusOK, dto.FreshDto{
		Ok: edge,
	})
	return
}

func (ch *ChatHandler) ChatsFilter(g *gin.Context) {
	userId, err := getUserId(g)
	if err != nil {
		ch.lgr.ErrorContext(g.Request.Context(), "Error parsing UserId", "err", err)
		g.Status(http.StatusInternalServerError)
		return
	}

	d := new(dto.ChatFilterDto)
	err = g.Bind(d)
	if err != nil {
		ch.lgr.ErrorContext(g.Request.Context(), "Error binding MessageFilterDto", "err", err)
		g.Status(http.StatusInternalServerError)
		return
	}

	searchString := d.SearchString
	chatId := d.ChatId

	found, err := ch.enrichingProjection.ChatFilter(g.Request.Context(), ch.dbWrapper, userId, chatId, searchString)
	if err != nil {
		if translateChatError(g, err) {
			return
		}

		ch.lgr.ErrorContext(g.Request.Context(), "Error invoking MessageFilter", "err", err)
		g.Status(http.StatusInternalServerError)
		return
	}

	g.JSON(http.StatusOK, dto.FilterDto{
		Found: found,
	})
	return
}

func (ch *ChatHandler) CheckAccess(g *gin.Context) {
	m := map[string]string{}
	err := g.BindQuery(&m)
	if err != nil {
		ch.lgr.ErrorContext(g.Request.Context(), "Error binding query", "err", err)
		g.Status(http.StatusInternalServerError)
		return
	}

	g.Status(ch.authorizeService.CheckAccess(g.Request.Context(), m))
	return
}

func (ch *ChatHandler) IsAdmin(g *gin.Context) {
	chatId, err := utils.ParseInt64(g.Query("chatId"))
	if err != nil {
		ch.lgr.ErrorContext(g.Request.Context(), "Error checking access", "err", err)
		g.Status(http.StatusInternalServerError)
		return
	}
	userId, err := utils.ParseInt64(g.Query("userId"))
	if err != nil {
		ch.lgr.ErrorContext(g.Request.Context(), "Error checking access", "err", err)
		g.Status(http.StatusInternalServerError)
		return
	}
	isAdmin, err := ch.commonProjection.IsChatAdmin(g.Request.Context(), ch.dbWrapper, userId, chatId)
	if err != nil {
		ch.lgr.ErrorContext(g.Request.Context(), "Error checking access", "err", err)
		g.Status(http.StatusInternalServerError)
		return
	}
	if isAdmin {
		g.Status(http.StatusOK)
		return
	} else {
		g.Status(http.StatusUnauthorized)
		return
	}
}

func (ch *ChatHandler) IsExists(g *gin.Context) {
	chatIdsStr := g.Query("chatId")
	chatIds := []int64{}
	chatIdsStrs := strings.Split(chatIdsStr, ",")
	for _, paramString := range chatIdsStrs {
		param, err := utils.ParseInt64(paramString)
		if err != nil {
			ch.lgr.ErrorContext(g.Request.Context(), "Error getting chatId slice", "err", err)
			g.Status(http.StatusInternalServerError)
			return
		}
		chatIds = append(chatIds, param)
	}

	responseList := make([]dto.ChatExists, 0)
	if len(chatIds) == 0 {
		g.JSON(http.StatusOK, responseList)
		return
	}

	existsFromDb, err := ch.commonProjection.GetExistingChatIds(g.Request.Context(), ch.dbWrapper, chatIds)
	if err != nil {
		ch.lgr.ErrorContext(g.Request.Context(), "Error getting chatId slice", "err", err)
		g.Status(http.StatusInternalServerError)
		return
	}

	for _, ce := range chatIds {
		responseList = append(responseList, dto.ChatExists{
			ChatId: ce,
			Exists: utils.Contains(existsFromDb, ce),
		})
	}

	g.JSON(http.StatusOK, responseList)
	return
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
	var participantsError *cqrs.ParticipantsError
	if errors.As(err, &validationError) {
		g.JSON(http.StatusBadRequest, &dto.ErrorMessageDto{validationError.Error()})
		return true
	} else if errors.As(err, &unauthError) {
		g.JSON(http.StatusUnauthorized, &dto.ErrorMessageDto{unauthError.Error()})
		return true
	} else if errors.As(err, &chatStillNotExistsError) {
		g.Status(http.StatusTeapot)
		return true
	} else if errors.As(err, &participantsError) {
		g.Status(http.StatusBadRequest)
		return true
	}
	return false
}
