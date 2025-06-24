package handlers

import (
	"go-cqrs-chat-example/client"
	"go-cqrs-chat-example/cqrs"
	"go-cqrs-chat-example/db"
	"go-cqrs-chat-example/dto"
	"go-cqrs-chat-example/logger"
	"go-cqrs-chat-example/utils"
	"net/http"
	"slices"
	"time"

	"github.com/gin-gonic/gin"
)

type ChatHandler struct {
	lgr              *logger.LoggerWrapper
	eventBus         *cqrs.PartitionAwareEventBus
	dbWrapper        *db.DB
	commonProjection *cqrs.CommonProjection
	restClient       *client.RestClient
}

func NewChatHandler(
	lgr *logger.LoggerWrapper,
	eventBus *cqrs.PartitionAwareEventBus,
	dbWrapper *db.DB,
	commonProjection *cqrs.CommonProjection,
	restClient *client.RestClient,
) *ChatHandler {
	return &ChatHandler{
		lgr:              lgr,
		eventBus:         eventBus,
		dbWrapper:        dbWrapper,
		commonProjection: commonProjection,
		restClient:       restClient,
	}
}

func (ch *ChatHandler) CreateChat(g *gin.Context) {
	ccd := new(dto.ChatCreateDto)

	err := g.Bind(ccd)
	if err != nil {
		ch.lgr.WithTrace(g.Request.Context()).Error("Error binding ChatCreateDto", "err", err)
		g.Status(http.StatusInternalServerError)
		return
	}

	userId, err := getUserId(g)
	if err != nil {
		ch.lgr.WithTrace(g.Request.Context()).Error("Error parsing UserId", "err", err)
		g.Status(http.StatusInternalServerError)
		return
	}

	cc := cqrs.ChatCreate{
		AdditionalData: cqrs.GenerateMessageAdditionalData(),
		Title:          ccd.Title,
		ParticipantIds: ccd.ParticipantIds,
	}

	if !slices.Contains(cc.ParticipantIds, userId) {
		cc.ParticipantIds = append(cc.ParticipantIds, userId)
	}

	chatId, err := cc.Handle(g.Request.Context(), ch.eventBus, ch.dbWrapper, ch.commonProjection)
	if err != nil {
		ch.lgr.WithTrace(g.Request.Context()).Error("Error sending ChatCreate command", "err", err)
		g.Status(http.StatusInternalServerError)
		return
	}

	m := dto.IdResponse{Id: chatId}

	g.JSON(http.StatusOK, m)
}

func (ch *ChatHandler) EditChat(g *gin.Context) {
	ccd := new(dto.ChatEditDto)

	err := g.Bind(ccd)
	if err != nil {
		ch.lgr.WithTrace(g.Request.Context()).Error("Error binding ChatEditDto", "err", err)
		g.Status(http.StatusInternalServerError)
		return
	}

	cc := cqrs.ChatEdit{
		AdditionalData:      cqrs.GenerateMessageAdditionalData(),
		ChatId:              ccd.Id,
		Title:               ccd.Title,
		ParticipantIdsToAdd: ccd.ParticipantIds,
		Blog:                ccd.Blog,
	}

	err = cc.Handle(g.Request.Context(), ch.eventBus, ch.dbWrapper, ch.commonProjection)
	if err != nil {
		ch.lgr.WithTrace(g.Request.Context()).Error("Error sending ChatEdit command", "err", err)
		g.Status(http.StatusInternalServerError)
		return
	}

	g.Status(http.StatusOK)
}

func (ch *ChatHandler) DeleteChat(g *gin.Context) {

	cid := g.Param(ChatIdParam)

	chatId, err := utils.ParseInt64(cid)
	if err != nil {
		ch.lgr.WithTrace(g.Request.Context()).Error("Error binding chatId", "err", err)
		g.Status(http.StatusInternalServerError)
		return
	}

	cc := cqrs.ChatDelete{
		AdditionalData: cqrs.GenerateMessageAdditionalData(),
		ChatId:         chatId,
	}

	err = cc.Handle(g.Request.Context(), ch.eventBus, ch.dbWrapper, ch.commonProjection)
	if err != nil {
		ch.lgr.WithTrace(g.Request.Context()).Error("Error sending ChatDelete command", "err", err)
		g.Status(http.StatusInternalServerError)
		return
	}

	g.Status(http.StatusOK)
}

func (ch *ChatHandler) PinChat(g *gin.Context) {
	cid := g.Param(ChatIdParam)

	chatId, err := utils.ParseInt64(cid)
	if err != nil {
		ch.lgr.WithTrace(g.Request.Context()).Error("Error binding chatId", "err", err)
		g.Status(http.StatusInternalServerError)
		return
	}

	p := g.Query(PinParam)

	pin := utils.GetBoolean(p)

	userId, err := getUserId(g)
	if err != nil {
		ch.lgr.WithTrace(g.Request.Context()).Error("Error parsing UserId", "err", err)
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
		ch.lgr.WithTrace(g.Request.Context()).Error("Error sending ChatPin command", "err", err)
		g.Status(http.StatusInternalServerError)
		return
	}

	g.Status(http.StatusOK)
}

func (ch *ChatHandler) SearchChats(g *gin.Context) {
	userId, err := getUserId(g)
	if err != nil {
		ch.lgr.WithTrace(g.Request.Context()).Error("Error parsing UserId", "err", err)
		g.Status(http.StatusInternalServerError)
		return
	}

	size := utils.FixSizeString(g.Query(SizeParam))
	reverse := utils.GetBoolean(g.Query(ReverseParam))

	pinned := utils.GetBooleanNullable(g.Query(PinnedParam))
	lastUpdateDateTime := utils.GetTimeNullable(g.Query(LastUpdateDateTimeParam))
	id := utils.ParseInt64Nullable(g.Query(ChatIdParam))
	startingFromItemId := ch.convertChatId(pinned, lastUpdateDateTime, id)

	includeStartingFrom := utils.GetBoolean(g.Query(IncludeStartingFromParam))

	chats, err := ch.commonProjection.GetChats(g.Request.Context(), userId, size, startingFromItemId, includeStartingFrom, reverse)
	if err != nil {
		ch.lgr.WithTrace(g.Request.Context()).Error("Error getting chats", "err", err)
		g.Status(http.StatusInternalServerError)
		return
	}

	userIds := getUserIds(chats)
	users, err := ch.restClient.GetUsers(g.Request.Context(), userIds)
	if err != nil {
		ch.lgr.WithTrace(g.Request.Context()).Warn("unable to get users")
	}
	chatsEnriched := enrichChats(chats, users)

	g.JSON(http.StatusOK, chatsEnriched)
}

func getUserIds(chats []dto.ChatViewDto) []int64 {
	m := map[int64]struct{}{}

	for _, ch := range chats {
		for _, p := range ch.ParticipantIds {
			m[p] = struct{}{}
		}
	}

	r := []int64{}

	for k, _ := range m {
		r = append(r, k)
	}
	return r
}

func findUserById(users []dto.User, userId int64) *dto.User {
	for _, u := range users {
		if u.Id == userId {
			return &u
		}
	}
	return nil
}

func enrichChats(chats []dto.ChatViewDto, users []dto.User) []dto.ChatViewEnrichedDto {
	res := make([]dto.ChatViewEnrichedDto, 0, len(users))
	for _, ch := range chats {
		che := dto.ChatViewEnrichedDto{
			ChatViewDto:  ch,
			Participants: makeParticipants(ch.ParticipantIds, users),
		}
		res = append(res, che)
	}
	return res
}

func makeParticipants(participantIds []int64, users []dto.User) []dto.User {
	res := make([]dto.User, 0, len(participantIds))

	for _, p := range participantIds {
		u := findUserById(users, p)
		if u != nil {
			res = append(res, *u)
		}
	}

	return res
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
