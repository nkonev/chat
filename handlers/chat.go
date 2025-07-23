package handlers

import (
	"go-cqrs-chat-example/client"
	"go-cqrs-chat-example/cqrs"
	"go-cqrs-chat-example/db"
	"go-cqrs-chat-example/dto"
	"go-cqrs-chat-example/logger"
	"go-cqrs-chat-example/services"
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
	aaaRestClient    client.AaaRestClient
	stripTagsPolicy  *services.StripTagsPolicy
}

func NewChatHandler(
	lgr *logger.LoggerWrapper,
	eventBus *cqrs.PartitionAwareEventBus,
	dbWrapper *db.DB,
	commonProjection *cqrs.CommonProjection,
	restClient client.AaaRestClient,
	stripTagsPolicy *services.StripTagsPolicy,
) *ChatHandler {
	return &ChatHandler{
		lgr:              lgr,
		eventBus:         eventBus,
		dbWrapper:        dbWrapper,
		commonProjection: commonProjection,
		aaaRestClient:    restClient,
		stripTagsPolicy:  stripTagsPolicy,
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

	ccd.Title = TrimAmdSanitizeChatTitle(ch.stripTagsPolicy, ccd.Title)

	if ccd.IsValidatabale() {
		if err = ccd.Validate(); err != nil {
			ch.lgr.DebugContext(g.Request.Context(), "Error during validation: %v", err)
			g.Status(http.StatusBadRequest)
			return
		}
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

	if !slices.Contains(cc.ParticipantIds, userId) {
		cc.ParticipantIds = append(cc.ParticipantIds, userId)
	}

	chatId, err := cc.Handle(g.Request.Context(), userId, ch.eventBus, ch.dbWrapper, ch.commonProjection)
	if err != nil {
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

	ccd.Title = TrimAmdSanitizeChatTitle(ch.stripTagsPolicy, ccd.Title)

	if ccd.IsValidatabale() {
		if err = ccd.Validate(); err != nil {
			ch.lgr.DebugContext(g.Request.Context(), "Error during validation: %v", err)
			g.Status(http.StatusBadRequest)
			return
		}
	}

	cc := cqrs.ChatEdit{
		AdditionalData:      cqrs.GenerateMessageAdditionalData(),
		ChatId:              ccd.Id,
		Title:               ccd.Title,
		ParticipantIdsToAdd: ccd.ParticipantIds,
		Blog:                ccd.Blog,
		BehalfUserId:        userId,
	}

	err = cc.Handle(g.Request.Context(), ch.eventBus, ch.dbWrapper, ch.commonProjection)
	if err != nil {
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

	chats, err := ch.commonProjection.GetChats(g.Request.Context(), userId, size, startingFromItemId, includeStartingFrom, reverse)
	if err != nil {
		ch.lgr.ErrorContext(g.Request.Context(), "Error getting chats", "err", err)
		g.Status(http.StatusInternalServerError)
		return
	}

	userIds := getUserIdsFromChats(chats)
	users, err := ch.aaaRestClient.GetUsers(g.Request.Context(), userIds)
	if err != nil {
		ch.lgr.WarnContext(g.Request.Context(), "unable to get users")
	}
	chatsEnriched := enrichChats(chats, users)

	g.JSON(http.StatusOK, chatsEnriched)
}

func getUserIdsFromChats(chats []dto.ChatViewDto) []int64 {
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
	res := make([]dto.ChatViewEnrichedDto, 0, len(chats))
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

func makeParticipantsWithAdmin(participants []cqrs.ParticipantWithAdmin, users []dto.User) []dto.UserWithAdmin {
	res := make([]dto.UserWithAdmin, 0, len(participants))

	for _, p := range participants {
		u := findUserById(users, p.ParticipantId)
		if u != nil {
			res = append(res, dto.UserWithAdmin{
				User:      *u,
				ChatAdmin: p.ChatAdmin,
			})
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
