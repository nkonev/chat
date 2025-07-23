package handlers

import (
	"errors"
	"go-cqrs-chat-example/client"
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
	lgr              *logger.LoggerWrapper
	eventBus         *cqrs.PartitionAwareEventBus
	dbWrapper        *db.DB
	commonProjection *cqrs.CommonProjection
	aaaRestClient    client.AaaRestClient
	policy           *services.SanitizerPolicy
	cfg              *config.AppConfig
}

func NewMessageHandler(
	lgr *logger.LoggerWrapper,
	eventBus *cqrs.PartitionAwareEventBus,
	dbWrapper *db.DB,
	commonProjection *cqrs.CommonProjection,
	restClient client.AaaRestClient,
	policy *services.SanitizerPolicy,
	cfg *config.AppConfig,
) *MessageHandler {
	return &MessageHandler{
		lgr:              lgr,
		eventBus:         eventBus,
		dbWrapper:        dbWrapper,
		commonProjection: commonProjection,
		aaaRestClient:    restClient,
		policy:           policy,
		cfg:              cfg,
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

	trimmedAndSanitized, err := TrimAmdSanitizeMessage(g.Request.Context(), mc.cfg, mc.lgr, mc.policy, mcd.Content)
	if err != nil {
		if translateMessageError(g, err) {
			return
		} else {
			mc.lgr.ErrorContext(g.Request.Context(), "Error while changing message text", "err", err)
			g.Status(http.StatusInternalServerError)
			return
		}
	}
	mcd.Content = trimmedAndSanitized

	if mcd.IsValidatabale() {
		if err = mcd.Validate(); err != nil {
			mc.lgr.DebugContext(g.Request.Context(), "Error during validation: %v", err)
			g.Status(http.StatusBadRequest)
			return
		}
	}

	cc := cqrs.MessageCreate{
		AdditionalData: cqrs.GenerateMessageAdditionalData(),
		ChatId:         chatId,
		Content:        mcd.Content,
		OwnerId:        userId,
	}

	mid, wasAdded, err := cc.Handle(g.Request.Context(), mc.eventBus, mc.dbWrapper, mc.commonProjection)
	if err != nil {
		mc.lgr.ErrorContext(g.Request.Context(), "Error sending MessageCreate command", "err", err)
		g.Status(http.StatusInternalServerError)
		return
	}

	if !wasAdded {
		g.Status(http.StatusTeapot)
		return
	}

	m := dto.IdResponse{Id: mid}

	g.JSON(http.StatusOK, m)
}

// returns should exit
func translateMessageError(g *gin.Context, err error) bool {
	if err == nil {
		return false
	}
	var mediaError *MediaUrlErr
	var mediaOverflowError *MediaOverflowErr
	if errors.As(err, &mediaError) {
		g.JSON(http.StatusBadRequest, &utils.H{"message": mediaError.Error(), "businessErrorCode": badMediaUrl})
		return true
	} else if errors.As(err, &mediaOverflowError) {
		g.JSON(http.StatusBadRequest, &utils.H{"message": mediaOverflowError.Error()})
		return true
	}
	return false
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

	trimmedAndSanitized, err := TrimAmdSanitizeMessage(g.Request.Context(), mc.cfg, mc.lgr, mc.policy, ccd.Content)
	if err != nil {
		if translateMessageError(g, err) {
			return
		} else {
			mc.lgr.ErrorContext(g.Request.Context(), "Error while changing message text", "err", err)
			g.Status(http.StatusInternalServerError)
			return
		}
	}
	ccd.Content = trimmedAndSanitized

	if ccd.IsValidatabale() {
		if err = ccd.Validate(); err != nil {
			mc.lgr.DebugContext(g.Request.Context(), "Error during validation: %v", err)
			g.Status(http.StatusBadRequest)
			return
		}
	}

	cc := cqrs.MessageEdit{
		AdditionalData: cqrs.GenerateMessageAdditionalData(),
		MessageId:      ccd.Id,
		ChatId:         chatId,
		Content:        ccd.Content,
	}

	err = cc.Handle(g.Request.Context(), mc.eventBus, mc.dbWrapper, mc.commonProjection, userId)
	if err != nil {
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
		AdditionalData: cqrs.GenerateMessageAdditionalData(),
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
		AdditionalData: cqrs.GenerateMessageAdditionalData(),
		ChatId:         chatId,
		MessageId:      messageId,
		ParticipantId:  userId,
	}

	err = mr.Handle(g.Request.Context(), mc.eventBus, mc.commonProjection)
	if err != nil {
		mc.lgr.ErrorContext(g.Request.Context(), "Error sending MessageRead command", "err", err)
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
		AdditionalData: cqrs.GenerateMessageAdditionalData(),
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

	messages, err := mc.commonProjection.GetMessages(g.Request.Context(), chatId, size, startingFromItemId, includeStartingFrom, reverse)
	if err != nil {
		mc.lgr.ErrorContext(g.Request.Context(), "Error getting messages", "err", err)
		g.Status(http.StatusInternalServerError)
		return
	}

	userIds := getUserIdsFromMessages(messages)
	users, err := mc.aaaRestClient.GetUsers(g.Request.Context(), userIds)
	if err != nil {
		mc.lgr.WarnContext(g.Request.Context(), "unable to get users")
	}
	messagesEnriched := enrichMessages(messages, users)

	g.JSON(http.StatusOK, messagesEnriched)
}

func getUserIdsFromMessages(messages []dto.MessageViewDto) []int64 {
	m := map[int64]struct{}{}

	for _, msg := range messages {
		m[msg.OwnerId] = struct{}{}
	}

	r := []int64{}

	for k, _ := range m {
		r = append(r, k)
	}
	return r
}

func enrichMessages(messages []dto.MessageViewDto, users []dto.User) []dto.MessageViewEnrichedDto {
	res := make([]dto.MessageViewEnrichedDto, 0, len(messages))
	for _, m := range messages {
		me := dto.MessageViewEnrichedDto{
			MessageViewDto: m,
			Owner:          findUserById(users, m.OwnerId),
		}
		res = append(res, me)
	}
	return res
}
