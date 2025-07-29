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

	cc := cqrs.MessageCreate{
		AdditionalData: cqrs.GenerateMessageAdditionalData(),
		ChatId:         chatId,
		Content:        mcd.Content,
		OwnerId:        userId,
	}

	mid, wasAdded, err := cc.Handle(g.Request.Context(), mc.eventBus, mc.dbWrapper, mc.commonProjection, mc.cfg, mc.lgr, mc.policy)
	if err != nil {
		if translateMessageError(g, err) {
			return
		}

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
		AdditionalData: cqrs.GenerateMessageAdditionalData(),
		MessageId:      ccd.Id,
		ChatId:         chatId,
		Content:        ccd.Content,
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

	messages, err := mc.commonProjection.GetMessages(g.Request.Context(), chatId, size, startingFromItemId, includeStartingFrom, reverse)
	if err != nil {
		mc.lgr.ErrorContext(g.Request.Context(), "Error getting messages", "err", err)
		g.Status(http.StatusInternalServerError)
		return
	}

	var usersSet = map[int64]bool{}
	var chatsPreSet = map[int64]bool{}
	for _, message := range messages {
		populateSets(message, usersSet, chatsPreSet, true)
	}
	chats, err := mc.commonProjection.GetChatsBasicExtended(g.Request.Context(), mc.dbWrapper, chatsPreSet, userId)
	if err != nil {
		mc.lgr.ErrorContext(g.Request.Context(), "Error getting chat basic", "err", err)
		g.Status(http.StatusInternalServerError)
		return
	}

	users, err := mc.aaaRestClient.GetUsers(g.Request.Context(), usersSet)
	if err != nil {
		mc.lgr.WarnContext(g.Request.Context(), "unable to get users")
	}

	messagesEnriched := enrichMessages(messages, users, chats)

	g.JSON(http.StatusOK, messagesEnriched)
}

// todo move enrichment to projections
func enrichMessages(messages []dto.MessageViewDto, users []dto.User, chats map[int64]*dto.BasicChatDtoExtended) []dto.MessageViewEnrichedDto {
	res := make([]dto.MessageViewEnrichedDto, 0, len(messages))
	for _, m := range messages {
		me := dto.MessageViewEnrichedDto{
			Id:             m.Id,
			OwnerId:        m.OwnerId,
			Content:        m.Content,
			BlogPost:       m.BlogPost,
			UpdateDateTime: m.UpdateDateTime,
			CreateDateTime: m.CreateDateTime,
			Owner:          findUserById(users, m.OwnerId),
		}
		res = append(res, me)
	}
	return res
}

func setEmbed(dbMessage dto.MessageViewDto, ret dto.MessageViewEnrichedDto, users map[int64]*dto.User, chats map[int64]*dto.BasicChatDtoExtended) {
	if dbMessage.ResponseEmbeddedMessageReplyOwnerId != nil {
		embeddedUser := users[*dbMessage.ResponseEmbeddedMessageReplyOwnerId]
		ret.EmbedMessage = &dto.EmbedMessageResponse{
			Id:        *dbMessage.ResponseEmbeddedMessageReplyId,
			Text:      *dbMessage.ResponseEmbeddedMessageReplyText,
			EmbedType: *dbMessage.ResponseEmbeddedMessageType,
			Owner:     embeddedUser,
		}
	} else if dbMessage.ResponseEmbeddedMessageResendOwnerId != nil {
		embeddedUser := users[*dbMessage.ResponseEmbeddedMessageResendOwnerId]
		basicEmbeddedChat := chats[*dbMessage.ResponseEmbeddedMessageResendChatId]
		var embedChatName *string = nil
		var isParticipant bool
		if basicEmbeddedChat != nil { // basicEmbeddedChat can be deleted
			if !basicEmbeddedChat.TetATet {
				embedChatName = &basicEmbeddedChat.Title
			}
			isParticipant = basicEmbeddedChat.BehalfUserIsParticipant
		}

		ret.EmbedMessage = &dto.EmbedMessageResponse{
			Id:            *dbMessage.ResponseEmbeddedMessageResendId,
			ChatId:        dbMessage.ResponseEmbeddedMessageResendChatId,
			ChatName:      embedChatName,
			Text:          dbMessage.Content,
			EmbedType:     *dbMessage.ResponseEmbeddedMessageType,
			Owner:         embeddedUser,
			IsParticipant: isParticipant,
		}
		ret.Content = ""
	}
}

// returns should exit
func translateMessageError(g *gin.Context, err error) bool {
	if err == nil {
		return false
	}
	var mediaError *cqrs.MediaUrlErr
	var mediaOverflowError *cqrs.MediaOverflowErr
	var validationError *cqrs.ValidationError
	if errors.As(err, &mediaError) {
		g.JSON(http.StatusBadRequest, &utils.H{"message": mediaError.Error(), "businessErrorCode": badMediaUrl})
		return true
	} else if errors.As(err, &mediaOverflowError) {
		g.JSON(http.StatusBadRequest, &utils.H{"message": mediaOverflowError.Error()})
		return true
	} else if errors.As(err, &validationError) {
		g.JSON(http.StatusBadRequest, &utils.H{"message": validationError.Error()})
		return true
	}
	return false
}
