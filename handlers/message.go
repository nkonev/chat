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
	stripAllTags        *sanitizer.StripTagsPolicy
	cfg                 *config.AppConfig
	enrichingProjection *cqrs.EnrichingProjection
	asyncMessageService *services.AsyncMessageService
	messageService      *services.MessageService
}

func NewMessageHandler(
	lgr *logger.LoggerWrapper,
	eventBus *cqrs.PartitionAwareEventBus,
	dbWrapper *db.DB,
	commonProjection *cqrs.CommonProjection,
	policy *sanitizer.SanitizerPolicy,
	stripAllTags *sanitizer.StripTagsPolicy,
	cfg *config.AppConfig,
	enrichingProjection *cqrs.EnrichingProjection,
	asyncMessageService *services.AsyncMessageService, // we use async message service in order not to perform potentially heavyweight iterations in user-facing handles
	messageService *services.MessageService,
) *MessageHandler {
	return &MessageHandler{
		lgr:                 lgr,
		eventBus:            eventBus,
		dbWrapper:           dbWrapper,
		commonProjection:    commonProjection,
		policy:              policy,
		stripAllTags:        stripAllTags,
		cfg:                 cfg,
		enrichingProjection: enrichingProjection,
		asyncMessageService: asyncMessageService,
		messageService:      messageService,
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

	err = mr.Handle(g.Request.Context(), mc.eventBus, mc.dbWrapper, mc.commonProjection, mc.policy)
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
	reverse := true // true for edge
	var startingFromItemId *int64 = nil
	includeStartingFrom := false
	searchString := g.Query(dto.SearchStringParam)

	var bindTo = make([]dto.MessageViewEnrichedDto, 0)
	if err := g.Bind(&bindTo); err != nil {
		mc.lgr.ErrorContext(g.Request.Context(), "Error during binding to dto", "err", err)
		g.Status(http.StatusInternalServerError)
		return
	}

	messageDtos, err := mc.enrichingProjection.GetMessagesEnriched(g.Request.Context(), []int64{userId}, true, &userId, chatId, size, startingFromItemId, includeStartingFrom, reverse, searchString, nil)
	if err != nil {
		if translateMessageError(g, err) {
			return
		}

		mc.lgr.ErrorContext(g.Request.Context(), "Error getting messages", "err", err)
		g.Status(http.StatusInternalServerError)
		return
	}

	edge := true

	aLen := min(len(messageDtos), len(bindTo))
	if len(bindTo) == 0 && len(messageDtos) != 0 {
		edge = false
	}

	for i := range aLen {
		currentMessage := messageDtos[i]
		gottenMessage := bindTo[i]
		if currentMessage.Id != gottenMessage.Id {
			edge = false
			break
		}

		// we strip tags because a (public) video link has "live" time parameter, which is changed between requests
		// it leads us to the false comparison
		// so we remove all the tags to mitigate this issue
		currentMsgText := mc.stripAllTags.Sanitize(currentMessage.Content)
		gottenMsgText := mc.stripAllTags.Sanitize(gottenMessage.Content)
		if currentMsgText != gottenMsgText {
			edge = false
			break
		}
		if len(currentMessage.Reactions) != len(gottenMessage.Reactions) {
			edge = false
			break
		}
		if currentMessage.BlogPost != gottenMessage.BlogPost {
			edge = false
			break
		}
	}

	g.JSON(http.StatusOK, dto.FreshDto{
		Ok: edge,
	})
	return
}

func (mc *MessageHandler) MessagesFilter(g *gin.Context) {
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

	d := new(dto.MessageFilterDto)
	err = g.Bind(d)
	if err != nil {
		mc.lgr.ErrorContext(g.Request.Context(), "Error binding MessageFilterDto", "err", err)
		g.Status(http.StatusInternalServerError)
		return
	}

	searchString := d.SearchString
	messageId := d.MessageId

	found, err := mc.enrichingProjection.MessageFilter(g.Request.Context(), mc.dbWrapper, userId, chatId, searchString, messageId)
	if err != nil {
		if translateMessageError(g, err) {
			return
		}

		mc.lgr.ErrorContext(g.Request.Context(), "Error invoking MessageFilter", "err", err)
		g.Status(http.StatusInternalServerError)
		return
	}

	g.JSON(http.StatusOK, dto.FilterDto{
		Found: found,
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

func (mc *MessageHandler) MessagePreview(g *gin.Context) {
	bindTo := new(dto.CleanHtmlTagsRequestDto)
	err := g.Bind(bindTo)
	if err != nil {
		mc.lgr.ErrorContext(g.Request.Context(), "Error binding CleanHtmlTagsRequestDto", "err", err)
		g.Status(http.StatusInternalServerError)
		return
	}

	preview := mc.messageService.CreatePreview(bindTo.Text, bindTo.Login)
	response := dto.CleanHtmlTagsResponseDto{
		Text: preview,
	}
	g.JSON(http.StatusOK, response)
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
