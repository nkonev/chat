package handlers

import (
	"errors"
	"go-cqrs-chat-example/client"
	"go-cqrs-chat-example/cqrs"
	"go-cqrs-chat-example/db"
	"go-cqrs-chat-example/dto"
	"go-cqrs-chat-example/logger"
	"go-cqrs-chat-example/utils"
	"net/http"

	"github.com/gin-gonic/gin"
)

type ParticipantHandler struct {
	lgr              *logger.LoggerWrapper
	eventBus         *cqrs.PartitionAwareEventBus
	dbWrapper        *db.DB
	commonProjection *cqrs.CommonProjection
	aaaRestClient    client.AaaRestClient
}

func NewParticipantHandler(
	lgr *logger.LoggerWrapper,
	eventBus *cqrs.PartitionAwareEventBus,
	dbWrapper *db.DB,
	commonProjection *cqrs.CommonProjection,
	restClient client.AaaRestClient,
) *ParticipantHandler {
	return &ParticipantHandler{
		lgr:              lgr,
		eventBus:         eventBus,
		dbWrapper:        dbWrapper,
		commonProjection: commonProjection,
		aaaRestClient:    restClient,
	}
}

func (ch *ParticipantHandler) AddParticipant(g *gin.Context) {
	cid := g.Param(dto.ChatIdParam)

	chatId, err := utils.ParseInt64(cid)
	if err != nil {
		ch.lgr.WithTrace(g.Request.Context()).Error("Error binding chatId", "err", err)
		g.Status(http.StatusInternalServerError)
		return
	}

	ccd := new(dto.ParticipantAddDto)

	err = g.Bind(ccd)
	if err != nil {
		ch.lgr.WithTrace(g.Request.Context()).Error("Error binding ParticipantAddDto", "err", err)
		g.Status(http.StatusInternalServerError)
		return
	}

	cc := cqrs.ParticipantAdd{
		AdditionalData: cqrs.GenerateMessageAdditionalData(),
		ParticipantIds: ccd.ParticipantIds,
		ChatId:         chatId,
	}

	err = cc.Handle(g.Request.Context(), ch.eventBus, ch.dbWrapper, ch.commonProjection)
	if err != nil {
		ch.lgr.WithTrace(g.Request.Context()).Error("Error sending ParticipantAdd command", "err", err)
		g.Status(http.StatusInternalServerError)
		return
	}

	g.Status(http.StatusOK)
}

func (ch *ParticipantHandler) DeleteParticipant(g *gin.Context) {
	cid := g.Param(dto.ChatIdParam)

	chatId, err := utils.ParseInt64(cid)
	if err != nil {
		ch.lgr.WithTrace(g.Request.Context()).Error("Error binding chatId", "err", err)
		g.Status(http.StatusInternalServerError)
		return
	}

	ccd := new(dto.ParticipantDeleteDto)

	err = g.Bind(ccd)
	if err != nil {
		ch.lgr.WithTrace(g.Request.Context()).Error("Error binding ParticipantDeleteDto", "err", err)
		g.Status(http.StatusInternalServerError)
		return
	}

	cc := cqrs.ParticipantDelete{
		AdditionalData: cqrs.GenerateMessageAdditionalData(),
		ParticipantIds: ccd.ParticipantIds,
		ChatId:         chatId,
	}

	err = cc.Handle(g.Request.Context(), ch.eventBus, ch.dbWrapper, ch.commonProjection)
	if err != nil {
		ch.lgr.WithTrace(g.Request.Context()).Error("Error sending ParticipantDelete command", "err", err)
		g.Status(http.StatusInternalServerError)
		return
	}

	g.Status(http.StatusOK)
}

func (ch *ParticipantHandler) ChangeParticipant(g *gin.Context) {
	userId, err := getUserId(g)
	if err != nil {
		ch.lgr.WithTrace(g.Request.Context()).Error("Error parsing UserId", "err", err)
		g.Status(http.StatusInternalServerError)
		return
	}

	cid := g.Param(dto.ChatIdParam)

	chatId, err := utils.ParseInt64(cid)
	if err != nil {
		ch.lgr.WithTrace(g.Request.Context()).Error("Error binding chatId", "err", err)
		g.Status(http.StatusInternalServerError)
		return
	}

	interestingUserId, err := utils.ParseInt64(g.Param(dto.ParticipantIdParam))
	if err != nil {
		ch.lgr.WithTrace(g.Request.Context()).Error("Error binding participantId", "err", err)
		g.Status(http.StatusInternalServerError)
		return
	}

	newAdmin := utils.GetBoolean(g.Query(dto.AdminParam))

	cc := cqrs.ParticipantChange{
		AdditionalData: cqrs.GenerateMessageAdditionalData(),
		ParticipantId:  interestingUserId,
		ChatId:         chatId,
		BehalfUserId:   userId,
		NewAdmin:       newAdmin,
	}

	err = cc.Handle(g.Request.Context(), ch.eventBus, ch.dbWrapper, ch.commonProjection)
	var unauthError *cqrs.UnauthorizedError
	if errors.As(err, &unauthError) {
		ch.lgr.WithTrace(g.Request.Context()).Info("unauthorized", "err", unauthError)
		g.Status(http.StatusUnauthorized)
		return
	} else if err != nil {
		ch.lgr.WithTrace(g.Request.Context()).Error("Error sending ParticipantChange command", "err", err)
		g.Status(http.StatusInternalServerError)
		return
	}

	g.Status(http.StatusOK)
}

func (ch *ParticipantHandler) GetParticipants(g *gin.Context) {
	cid := g.Param(dto.ChatIdParam)

	chatId, err := utils.ParseInt64(cid)
	if err != nil {
		ch.lgr.WithTrace(g.Request.Context()).Error("Error binding chatId", "err", err)
		g.Status(http.StatusInternalServerError)
		return
	}

	participantsPage := utils.FixPageString(g.Query(dto.PageParam))
	participantsSize := utils.FixSizeString(g.Query(dto.SizeParam))
	participantsOffset := utils.GetOffset(participantsPage, participantsSize)
	reverse := utils.GetBooleanOr(g.Query(dto.ReverseParam), true)

	participants, err := ch.commonProjection.GetParticipantIdsForExternal(g.Request.Context(), chatId, participantsSize, participantsOffset, reverse)
	if err != nil {
		ch.lgr.WithTrace(g.Request.Context()).Error("Error getting participants", "err", err)
		g.Status(http.StatusInternalServerError)
		return
	}

	participantIds := cqrs.GetParticipantIds(participants)

	users, err := ch.aaaRestClient.GetUsers(g.Request.Context(), participantIds)
	if err != nil {
		ch.lgr.WithTrace(g.Request.Context()).Warn("unable to get users")
	}

	orderedEnrichedParticipants := makeParticipantsWithAdmin(participants, users)

	g.JSON(http.StatusOK, orderedEnrichedParticipants)
}
