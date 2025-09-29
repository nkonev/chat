package handlers

import (
	"errors"
	"go-cqrs-chat-example/config"
	"go-cqrs-chat-example/cqrs"
	"go-cqrs-chat-example/db"
	"go-cqrs-chat-example/dto"
	"go-cqrs-chat-example/logger"
	"go-cqrs-chat-example/utils"
	"net/http"

	"github.com/gin-gonic/gin"
)

type ParticipantHandler struct {
	lgr                 *logger.LoggerWrapper
	eventBus            *cqrs.PartitionAwareEventBus
	dbWrapper           *db.DB
	commonProjection    *cqrs.CommonProjection
	enrichingProjection *cqrs.EnrichingProjection
	cfg                 *config.AppConfig
}

func NewParticipantHandler(
	lgr *logger.LoggerWrapper,
	eventBus *cqrs.PartitionAwareEventBus,
	dbWrapper *db.DB,
	commonProjection *cqrs.CommonProjection,
	enrichingProjection *cqrs.EnrichingProjection,
	cfg *config.AppConfig,
) *ParticipantHandler {
	return &ParticipantHandler{
		lgr:                 lgr,
		eventBus:            eventBus,
		dbWrapper:           dbWrapper,
		commonProjection:    commonProjection,
		enrichingProjection: enrichingProjection,
		cfg:                 cfg,
	}
}

func (ch *ParticipantHandler) AddParticipant(g *gin.Context) {
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

	ccd := new(dto.ParticipantAddDto)

	err = g.Bind(ccd)
	if err != nil {
		ch.lgr.ErrorContext(g.Request.Context(), "Error binding ParticipantAddDto", "err", err)
		g.Status(http.StatusInternalServerError)
		return
	}

	cc := cqrs.ParticipantAdd{
		AdditionalData: cqrs.GenerateMessageAdditionalData(getCorrelationId(g), userId),
		ParticipantIds: ccd.ParticipantIds,
		ChatId:         chatId,
	}

	err = cc.Handle(g.Request.Context(), ch.eventBus, ch.dbWrapper, ch.commonProjection, ch.cfg)
	if err != nil {
		if translateParticipantError(g, err) {
			return
		}

		ch.lgr.ErrorContext(g.Request.Context(), "Error sending ParticipantAdd command", "err", err)
		g.Status(http.StatusInternalServerError)
		return
	}

	g.Status(http.StatusOK)
}

func (ch *ParticipantHandler) DeleteParticipant(g *gin.Context) {
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

	ccd := new(dto.ParticipantDeleteDto)

	err = g.Bind(ccd)
	if err != nil {
		ch.lgr.ErrorContext(g.Request.Context(), "Error binding ParticipantDeleteDto", "err", err)
		g.Status(http.StatusInternalServerError)
		return
	}

	cc := cqrs.ParticipantDelete{
		AdditionalData: cqrs.GenerateMessageAdditionalData(getCorrelationId(g), userId),
		ParticipantIds: ccd.ParticipantIds,
		ChatId:         chatId,
	}

	err = cc.Handle(g.Request.Context(), ch.eventBus, ch.dbWrapper, ch.commonProjection, ch.cfg)
	if err != nil {
		if translateParticipantError(g, err) {
			return
		}

		ch.lgr.ErrorContext(g.Request.Context(), "Error sending ParticipantDelete command", "err", err)
		g.Status(http.StatusInternalServerError)
		return
	}

	g.Status(http.StatusOK)
}

func (ch *ParticipantHandler) ChangeParticipant(g *gin.Context) {
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

	interestingUserId, err := utils.ParseInt64(g.Param(dto.ParticipantIdParam))
	if err != nil {
		ch.lgr.ErrorContext(g.Request.Context(), "Error binding participantId", "err", err)
		g.Status(http.StatusInternalServerError)
		return
	}

	newAdmin := utils.GetBoolean(g.Query(dto.AdminParam))

	cc := cqrs.ParticipantChange{
		AdditionalData: cqrs.GenerateMessageAdditionalData(getCorrelationId(g), userId),
		ParticipantId:  interestingUserId,
		ChatId:         chatId,
		NewAdmin:       newAdmin,
	}

	err = cc.Handle(g.Request.Context(), ch.eventBus, ch.dbWrapper, ch.commonProjection)
	if err != nil {
		if translateParticipantError(g, err) {
			return
		}

		ch.lgr.ErrorContext(g.Request.Context(), "Error sending ParticipantChange command", "err", err)
		g.Status(http.StatusInternalServerError)
		return
	}

	g.Status(http.StatusOK)
}

func (ch *ParticipantHandler) SearchParticipants(g *gin.Context) {
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

	participantsPage := utils.FixPageString(g.Query(dto.PageParam))
	participantsSize := utils.FixSizeString(g.Query(dto.SizeParam))
	participantsOffset := utils.GetOffset(participantsPage, participantsSize)
	searchString := g.Query(dto.SearchStringParam)

	participants, count, err := ch.enrichingProjection.GetParticipantsEnriched(g.Request.Context(), userId, chatId, participantsSize, participantsOffset, searchString)
	if err != nil {
		if translateParticipantError(g, err) {
			return
		}

		ch.lgr.ErrorContext(g.Request.Context(), "Error getting participants", "err", err)
		g.Status(http.StatusInternalServerError)
		return
	}

	g.JSON(http.StatusOK, dto.ParticipantsWithAdminWrapper{
		Data:  participants,
		Count: count,
	})
}

// returns should exit
func translateParticipantError(g *gin.Context, err error) bool {
	if err == nil {
		return false
	}
	var unauthError *cqrs.UnauthorizedError
	var chatStillNotExistsError *cqrs.ChatStillNotExistsError
	if errors.As(err, &unauthError) {
		g.JSON(http.StatusUnauthorized, &dto.ErrorMessageDto{unauthError.Error()})
		return true
	} else if errors.As(err, &chatStillNotExistsError) {
		g.Status(http.StatusTeapot)
		return true
	}
	return false
}
