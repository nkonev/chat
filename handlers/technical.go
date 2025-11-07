package handlers

import (
	"github.com/gin-gonic/gin"
	"go-cqrs-chat-example/config"
	"go-cqrs-chat-example/cqrs"
	"go-cqrs-chat-example/db"
	"go-cqrs-chat-example/logger"
	"net/http"
)

type TechnicalHandler struct {
	lgr              *logger.LoggerWrapper
	eventBus         *cqrs.PartitionAwareEventBus
	dbWrapper        *db.DB
	commonProjection *cqrs.CommonProjection
	cfg              *config.AppConfig
}

func NewTechnicalHandler(
	lgr *logger.LoggerWrapper,
	eventBus *cqrs.PartitionAwareEventBus,
	dbWrapper *db.DB,
	commonProjection *cqrs.CommonProjection,
	cfg *config.AppConfig,
) *TechnicalHandler {
	return &TechnicalHandler{
		lgr:              lgr,
		eventBus:         eventBus,
		dbWrapper:        dbWrapper,
		commonProjection: commonProjection,
		cfg:              cfg,
	}
}

func (ch *TechnicalHandler) Health(g *gin.Context) {
	g.Status(http.StatusOK)
}

func (ch *TechnicalHandler) Truncate(g *gin.Context) {
	cc := cqrs.Truncate{}

	err := cc.Handle(g.Request.Context(), ch.eventBus, ch.dbWrapper, ch.commonProjection, ch.lgr, ch.cfg)
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
