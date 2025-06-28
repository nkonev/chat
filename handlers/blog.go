package handlers

import (
	"github.com/gin-gonic/gin"
	"go-cqrs-chat-example/cqrs"
	"go-cqrs-chat-example/db"
	"go-cqrs-chat-example/logger"
	"go-cqrs-chat-example/utils"
	"net/http"
)

type BlogHandler struct {
	lgr              *logger.LoggerWrapper
	eventBus         *cqrs.PartitionAwareEventBus
	dbWrapper        *db.DB
	commonProjection *cqrs.CommonProjection
}

func NewBlogHandler(
	lgr *logger.LoggerWrapper,
	eventBus *cqrs.PartitionAwareEventBus,
	dbWrapper *db.DB,
	commonProjection *cqrs.CommonProjection,
) *BlogHandler {
	return &BlogHandler{
		lgr:              lgr,
		eventBus:         eventBus,
		dbWrapper:        dbWrapper,
		commonProjection: commonProjection,
	}
}

func (ch *BlogHandler) SearchBlogs(g *gin.Context) {
	page := utils.FixPageString(g.Query(PageParam))
	size := utils.FixSizeString(g.Query(SizeParam))
	offset := utils.GetOffset(page, size)
	reverse := utils.GetBooleanOr(g.Query(ReverseParam), true)

	blogs, err := ch.commonProjection.GetBlogs(g.Request.Context(), size, offset, reverse)
	if err != nil {
		ch.lgr.WithTrace(g.Request.Context()).Error("Error getting blogs", "err", err)
		g.Status(http.StatusInternalServerError)
		return
	}
	g.JSON(http.StatusOK, blogs)
}

func (ch *BlogHandler) GetBlog(g *gin.Context) {
	cid := g.Param(BlogIdParam)

	blogId, err := utils.ParseInt64(cid)
	if err != nil {
		ch.lgr.WithTrace(g.Request.Context()).Error("Error binding blogId", "err", err)
		g.Status(http.StatusInternalServerError)
		return
	}

	blog, err := ch.commonProjection.GetBlog(g.Request.Context(), blogId)
	if err != nil {
		ch.lgr.WithTrace(g.Request.Context()).Error("Error getting blog", "err", err)
		g.Status(http.StatusInternalServerError)
		return
	}

	if blog == nil {
		g.Status(http.StatusNoContent)
		return
	}

	g.JSON(http.StatusOK, blog)
}

func (ch *BlogHandler) SearchComments(g *gin.Context) {
	cid := g.Param(BlogIdParam)
	blogId, err := utils.ParseInt64(cid)
	if err != nil {
		ch.lgr.WithTrace(g.Request.Context()).Error("Error binding blogId", "err", err)
		g.Status(http.StatusInternalServerError)
		return
	}

	page := utils.FixPageString(g.Query(PageParam))
	size := utils.FixSizeString(g.Query(SizeParam))
	offset := utils.GetOffset(page, size)
	reverse := utils.GetBooleanOr(g.Query(ReverseParam), false)

	comments, err := ch.commonProjection.GetComments(g.Request.Context(), blogId, size, offset, reverse)
	if err != nil {
		ch.lgr.WithTrace(g.Request.Context()).Error("Error getting blog comments", "err", err)
		g.Status(http.StatusInternalServerError)
		return
	}
	g.JSON(http.StatusOK, comments)
}
