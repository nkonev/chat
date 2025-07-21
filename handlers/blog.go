package handlers

import (
	"github.com/gin-gonic/gin"
	"go-cqrs-chat-example/client"
	"go-cqrs-chat-example/cqrs"
	"go-cqrs-chat-example/db"
	"go-cqrs-chat-example/dto"
	"go-cqrs-chat-example/logger"
	"go-cqrs-chat-example/utils"
	"net/http"
)

type BlogHandler struct {
	lgr              *logger.LoggerWrapper
	eventBus         *cqrs.PartitionAwareEventBus
	dbWrapper        *db.DB
	commonProjection *cqrs.CommonProjection
	aaaRestClient    client.AaaRestClient
}

func NewBlogHandler(
	lgr *logger.LoggerWrapper,
	eventBus *cqrs.PartitionAwareEventBus,
	dbWrapper *db.DB,
	commonProjection *cqrs.CommonProjection,
	restClient client.AaaRestClient,
) *BlogHandler {
	return &BlogHandler{
		lgr:              lgr,
		eventBus:         eventBus,
		dbWrapper:        dbWrapper,
		commonProjection: commonProjection,
		aaaRestClient:    restClient,
	}
}

func (ch *BlogHandler) SearchBlogs(g *gin.Context) {
	page := utils.FixPageString(g.Query(dto.PageParam))
	size := utils.FixSizeString(g.Query(dto.SizeParam))
	offset := utils.GetOffset(page, size)
	reverse := utils.GetBooleanOr(g.Query(dto.ReverseParam), true)

	blogs, err := ch.commonProjection.GetBlogs(g.Request.Context(), size, offset, reverse)
	if err != nil {
		ch.lgr.ErrorContext(g.Request.Context(), "Error getting blogs", "err", err)
		g.Status(http.StatusInternalServerError)
		return
	}

	userIds := getUserIdsFromBlogs(blogs)
	users, err := ch.aaaRestClient.GetUsers(g.Request.Context(), userIds)
	if err != nil {
		ch.lgr.WarnContext(g.Request.Context(), "unable to get users")
	}
	blogsEnriched := enrichBlogs(blogs, users)

	g.JSON(http.StatusOK, blogsEnriched)
}

func getUserIdsFromBlogs(chats []dto.BlogViewDto) []int64 {
	m := map[int64]struct{}{}

	for _, ch := range chats {
		if ch.OwnerId != nil {
			m[*ch.OwnerId] = struct{}{}
		}
	}

	r := []int64{}

	for k, _ := range m {
		r = append(r, k)
	}
	return r
}

func enrichBlogs(blogs []dto.BlogViewDto, users []dto.User) []dto.BlogViewEnrichedDto {
	res := make([]dto.BlogViewEnrichedDto, 0, len(blogs))
	for _, ch := range blogs {
		var u *dto.User
		if ch.OwnerId != nil {
			u = findUserById(users, *ch.OwnerId)
		}
		che := dto.BlogViewEnrichedDto{
			BlogViewDto: ch,
			Owner:       u,
		}
		res = append(res, che)
	}
	return res
}

func (ch *BlogHandler) GetBlog(g *gin.Context) {
	cid := g.Param(dto.BlogIdParam)

	blogId, err := utils.ParseInt64(cid)
	if err != nil {
		ch.lgr.ErrorContext(g.Request.Context(), "Error binding blogId", "err", err)
		g.Status(http.StatusInternalServerError)
		return
	}

	blog, err := ch.commonProjection.GetBlog(g.Request.Context(), blogId)
	if err != nil {
		ch.lgr.ErrorContext(g.Request.Context(), "Error getting blog", "err", err)
		g.Status(http.StatusInternalServerError)
		return
	}

	if blog == nil {
		g.Status(http.StatusNoContent)
		return
	}

	userIds := getUserIdsFromBlog(blog)
	users, err := ch.aaaRestClient.GetUsers(g.Request.Context(), userIds)
	if err != nil {
		ch.lgr.WarnContext(g.Request.Context(), "unable to get users")
	}
	blogEnriched := enrichBlog(blog, users)

	g.JSON(http.StatusOK, blogEnriched)
}

func getUserIdsFromBlog(blog *dto.BlogDto) []int64 {
	ret := []int64{}
	if blog == nil {
		return ret
	}
	ownerIdP := blog.OwnerId
	if ownerIdP != nil {
		ret = append(ret, *ownerIdP)
	}
	return ret
}

func enrichBlog(blog *dto.BlogDto, users []dto.User) *dto.BlogEnrichedDto {
	if blog == nil {
		return nil
	}

	var u *dto.User
	ownerIdP := blog.OwnerId
	if ownerIdP != nil {
		u = findUserById(users, *ownerIdP)
	}

	return &dto.BlogEnrichedDto{
		BlogDto: *blog,
		Owner:   u,
	}
}

func (ch *BlogHandler) SearchComments(g *gin.Context) {
	cid := g.Param(dto.BlogIdParam)
	blogId, err := utils.ParseInt64(cid)
	if err != nil {
		ch.lgr.ErrorContext(g.Request.Context(), "Error binding blogId", "err", err)
		g.Status(http.StatusInternalServerError)
		return
	}

	page := utils.FixPageString(g.Query(dto.PageParam))
	size := utils.FixSizeString(g.Query(dto.SizeParam))
	offset := utils.GetOffset(page, size)
	reverse := utils.GetBooleanOr(g.Query(dto.ReverseParam), false)

	comments, err := ch.commonProjection.GetComments(g.Request.Context(), blogId, size, offset, reverse)
	if err != nil {
		ch.lgr.ErrorContext(g.Request.Context(), "Error getting blog comments", "err", err)
		g.Status(http.StatusInternalServerError)
		return
	}

	userIds := getUserIdsFromComments(comments)
	users, err := ch.aaaRestClient.GetUsers(g.Request.Context(), userIds)
	if err != nil {
		ch.lgr.WarnContext(g.Request.Context(), "unable to get users")
	}
	commentsEnriched := enrichComments(comments, users)

	g.JSON(http.StatusOK, commentsEnriched)
}

func getUserIdsFromComments(comments []dto.CommentViewDto) []int64 {
	m := map[int64]struct{}{}

	for _, msg := range comments {
		m[msg.OwnerId] = struct{}{}
	}

	r := []int64{}

	for k, _ := range m {
		r = append(r, k)
	}
	return r
}

func enrichComments(comments []dto.CommentViewDto, users []dto.User) []dto.CommentViewEnrichedDto {
	res := make([]dto.CommentViewEnrichedDto, 0, len(comments))
	for _, m := range comments {
		me := dto.CommentViewEnrichedDto{
			CommentViewDto: m,
			Owner:          findUserById(users, m.OwnerId),
		}
		res = append(res, me)
	}
	return res
}
