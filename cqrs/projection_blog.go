package cqrs

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"github.com/georgysavva/scany/v2/sqlscan"
	"go-cqrs-chat-example/db"
	"go-cqrs-chat-example/dto"
	"go-cqrs-chat-example/utils"
	"time"
)

func (m *CommonProjection) refreshBlog(ctx context.Context, tx *db.Tx, chatId int64, createdTime time.Time) error {
	_, errInner := tx.ExecContext(ctx, `
				with blog_message as (
					select m.* from message m where m.chat_id = $1 and m.blog_post = true
				)	
				insert into blog(id, owner_id, title, post, preview, create_date_time)
				select 
				    cast ($1 as bigint), 
				    (select m.owner_id from blog_message m),
				    (select c.title from chat_common c where c.id = $1),
				    (select m.content from blog_message m),
				    (select left(strip_tags(m.content), $2) from blog_message m),
					$3
				on conflict(id) do update set 
					owner_id = excluded.owner_id
					, title = excluded.title
					, post = excluded.post
					, preview = excluded.preview
			`, chatId, m.blogViewConfig.MaxTextPreviewSize, createdTime)
	if errInner != nil {
		return errInner
	}
	return nil
}

func (m *CommonProjection) OnMessageBlogPostMade(ctx context.Context, event *MessageBlogPostMade) error {
	errOuter := db.Transact(ctx, m.db, func(tx *db.Tx) error {
		admin, err := m.IsChatAdmin(ctx, tx, event.BehalfUserId, event.ChatId)
		if err != nil {
			return err
		}
		if !admin {
			m.lgr.InfoContext(ctx,
				"Participant isn't admin so he cannon make message blog post",
				"user_id", event.BehalfUserId,
				"chat_id", event.ChatId,
			)
			return nil
		}

		chatExists, errInner := m.checkChatExists(ctx, tx, event.ChatId)
		if errInner != nil {
			return errInner
		}
		if !chatExists {
			m.lgr.InfoContext(ctx, "Skipping MessageBlogPostMade because there is no chat", "chat_id", event.ChatId)
			return nil
		}

		messageExists, errInner := m.checkMessageExists(ctx, tx, event.ChatId, event.MessageId)
		if errInner != nil {
			return errInner
		}
		if !messageExists {
			m.lgr.InfoContext(ctx, "Skipping MessageBlogPostMade because there is no message", "chat_id", event.ChatId, "message_id", event.MessageId)
			return nil
		}

		// unset previous
		_, errInner = tx.ExecContext(ctx, "update message set blog_post = false where chat_id = $1 and id = (select id from message where chat_id = $1 and blog_post = true)", event.ChatId)
		if errInner != nil {
			return errInner
		}

		_, errInner = tx.ExecContext(ctx, "update message set blog_post = $3 where chat_id = $1 and id = $2", event.ChatId, event.MessageId, event.BlogPost)
		if errInner != nil {
			return errInner
		}

		// TODO think how to "unblog"
		errInner = m.refreshBlog(ctx, tx, event.ChatId, event.AdditionalData.CreatedAt)
		if errInner != nil {
			return errInner
		}
		return nil
	})

	return errOuter
}

func (m *CommonProjection) isChatBlog(ctx context.Context, co db.CommonOperations, chatId int64) (bool, error) {
	var blog bool
	err := sqlscan.Get(ctx, co, &blog, "select exists(select * from chat_common where id = $1 and blog = true)", chatId)
	if err != nil {
		return false, err
	}
	return blog, nil
}

func (m *CommonProjection) isMessageBlogPost(ctx context.Context, co db.CommonOperations, chatId, messageId int64) (bool, error) {
	var blog bool
	err := sqlscan.Get(ctx, co, &blog, "select exists(select * from message where chat_id = $1 and id = $2 and blog_post = true order by id desc limit 1)", chatId, messageId)
	if err != nil {
		return false, err
	}
	return blog, nil
}

func (m *EnrichingProjection) GetBlogsEnriched(ctx context.Context, size int32, offset int64, reverseOrder bool) ([]dto.BlogViewEnrichedDto, error) {
	blogs, err := m.cp.GetBlogs(ctx, size, offset, reverseOrder)
	if err != nil {
		m.lgr.ErrorContext(ctx, "Error getting blogs", "err", err)
		return nil, err
	}

	userIds := getUserIdsFromBlogs(blogs)
	users, err := m.aaaRestClient.GetUsers(ctx, userIds)
	if err != nil {
		m.lgr.WarnContext(ctx, "unable to get users")
	}
	blogsEnriched := enrichBlogs(blogs, utils.ToMap(users))
	return blogsEnriched, nil
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

func enrichBlogs(blogs []dto.BlogViewDto, users map[int64]*dto.User) []dto.BlogViewEnrichedDto {
	res := make([]dto.BlogViewEnrichedDto, 0, len(blogs))
	for _, ch := range blogs {
		var u *dto.User
		if ch.OwnerId != nil {
			u = users[*ch.OwnerId]
		}
		che := dto.BlogViewEnrichedDto{
			BlogViewDto: ch,
			Owner:       u,
		}
		res = append(res, che)
	}
	return res
}

func (m *CommonProjection) GetBlogs(ctx context.Context, size int32, offset int64, reverseOrder bool) ([]dto.BlogViewDto, error) {
	ma := []dto.BlogViewDto{}

	order := "asc"
	if reverseOrder {
		order = "desc"
	}

	err := sqlscan.Select(ctx, m.db, &ma, fmt.Sprintf(`
		select 
		    b.id,
			b.owner_id,
		    b.title,
		    b.preview,
		    b.create_date_time
		from blog b
		order by b.create_date_time %s
		limit $1 offset $2
	`, order), size, offset)
	if err != nil {
		return ma, err
	}
	return ma, nil
}

func (m *EnrichingProjection) GetBlogEnriched(ctx context.Context, blogId int64) (*dto.BlogEnrichedDto, error) {
	blog, err := m.cp.GetBlog(ctx, blogId)
	if err != nil {
		m.lgr.ErrorContext(ctx, "Error getting blog", "err", err)
		return nil, err
	}

	if blog == nil {
		return nil, nil
	}

	userIds := getUserIdsFromBlog(blog)
	users, err := m.aaaRestClient.GetUsers(ctx, userIds)
	if err != nil {
		m.lgr.WarnContext(ctx, "unable to get users")
	}
	blogEnriched := enrichBlog(blog, utils.ToMap(users))
	return blogEnriched, nil
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

func enrichBlog(blog *dto.BlogDto, users map[int64]*dto.User) *dto.BlogEnrichedDto {
	if blog == nil {
		return nil
	}

	var u *dto.User
	ownerIdP := blog.OwnerId
	if ownerIdP != nil {
		u = users[*ownerIdP]
	}

	return &dto.BlogEnrichedDto{
		BlogDto: *blog,
		Owner:   u,
	}
}

func (m *CommonProjection) GetBlog(ctx context.Context, blogId int64) (*dto.BlogDto, error) {
	var res *dto.BlogDto
	err := sqlscan.Get(ctx, m.db, &res, `
		select 
		    b.id,
			b.owner_id,
		    b.title,
		    b.post,
		    b.create_date_time
		from blog b
		where b.id = $1
		order by b.create_date_time desc 
	`, blogId)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			// there were no rows, but otherwise no error occurred
			return nil, nil
		}
		return nil, err
	}

	return res, nil
}

func (m *CommonProjection) getBlogPostMessageId(ctx context.Context, co db.CommonOperations, blogId int64) (int64, error) {
	var messageId int64
	err := sqlscan.Get(ctx, co, &messageId, "select id from message where chat_id = $1 and blog_post = true order by id desc limit 1", blogId)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			// there were no rows, but otherwise no error occurred
			return 0, nil
		}
		return 0, err
	}
	return messageId, nil
}

func (m *CommonProjection) getComments(ctx context.Context, co db.CommonOperations, blogId, postMessageId int64, size int32, offset int64, reverseOrder bool) ([]dto.CommentViewDto, error) {
	ma := []dto.CommentViewDto{}

	order := "asc"
	if reverseOrder {
		order = "desc"
	}

	err := sqlscan.Select(ctx, co, &ma, fmt.Sprintf(`
		select 
		    id, 
		    owner_id,
		    content, 
		    create_date_time,
		    update_date_time
		from message 
		where chat_id = $1 and id > $2
		order by id %s
		limit $3 offset $4
	`, order), blogId, postMessageId, size, offset)

	if err != nil {
		return ma, err
	}

	return ma, nil
}

func (m *EnrichingProjection) GetCommentsEnriched(ctx context.Context, blogId int64, size int32, offset int64, reverseOrder bool) ([]dto.CommentViewEnrichedDto, error) {
	comments, err := m.cp.GetComments(ctx, blogId, size, offset, reverseOrder)
	if err != nil {
		m.lgr.ErrorContext(ctx, "Error getting blog comments", "err", err)
		return nil, err
	}

	userIds := getUserIdsFromComments(comments)
	users, err := m.aaaRestClient.GetUsers(ctx, userIds)
	if err != nil {
		m.lgr.WarnContext(ctx, "unable to get users")
	}
	commentsEnriched := enrichComments(comments, utils.ToMap(users))
	return commentsEnriched, nil
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

func enrichComments(comments []dto.CommentViewDto, users map[int64]*dto.User) []dto.CommentViewEnrichedDto {
	res := make([]dto.CommentViewEnrichedDto, 0, len(comments))
	for _, m := range comments {
		me := dto.CommentViewEnrichedDto{
			CommentViewDto: m,
			Owner:          users[m.OwnerId],
		}
		res = append(res, me)
	}
	return res
}

func (m *CommonProjection) GetComments(ctx context.Context, blogId int64, size int32, offset int64, reverseOrder bool) ([]dto.CommentViewDto, error) {
	res, errOuter := db.TransactWithResult(ctx, m.db, func(tx *db.Tx) ([]dto.CommentViewDto, error) {
		postMessageId, err := m.getBlogPostMessageId(ctx, tx, blogId)
		if err != nil {
			return []dto.CommentViewDto{}, err
		}
		comments, err := m.getComments(ctx, tx, blogId, postMessageId, size, offset, reverseOrder)
		if err != nil {
			return []dto.CommentViewDto{}, err
		}
		return comments, nil
	})
	if errOuter != nil {
		return []dto.CommentViewDto{}, errOuter
	}
	return res, nil
}
