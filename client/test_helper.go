package client

import (
	"context"
	"go-cqrs-chat-example/dto"
	"go-cqrs-chat-example/utils"
	"net/url"
)

func (rc *RestClient) CreateChat(ctx context.Context, behalfUserId int64, chatName string) (int64, error) {
	req := dto.ChatCreateDto{
		Title: chatName,
	}
	resp, err := query[dto.ChatCreateDto, dto.IdResponse](ctx, rc, behalfUserId, "POST", "/chat", "chat.Create", &req, nil)
	if err != nil {
		return 0, err
	}
	return resp.Id, nil
}

func (rc *RestClient) EditChat(ctx context.Context, chatId int64, chatName string, blog bool) error {
	req := dto.ChatEditDto{
		Id: chatId,
		ChatCreateDto: dto.ChatCreateDto{
			Title: chatName,
		},
		Blog: blog,
	}
	err := queryNoResponse[dto.ChatEditDto](ctx, rc, 0, "PUT", "/chat", "chat.Edit", &req)
	if err != nil {
		return err
	}
	return nil
}

func (rc *RestClient) PinChat(ctx context.Context, behalfUserId int64, chatId int64, pin bool) error {
	return queryNoResponse[any](ctx, rc, behalfUserId, "PUT", "/chat/"+utils.ToString(chatId)+"/pin?pin="+utils.ToString(pin), "chat.Pin", nil)
}

func (rc *RestClient) DeleteChat(ctx context.Context, chatId int64) error {
	return queryNoResponse[any](ctx, rc, 0, "DELETE", "/chat/"+utils.ToString(chatId), "chat.Delete", nil)
}

func (rc *RestClient) GetChatsByUserId(ctx context.Context, behalfUserId int64, queryParams *url.Values) ([]dto.ChatViewDto, error) {
	return query[any, []dto.ChatViewDto](ctx, rc, behalfUserId, "GET", "/chat/search", "chat.Search", nil, queryParams)
}

func (rc *RestClient) SearchBlogs(ctx context.Context) ([]dto.BlogViewDto, error) {
	return query[any, []dto.BlogViewDto](ctx, rc, 0, "GET", "/blog/search", "blog.Search", nil, nil)
}

func (rc *RestClient) CreateMessage(ctx context.Context, behalfUserId int64, chatId int64, text string) (int64, error) {
	req := dto.MessageCreateDto{
		Content: text,
	}
	resp, err := query[dto.MessageCreateDto, dto.IdResponse](ctx, rc, behalfUserId, "POST", "/chat/"+utils.ToString(chatId)+"/message", "message.Create", &req, nil)
	if err != nil {
		return 0, err
	}
	return resp.Id, nil
}

func (rc *RestClient) EditMessage(ctx context.Context, behalfUserId int64, chatId, messageId int64, text string) error {
	req := dto.MessageEditDto{
		Id: messageId,
		MessageCreateDto: dto.MessageCreateDto{
			Content: text,
		},
	}
	return queryNoResponse[dto.MessageEditDto](ctx, rc, behalfUserId, "PUT", "/chat/"+utils.ToString(chatId)+"/message", "message.Edit", &req)
}

func (rc *RestClient) DeleteMessage(ctx context.Context, behalfUserId int64, chatId, messageId int64) error {
	return queryNoResponse[any](ctx, rc, behalfUserId, "DELETE", "/chat/"+utils.ToString(chatId)+"/message/"+utils.ToString(messageId), "message.Delete", nil)
}

func (rc *RestClient) GetMessages(ctx context.Context, behalfUserId int64, chatId int64, queryParams *url.Values) ([]dto.MessageViewDto, error) {
	return query[any, []dto.MessageViewDto](ctx, rc, behalfUserId, "GET", "/chat/"+utils.ToString(chatId)+"/message/search", "message.Search", nil, queryParams)
}

func (rc *RestClient) MakeMessageBlogPost(ctx context.Context, chatId, messageId int64) error {
	return queryNoResponse[any](ctx, rc, 0, "PUT", "/chat/"+utils.ToString(chatId)+"/message/"+utils.ToString(messageId)+"/blog-post", "message.MakeBlogPost", nil)
}

func (rc *RestClient) SearchBlogComments(ctx context.Context, blogId int64) ([]dto.CommentViewDto, error) {
	return query[any, []dto.CommentViewDto](ctx, rc, 0, "GET", "/blog/"+utils.ToString(blogId)+"/comment/search", "blog.SearchComments", nil, nil)
}

func (rc *RestClient) AddChatParticipants(ctx context.Context, chatId int64, participantIds []int64) error {
	req := dto.ParticipantAddDto{
		ParticipantIds: participantIds,
	}
	return queryNoResponse[dto.ParticipantAddDto](ctx, rc, 0, "PUT", "/chat/"+utils.ToString(chatId)+"/participant", "participants.Add", &req)
}

func (rc *RestClient) DeleteChatParticipants(ctx context.Context, chatId int64, participantIds []int64) error {
	req := dto.ParticipantDeleteDto{
		ParticipantIds: participantIds,
	}
	return queryNoResponse[dto.ParticipantDeleteDto](ctx, rc, 0, "DELETE", "/chat/"+utils.ToString(chatId)+"/participant", "participants.Delete", &req)
}

func (rc *RestClient) GetChatParticipants(ctx context.Context, chatId int64) ([]int64, error) {
	return query[any, []int64](ctx, rc, 0, "GET", "/chat/"+utils.ToString(chatId)+"/participants", "participants.Get", nil, nil)
}

func (rc *RestClient) ReadMessage(ctx context.Context, behalfUserId int64, chatId, messageId int64) error {
	return queryNoResponse[any](ctx, rc, behalfUserId, "PUT", "/chat/"+utils.ToString(chatId)+"/message/"+utils.ToString(messageId)+"/read", "message.Read", nil)
}

func (rc *RestClient) HealthCheck(ctx context.Context) error {
	return queryNoResponse[any](ctx, rc, 0, "GET", "/internal/health", "internal.HealthCheck", nil)
}
