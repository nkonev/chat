package client

import (
	"context"
	"crypto/tls"
	"go-cqrs-chat-example/config"
	"go-cqrs-chat-example/dto"
	"go-cqrs-chat-example/logger"
	"go-cqrs-chat-example/utils"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"go.opentelemetry.io/otel"
	"net/http"
	"net/url"
)

type TestRestClient struct {
	restClient
}

func NewTestRestClient(cfg *config.AppConfig, lgr *logger.LoggerWrapper) *TestRestClient {
	tr := &http.Transport{
		MaxIdleConns:       cfg.RestClientConfig.MaxIdleConns,
		IdleConnTimeout:    cfg.RestClientConfig.IdleConnTimeout,
		DisableCompression: cfg.RestClientConfig.DisableCompression,
	}
	tr.TLSClientConfig = &tls.Config{InsecureSkipVerify: true}
	trR := otelhttp.NewTransport(tr)
	client := &http.Client{Transport: trR}
	trcr := otel.Tracer("test/rest/client")

	return &TestRestClient{restClient{client, "http://localhost" + cfg.HttpServerConfig.Address, trcr, cfg, lgr, "[test http client]"}}
}

func (rc *TestRestClient) CreateChat(ctx context.Context, behalfUserId int64, chatName string) (int64, error) {
	req := dto.ChatCreateDto{
		Title: chatName,
	}
	resp, err := query[dto.ChatCreateDto, dto.IdResponse](ctx, &rc.restClient, behalfUserId, "POST", "/chat", "chat.Create", &req, nil)
	if err != nil {
		return 0, err
	}
	return resp.Id, nil
}

func (rc *TestRestClient) EditChat(ctx context.Context, behalfUserId int64, chatId int64, chatName string, blog bool) error {
	req := dto.ChatEditDto{
		Id: chatId,
		ChatCreateDto: dto.ChatCreateDto{
			Title: chatName,
		},
		Blog: blog,
	}
	err := queryNoResponse[dto.ChatEditDto](ctx, &rc.restClient, behalfUserId, "PUT", "/chat", "chat.Edit", &req, nil)
	if err != nil {
		return err
	}
	return nil
}

func (rc *TestRestClient) PinChat(ctx context.Context, behalfUserId int64, chatId int64, pin bool) error {
	return queryNoResponse[any](ctx, &rc.restClient, behalfUserId, "PUT", "/chat/"+utils.ToString(chatId)+"/pin?pin="+utils.ToString(pin), "chat.Pin", nil, nil)
}

func (rc *TestRestClient) DeleteChat(ctx context.Context, behalfUserId int64, chatId int64) error {
	return queryNoResponse[any](ctx, &rc.restClient, behalfUserId, "DELETE", "/chat/"+utils.ToString(chatId), "chat.Delete", nil, nil)
}

func (rc *TestRestClient) GetChatsByUserId(ctx context.Context, behalfUserId int64, queryParams *url.Values) ([]dto.ChatViewDto, error) {
	return query[any, []dto.ChatViewDto](ctx, &rc.restClient, behalfUserId, "GET", "/chat/search", "chat.Search", nil, queryParams)
}

func (rc *TestRestClient) SearchBlogs(ctx context.Context) ([]dto.BlogViewDto, error) {
	return query[any, []dto.BlogViewDto](ctx, &rc.restClient, 0, "GET", "/blog/search", "blog.Search", nil, nil)
}

func (rc *TestRestClient) CreateMessage(ctx context.Context, behalfUserId int64, chatId int64, text string) (int64, error) {
	req := dto.MessageCreateDto{
		Content: text,
	}
	resp, err := query[dto.MessageCreateDto, dto.IdResponse](ctx, &rc.restClient, behalfUserId, "POST", "/chat/"+utils.ToString(chatId)+"/message", "message.Create", &req, nil)
	if err != nil {
		return 0, err
	}
	return resp.Id, nil
}

func (rc *TestRestClient) EditMessage(ctx context.Context, behalfUserId int64, chatId, messageId int64, text string) error {
	req := dto.MessageEditDto{
		Id: messageId,
		MessageCreateDto: dto.MessageCreateDto{
			Content: text,
		},
	}
	return queryNoResponse[dto.MessageEditDto](ctx, &rc.restClient, behalfUserId, "PUT", "/chat/"+utils.ToString(chatId)+"/message", "message.Edit", &req, nil)
}

func (rc *TestRestClient) DeleteMessage(ctx context.Context, behalfUserId int64, chatId, messageId int64) error {
	return queryNoResponse[any](ctx, &rc.restClient, behalfUserId, "DELETE", "/chat/"+utils.ToString(chatId)+"/message/"+utils.ToString(messageId), "message.Delete", nil, nil)
}

func (rc *TestRestClient) GetMessages(ctx context.Context, behalfUserId int64, chatId int64, queryParams *url.Values) ([]dto.MessageViewEnrichedDto, error) {
	return query[any, []dto.MessageViewEnrichedDto](ctx, &rc.restClient, behalfUserId, "GET", "/chat/"+utils.ToString(chatId)+"/message/search", "message.Search", nil, queryParams)
}

func (rc *TestRestClient) MakeMessageBlogPost(ctx context.Context, behalfUserId int64, chatId, messageId int64) error {
	return queryNoResponse[any](ctx, &rc.restClient, behalfUserId, "PUT", "/chat/"+utils.ToString(chatId)+"/message/"+utils.ToString(messageId)+"/blog-post", "message.MakeBlogPost", nil, nil)
}

func (rc *TestRestClient) SearchBlogComments(ctx context.Context, blogId int64) ([]dto.CommentViewDto, error) {
	return query[any, []dto.CommentViewDto](ctx, &rc.restClient, 0, "GET", "/blog/"+utils.ToString(blogId)+"/comment/search", "blog.SearchComments", nil, nil)
}

// You must await after this command, because it takes a time to apply "ParticipantAdd" event
func (rc *TestRestClient) AddChatParticipants(ctx context.Context, behalfUserId int64, chatId int64, participantIds []int64) error {
	req := dto.ParticipantAddDto{
		ParticipantIds: participantIds,
	}
	return queryNoResponse[dto.ParticipantAddDto](ctx, &rc.restClient, behalfUserId, "PUT", "/chat/"+utils.ToString(chatId)+"/participant", "participants.Add", &req, nil)
}

func (rc *TestRestClient) DeleteChatParticipants(ctx context.Context, behalfUserId int64, chatId int64, participantIds []int64) error {
	req := dto.ParticipantDeleteDto{
		ParticipantIds: participantIds,
	}
	return queryNoResponse[dto.ParticipantDeleteDto](ctx, &rc.restClient, behalfUserId, "DELETE", "/chat/"+utils.ToString(chatId)+"/participant", "participants.Delete", &req, nil)
}

func (rc *TestRestClient) ChangeChatParticipant(ctx context.Context, behalfUserId int64, chatId int64, participantId int64, newAdmin bool) error {
	query1 := url.Values{
		dto.AdminParam: []string{utils.ToString(newAdmin)},
	}
	return queryNoResponse[any](ctx, &rc.restClient, behalfUserId, "PUT", "/chat/"+utils.ToString(chatId)+"/participant/"+utils.ToString(participantId), "participants.Change", nil, &query1)
}

func (rc *TestRestClient) GetChatParticipants(ctx context.Context, chatId int64) ([]dto.UserWithAdmin, error) {
	return query[any, []dto.UserWithAdmin](ctx, &rc.restClient, 0, "GET", "/chat/"+utils.ToString(chatId)+"/participants", "participants.Get", nil, nil)
}

func (rc *TestRestClient) ReadMessage(ctx context.Context, behalfUserId int64, chatId, messageId int64) error {
	return queryNoResponse[any](ctx, &rc.restClient, behalfUserId, "PUT", "/chat/"+utils.ToString(chatId)+"/message/"+utils.ToString(messageId)+"/read", "message.Read", nil, nil)
}

func (rc *TestRestClient) HealthCheck(ctx context.Context) error {
	return queryNoResponse[any](ctx, &rc.restClient, 0, "GET", "/internal/health", "internal.HealthCheck", nil, nil)
}
