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

type aaaRestClient struct {
	restClient
}

type AaaRestClient interface {
	GetUsers(ctx context.Context, userIds []int64) ([]*dto.User, error)
}

func NewAAARestClient(cfg *config.AppConfig, lgr *logger.LoggerWrapper) AaaRestClient {
	tr := &http.Transport{
		MaxIdleConns:       cfg.Http.MaxIdleConns,
		IdleConnTimeout:    cfg.Http.IdleConnTimeout,
		DisableCompression: cfg.Http.DisableCompression,
	}
	tr.TLSClientConfig = &tls.Config{InsecureSkipVerify: true}
	trR := otelhttp.NewTransport(tr)
	client := &http.Client{Transport: trR}
	trcr := otel.Tracer("rest/client")

	return &aaaRestClient{restClient{client, cfg.Aaa.Url.Base, trcr, cfg, lgr, "[aaa client]"}}
}

func (rc *aaaRestClient) GetUsers(ctx context.Context, userIds []int64) ([]*dto.User, error) {
	if len(userIds) == 0 {
		return []*dto.User{}, nil
	}

	queryParams := url.Values{}
	for _, u := range userIds {
		queryParams.Add("userId", utils.ToString(u))
	}
	resp, err := query[any, []*dto.User](ctx, &rc.restClient, 0, http.MethodGet, rc.cfg.Aaa.Url.GetUsers, "user.Get", nil, &queryParams)
	if err != nil {
		return []*dto.User{}, err
	}
	return resp, nil
}
