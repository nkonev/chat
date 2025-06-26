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

//go:generate go get go.uber.org/mock/mockgen@v0.5.2
//go:generate go run go.uber.org/mock/mockgen -source=aaa.go -destination mock/lock.go
//go:generate go mod tidy
type AaaRestClient interface {
	GetUsers(ctx context.Context, userIds []int64) ([]dto.User, error)
}

func NewAAARestClient(cfg *config.AppConfig, lgr *logger.LoggerWrapper) AaaRestClient {
	tr := &http.Transport{
		MaxIdleConns:       cfg.RestClientConfig.MaxIdleConns,
		IdleConnTimeout:    cfg.RestClientConfig.IdleConnTimeout,
		DisableCompression: cfg.RestClientConfig.DisableCompression,
	}
	tr.TLSClientConfig = &tls.Config{InsecureSkipVerify: true}
	trR := otelhttp.NewTransport(tr)
	client := &http.Client{Transport: trR}
	trcr := otel.Tracer("rest/client")

	return &aaaRestClient{restClient{client, cfg.AaaConfig.AaaUrlConfig.Base, trcr, cfg, lgr}}
}

func (rc *aaaRestClient) GetUsers(ctx context.Context, userIds []int64) ([]dto.User, error) {
	queryParams := url.Values{}
	for _, u := range userIds {
		queryParams.Add("userId", utils.ToString(u))
	}
	resp, err := query[any, []dto.User](ctx, &rc.restClient, 0, "GET", rc.cfg.AaaConfig.AaaUrlConfig.GetUsers, "user.Get", nil, &queryParams)
	if err != nil {
		return []dto.User{}, err
	}
	return resp, nil
}
