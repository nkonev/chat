package client

import (
	"context"
	"go-cqrs-chat-example/dto"
	"go-cqrs-chat-example/utils"
	"net/url"
)

func (rc *RestClient) GetUsers(ctx context.Context, userIds []int64) ([]dto.User, error) {
	queryParams := url.Values{}
	for _, u := range userIds {
		queryParams.Add("userId", utils.ToString(u))
	}
	resp, err := query[any, []dto.User](ctx, rc, 0, "GET", rc.cfg.AaaConfig.AaaUrlConfig.GetUsers, "user.Get", nil, &queryParams)
	if err != nil {
		return []dto.User{}, err
	}
	return resp, nil
}
