package services

import (
	"context"
	"go-cqrs-chat-example/db"
	"go-cqrs-chat-example/dto"
)

type InputEventHandler struct {
}

func NewInputEventHandler() *InputEventHandler {
	return &InputEventHandler{}
}

func (h InputEventHandler) NotifyAboutProfileChanged(ctx context.Context, user *dto.User, db *db.DB) {

}
