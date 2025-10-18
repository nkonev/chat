package services

import (
	"context"
	"fmt"
	"go-cqrs-chat-example/config"
	"go-cqrs-chat-example/cqrs"
	"go-cqrs-chat-example/db"
	"go-cqrs-chat-example/dto"
	"go-cqrs-chat-example/logger"
	"go-cqrs-chat-example/preview"
	"go-cqrs-chat-example/producer"
	"go-cqrs-chat-example/sanitizer"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"
)

type AsyncMessageService struct {
	lgr                          *logger.LoggerWrapper
	tr                           trace.Tracer
	rabbitmqOutputEventPublisher *producer.RabbitInternalEventsPublisher
}

func NewAsyncMessageService(
	lgr *logger.LoggerWrapper,
	rabbitmqEventPublisher *producer.RabbitInternalEventsPublisher,
) *AsyncMessageService {
	tr := otel.Tracer("event")

	return &AsyncMessageService{
		lgr:                          lgr,
		tr:                           tr,
		rabbitmqOutputEventPublisher: rabbitmqEventPublisher,
	}
}
func (p *AsyncMessageService) BroadcastMessage(ctx context.Context, messageText string, chatId, userId int64, userLogin string) {
	err := p.rabbitmqOutputEventPublisher.Publish(ctx, dto.PublishBroadcastMessage{
		MessageText: messageText,
		ChatId:      chatId,
		UserId:      userId,
		UserLogin:   userLogin,
	})
	if err != nil {
		p.lgr.ErrorContext(ctx, "Error during sending to rabbitmq", "err", err)
	}
}

func (p *AsyncMessageService) TypeMessage(ctx context.Context, chatId, userId int64, userLogin string) {
	err := p.rabbitmqOutputEventPublisher.Publish(ctx, dto.PublishUserTyping{
		ChatId:    chatId,
		UserId:    userId,
		UserLogin: userLogin,
	})
	if err != nil {
		p.lgr.ErrorContext(ctx, "Error during sending to rabbitmq", "err", err)
	}
}

type MessageService struct {
	lgr                          *logger.LoggerWrapper
	dbWrapper                    *db.DB
	commonProjection             *cqrs.CommonProjection
	stripAllTags                 *sanitizer.StripTagsPolicy
	cfg                          *config.AppConfig
	tr                           trace.Tracer
	rabbitmqOutputEventPublisher *producer.RabbitOutputEventsPublisher
}

func NewMessageService(
	lgr *logger.LoggerWrapper,
	dbWrapper *db.DB,
	commonProjection *cqrs.CommonProjection,
	stripAllTags *sanitizer.StripTagsPolicy,
	cfg *config.AppConfig,
	rabbitmqEventPublisher *producer.RabbitOutputEventsPublisher,
) *MessageService {
	tr := otel.Tracer("event")

	return &MessageService{
		lgr:                          lgr,
		dbWrapper:                    dbWrapper,
		commonProjection:             commonProjection,
		stripAllTags:                 stripAllTags,
		cfg:                          cfg,
		tr:                           tr,
		rabbitmqOutputEventPublisher: rabbitmqEventPublisher,
	}
}

func (p *MessageService) BroadcastMessage(ctx context.Context, messageText string, chatId, userId int64, userLogin string) {
	previewStr := preview.CreateMessagePreview(p.stripAllTags, p.cfg.Message.PreviewMaxTextSize, messageText, userLogin)
	if previewStr == preview.LoginPrefix(userLogin) {
		previewStr = ""
	}

	eventType := dto.EventTypeMessageBroadCast
	ctx, messageSpan := p.tr.Start(ctx, fmt.Sprintf("chat.%s", eventType))
	defer messageSpan.End()

	ut := dto.MessageBroadcastNotification{
		Login:  userLogin,
		UserId: userId,
		Text:   previewStr,
	}

	err := p.commonProjection.IterateOverChatParticipantIds(ctx, p.dbWrapper, chatId, []int64{}, func(participantIds []int64) error {
		for _, participantId := range participantIds {
			err := p.rabbitmqOutputEventPublisher.Publish(ctx, nil, dto.ChatEvent{
				EventType:                    eventType,
				MessageBroadcastNotification: &ut,
				UserId:                       participantId,
				ChatId:                       chatId,
			})
			if err != nil {
				p.lgr.ErrorContext(ctx, "Error during sending to rabbitmq", "err", err)
			}
		}
		return nil
	})
	if err != nil {
		p.lgr.ErrorContext(ctx, "Error during getting chat participants", "err", err)
		return
	}
}

func (p *MessageService) TypeMessage(ctx context.Context, chatId, userId int64, userLogin string) {
	eventType := dto.EventTypeMessageType
	ctx, messageSpan := p.tr.Start(ctx, fmt.Sprintf("chat.%s", eventType))
	defer messageSpan.End()

	ut := dto.UserTypingNotification{
		Login:         userLogin,
		ParticipantId: userId,
		ChatId:        chatId,
	}

	err := p.commonProjection.IterateOverChatParticipantIds(ctx, p.dbWrapper, chatId, []int64{userId}, func(participantIds []int64) error {
		for _, participantId := range participantIds {
			err := p.rabbitmqOutputEventPublisher.Publish(ctx, nil, dto.GlobalUserEvent{
				UserId:                 participantId,
				EventType:              eventType,
				UserTypingNotification: &ut,
			})
			if err != nil {
				p.lgr.ErrorContext(ctx, "Error during sending to rabbitmq", "err", err)
			}
		}
		return nil
	})
	if err != nil {
		p.lgr.ErrorContext(ctx, "Error during getting chat participants", "err", err)
		return
	}
}

func (p *MessageService) CreatePreview(messageText, userLogin string) string {
	input := preview.LoginPrefix(userLogin) + messageText
	return preview.CreateMessagePreviewWithoutLogin(p.stripAllTags, p.cfg.Message.PreviewMaxTextSize, input)
}
