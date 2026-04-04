package services

import (
	"context"
	"fmt"
	"go-cqrs-chat-example/cqrs"
	"go-cqrs-chat-example/db"
	"go-cqrs-chat-example/dto"
	"go-cqrs-chat-example/logger"
	"go-cqrs-chat-example/producer"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"
)

type InputEventHandler struct {
	commonProjection             *cqrs.CommonProjection
	dba                          *db.DB
	lgr                          *logger.LoggerWrapper
	tr                           trace.Tracer
	rabbitmqOutputEventPublisher *producer.RabbitOutputEventsPublisher
}

func NewInputEventHandler(
	commonProjection *cqrs.CommonProjection,
	dba *db.DB,
	lgr *logger.LoggerWrapper,
	rabbitmqEventPublisher *producer.RabbitOutputEventsPublisher,
) *InputEventHandler {
	tr := otel.Tracer("event")

	return &InputEventHandler{
		commonProjection:             commonProjection,
		dba:                          dba,
		lgr:                          lgr,
		tr:                           tr,
		rabbitmqOutputEventPublisher: rabbitmqEventPublisher,
	}
}

func (not InputEventHandler) NotifyAboutProfileChanged(ctx context.Context, user *dto.User) {
	if user == nil {
		not.lgr.ErrorContext(ctx, "user cannot be null")
		return
	}

	eventType := dto.EventTypeParticipantChanged
	ctx, messageSpan := not.tr.Start(ctx, fmt.Sprintf("global.user.%s", eventType))
	defer messageSpan.End()

	err := not.commonProjection.IterateOverCoChattedParticipantIds(ctx, not.dba, user.Id, func(participantIds []int64) error {
		var internalErr error
		for _, participantId := range participantIds {
			internalErr = not.rabbitmqOutputEventPublisher.Publish(ctx, nil, dto.GlobalUserEvent{
				UserId:                           participantId,
				EventType:                        eventType,
				CoChattedParticipantNotification: user,
			})
		}
		return internalErr
	})
	if err != nil {
		not.lgr.ErrorContext(ctx, "Error during sending to rabbitmq", logger.AttributeError, err)
	}
}
