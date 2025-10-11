package listener

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/streadway/amqp"
	"go-cqrs-chat-example/dto"
	"go-cqrs-chat-example/logger"
	"go-cqrs-chat-example/rabbitmq"
	"go-cqrs-chat-example/services"
	"go-cqrs-chat-example/type_registry"
	"go.opentelemetry.io/otel"
)

type InternalEventsListener func(*amqp.Delivery) error

func CreateInternalEventsListener(
	lgr *logger.LoggerWrapper,
	typeRegistry *type_registry.TypeRegistryInstance,
	messageService *services.MessageService,
) InternalEventsListener {
	tr := otel.Tracer("amqp/listener")

	return func(msg *amqp.Delivery) error {
		ctx := rabbitmq.ExtractAMQPHeaders(context.Background(), msg.Headers)
		ctx, span := tr.Start(ctx, "internal.event.listener")
		defer span.End()

		bytesData := msg.Body
		strData := string(bytesData)
		aType := msg.Type
		lgr.DebugContext(ctx, "Received", "data", strData, "type", aType)

		if !typeRegistry.HasType(aType) {
			lgr.ErrorContext(ctx, "Unexpected type in rabbit internal_listener", "type", aType)
			return nil
		}

		anInstance := typeRegistry.MakeInstance(aType)

		switch bindTo := anInstance.(type) {
		case dto.PublishBroadcastMessage:
			err := json.Unmarshal(bytesData, &bindTo)
			if err != nil {
				lgr.ErrorContext(ctx, "Error during deserialize notification", "err", err)
				return err
			}

			messageService.BroadcastMessage(ctx, bindTo.MessageText, bindTo.ChatId, bindTo.UserId, bindTo.UserLogin)
		case dto.PublishUserTyping:
			err := json.Unmarshal(bytesData, &bindTo)
			if err != nil {
				lgr.ErrorContext(ctx, "Error during deserialize notification", "err", err)
				return err
			}

			messageService.TypeMessage(ctx, bindTo.ChatId, bindTo.UserId, bindTo.UserLogin)
		default:
			lgr.ErrorContext(ctx, "Unexpected type:", "instance", anInstance)
			return errors.New(fmt.Sprintf("Unexpected type : %v", anInstance))
		}

		return nil
	}
}
