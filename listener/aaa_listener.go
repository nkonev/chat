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

type AaaUserProfileUpdateListener func(*amqp.Delivery) error

func CreateAaaUserProfileUpdateListener(lgr *logger.LoggerWrapper, not *services.InputEventHandler, typeRegistry *type_registry.TypeRegistryInstance) AaaUserProfileUpdateListener {
	tr := otel.Tracer("amqp/listener")

	return func(msg *amqp.Delivery) error {
		ctx := rabbitmq.ExtractAMQPHeaders(context.Background(), msg.Headers)
		ctx, span := tr.Start(ctx, "aaa.listener")
		defer span.End()

		bytesData := msg.Body
		strData := string(bytesData)
		aType := msg.Type
		lgr.DebugContext(ctx, "Received", "data", strData, "type", aType)

		if !typeRegistry.HasType(aType) {
			lgr.ErrorContext(ctx, "Unexpected type in rabbit test_listener", "type", aType)
			return nil
		}

		anInstance := typeRegistry.MakeInstance(aType)

		switch bindTo := anInstance.(type) {
		case dto.UserAccountEventChanged:
			err := json.Unmarshal(bytesData, &bindTo)
			if err != nil {
				lgr.ErrorContext(ctx, "Error during deserialize notification", "err", err)
				return err
			}
			if bindTo.EventType == dto.EventTypeUserAccountChanged {
				not.NotifyAboutProfileChanged(ctx, bindTo.User)
			}

		default:
			lgr.ErrorContext(ctx, "Unexpected type:", "instance", anInstance)
			return errors.New(fmt.Sprintf("Unexpected type : %v", anInstance))
		}

		return nil
	}
}
