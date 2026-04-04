package cmd

import (
	"go-cqrs-chat-example/app"
	"go-cqrs-chat-example/client"
	"go-cqrs-chat-example/config"
	"go-cqrs-chat-example/cqrs"
	"go-cqrs-chat-example/db"
	"go-cqrs-chat-example/kafka"
	"go-cqrs-chat-example/logger"
	"go-cqrs-chat-example/otel"
	"go-cqrs-chat-example/producer"
	"go-cqrs-chat-example/rabbitmq"
	"go-cqrs-chat-example/sanitizer"
	"go-cqrs-chat-example/services"
	"go-cqrs-chat-example/type_registry"
	"log/slog"
	"os"

	"go.uber.org/fx"
	"go.uber.org/fx/fxevent"
)

const CommandMigrateName = "migrate"

func RunMigrate(args []string) {
	cfg, err := config.CreateTypedConfig(args)
	if err != nil {
		panic(err)
	}
	lgr := logger.NewLogger(os.Stdout, cfg)
	defer lgr.CloseLogger()

	lgr.Info("Start migrate command")

	appFx := fx.New(
		fx.Supply(cfg),
		fx.Supply(lgr),
		fx.WithLogger(func(lgr *logger.LoggerWrapper) fxevent.Logger {
			fsl := &fxevent.SlogLogger{Logger: lgr.Logger}
			fsl.UseLogLevel(slog.LevelDebug)
			return fsl
		}),
		fx.Provide(
			otel.ConfigureTracePropagator,
			otel.ConfigureTraceProvider,
			otel.ConfigureTraceExporter,
			cqrs.NewKotelTracer,
			cqrs.NewKotel,
			db.ConfigureDatabase,
			kafka.ConfigureKafkaAdmin,
			cqrs.ConfigurePublisher,
			cqrs.ConfigureCommonProjection,
			cqrs.NewEnrichingProjection,
			client.NewAAARestClient,
			sanitizer.CreateSanitizer,
			sanitizer.CreateStripTags,
			sanitizer.CreateStripSource,
			services.NewAuthorizationService,
			services.NewMessageService,
			services.NewAsyncMessageService,
			services.NewInputEventHandler,
			producer.NewRabbitOutputEventsPublisher,
			producer.NewRabbitNotificationEventsPublisher,
			producer.NewRabbitInternalEventsPublisher,
			rabbitmq.CreateRabbitMqConnection,
			type_registry.NewTypeRegistryInstance,
		),
		fx.Invoke(
			db.RunMigrations,
			kafka.RunCreateTopicChat,
			kafka.RunCreateTopicUser,
			cqrs.RunMigrateFromOldDb,
			app.Shutdown,
		),
	)
	appFx.Run()
	lgr.Info("Exit migrate command")
}
