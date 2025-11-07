package cmd

import (
	"go-cqrs-chat-example/client"
	"go-cqrs-chat-example/config"
	"go-cqrs-chat-example/cqrs"
	"go-cqrs-chat-example/db"
	"go-cqrs-chat-example/handlers"
	"go-cqrs-chat-example/kafka"
	"go-cqrs-chat-example/listener"
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

const CommandServeName = "serve"

func RunServe(args []string) {
	cfg, err := config.CreateTypedConfig(args)
	if err != nil {
		panic(err)
	}
	lgr := logger.NewLogger(os.Stdout, cfg)
	defer lgr.CloseLogger()

	lgr.Info("Start serve command")

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
			db.ConfigureDatabase,
			kafka.ConfigureKafkaAdmin,
			cqrs.ConfigureKafkaMarshaller,
			cqrs.ConfigureWatermillLogger,
			cqrs.ConfigurePublisher,
			cqrs.ConfigureCqrsRouter,
			cqrs.ConfigureCqrsMarshaller,
			cqrs.ConfigureEventBus,
			cqrs.ConfigureEventProcessor,
			cqrs.ConfigureCommonProjection,
			cqrs.NewEnrichingProjection,
			handlers.NewChatHandler,
			handlers.NewParticipantHandler,
			handlers.NewMessageHandler,
			handlers.NewBlogHandler,
			handlers.NewTechnicalHandler,
			handlers.CreateHttpRouter,
			handlers.ConfigureHttpServer,
			kafka.ConfigureSaramaClient,
			client.NewAAARestClient,
			sanitizer.CreateSanitizer,
			sanitizer.CreateStripTags,
			services.NewAuthorizationService,
			services.NewMessageService,
			services.NewAsyncMessageService,
			services.NewInputEventHandler,
			producer.NewRabbitOutputEventsPublisher,
			producer.NewRabbitInternalEventsPublisher,
			rabbitmq.CreateRabbitMqConnection,
			cqrs.NewEventHandler,
			listener.CreateInternalEventsListener,
			listener.CreateAaaUserProfileUpdateListener,
			type_registry.NewTypeRegistryInstance,
		),
		fx.Invoke(
			db.RunMigrations,
			listener.CreateAndListenInternalEventsChannel,
			listener.CreateAndListenAaaChannel,
			kafka.RunCreateTopic,
			cqrs.RunCqrsRouter,
			kafka.WaitForAllEventsProcessed,
			cqrs.RunSequenceFastforwarder,
			producer.EnableOutputEvents,
			handlers.RunHttpServer,
		),
	)
	appFx.Run()
	lgr.Info("Exit serve command")
}
