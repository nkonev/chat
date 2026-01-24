package cmd

import (
	"fmt"
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
	"go-cqrs-chat-example/type_registry"
	"log/slog"
	"os"

	"go.uber.org/fx"
	"go.uber.org/fx/fxevent"
)

const CommandRewindName = "rewind"

func RunRewind(args []string) {
	if app.IsHelp(args) {
		fmt.Println(`
Consumes all the events from the Kafka events topic and processes
the 'need_to_fast_forward_sequences' and 'need_to_fast_forward_sequences' tasks
in "technical" PostgreSQL table.
		`)

		return
	}

	cfg, err := config.CreateTypedConfig(args)
	if err != nil {
		panic(err)
	}
	lgr := logger.NewLogger(os.Stdout, cfg)
	defer lgr.CloseLogger()

	lgr.Info("Start rewind command")

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
			kafka.ConfigureSaramaClient,
			client.NewAAARestClient,
			sanitizer.CreateSanitizer,
			sanitizer.CreateStripTags,
			sanitizer.CreateStripSource,
			producer.NewRabbitOutputEventsPublisher,
			producer.NewRabbitNotificationEventsPublisher,
			rabbitmq.CreateRabbitMqConnection,
			cqrs.NewEventHandler,
			type_registry.NewTypeRegistryInstance,
		),
		fx.Invoke(
			db.RunMigrations,
			kafka.RunCreateTopicChat,
			kafka.RunCreateTopicUser,
			cqrs.RunMigrateFromOldDb,
			cqrs.RunCqrsRouter,
			kafka.WaitForAllEventsProcessedChat,
			kafka.WaitForAllEventsProcessedUser,
			cqrs.RunSequenceFastforwarder,
			app.Shutdown,
		),
	)
	appFx.Run()
	lgr.Info("Exit rewind command")
}
