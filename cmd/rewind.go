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
	processedArgs, hasHelp := app.IsHelp(args)
	if hasHelp {
		fmt.Printf(`
Consumes all the events from the Kafka events topic
hereby (re)building PostgreSQL projections
and processes the 'need_to_fast_forward_sequences' task
in "technical" PostgreSQL table.
Then exits.

`)

		return
	}

	cfg, err := config.CreateTypedConfig(processedArgs)
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
			producer.NewRabbitOutputEventsPublisher,
			producer.NewRabbitNotificationEventsPublisher,
			rabbitmq.CreateRabbitMqConnection,
			cqrs.NewEventHandler,
			type_registry.NewTypeRegistryInstance,
			cqrs.NewKafkaListener,
		),
		fx.Invoke(
			db.RunMigrations,
			kafka.RunCreateTopicChat,
			kafka.RunCreateTopicUser,
			cqrs.ListenChatTopic,
			cqrs.ListenUserTopic,
			kafka.WaitForAllEventsProcessedChat,
			kafka.WaitForAllEventsProcessedUser,
			cqrs.RunSequenceFastforwarder,
			app.Shutdown,
		),
	)
	appFx.Run()
	lgr.Info("Exit rewind command")
}
