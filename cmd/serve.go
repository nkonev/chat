package cmd

import (
	"fmt"
	"go-cqrs-chat-example/app"
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
	"go-cqrs-chat-example/tasks"
	"go-cqrs-chat-example/type_registry"
	"log/slog"
	"os"

	"go.uber.org/fx"
	"go.uber.org/fx/fxevent"
)

const CommandServeName = "serve"

func RunServe(args []string) {
	if app.IsHelp(args) {
		fmt.Printf(`
Starts normal serving api requests.
Http server starts when all the events from the Kafka events topic were consumed and
the 'need_to_fast_forward_sequences' task
in "technical" PostgreSQL table is finished.
Also starts schedulers and RabbitMQ listeners.

To run with config:
./%s %s %s=/path/to/config.yaml

To run with override log level:
./%s %s --logger.level=debug

Or via environment variable:
CHAT_LOGGER_LEVEL=debug ./%s %s

To run with override log json:
./%s %s --logger.json=false

To run on the specific port:
./%s %s --server.address=:8888

To run without schedulers:
./%s %s --schedulers.cleanAbandonedChatsTask.enabled=false --schedulers.cleanDeletedUsersDataTask.enabled=false

`, ExecutableName, CommandServeName, app.ConfigLongPrefix,
			ExecutableName, CommandServeName,
			ExecutableName, CommandServeName,
			ExecutableName, CommandServeName,
			ExecutableName, CommandServeName,
			ExecutableName, CommandServeName,
		)

		return
	}

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
			sanitizer.CreateStripSource,
			services.NewAuthorizationService,
			services.NewMessageService,
			services.NewAsyncMessageService,
			services.NewInputEventHandler,
			producer.NewRabbitOutputEventsPublisher,
			producer.NewRabbitNotificationEventsPublisher,
			producer.NewRabbitInternalEventsPublisher,
			rabbitmq.CreateRabbitMqConnection,
			cqrs.NewEventHandler,
			listener.CreateRabbitInternalEventsListener,
			listener.CreateRabbitAaaUserProfileUpdateListener,
			type_registry.NewTypeRegistryInstance,
			tasks.RedisV9,
			tasks.RedisLocker,
			tasks.Scheduler,
			tasks.CleanAbandonedChatsScheduler,
			tasks.CleanDeletedUserDataScheduler,
			tasks.NewCleanAbandonedChatsService,
			tasks.NewCleanDeletedUserDataService,
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
			producer.EnableOutputEvents,
			listener.CreateAndListenInternalEventsChannel,
			listener.CreateAndListenAaaChannel,
			tasks.RunScheduler,
			handlers.RunHttpServer,
		),
	)
	appFx.Run()
	lgr.Info("Exit serve command")
}
