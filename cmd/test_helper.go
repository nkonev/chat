package cmd

import (
	"context"
	"go-cqrs-chat-example/app"
	"go-cqrs-chat-example/client"
	mock_client "go-cqrs-chat-example/client/mock"
	"go-cqrs-chat-example/config"
	"go-cqrs-chat-example/cqrs"
	"go-cqrs-chat-example/db"
	"go-cqrs-chat-example/handlers"
	"go-cqrs-chat-example/kafka"
	"go-cqrs-chat-example/logger"
	"go-cqrs-chat-example/otel"
	"go.uber.org/mock/gomock"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"go.uber.org/fx"
	"go.uber.org/fx/fxevent"
	"go.uber.org/fx/fxtest"
)

func TestMain(m *testing.M) {
	setup()
	retCode := m.Run()
	shutdown()
	os.Exit(retCode)
}

func setup() {

}

func shutdown() {

}

func resetInfra(lgr *logger.LoggerWrapper, cfg *config.AppConfig) {
	appFx := fx.New(
		fx.Supply(cfg),
		fx.Supply(lgr),
		fx.WithLogger(func(lgr *logger.LoggerWrapper) fxevent.Logger {
			return &fxevent.SlogLogger{Logger: lgr.Logger}
		}),
		fx.Provide(
			otel.ConfigureTracePropagator,
			otel.ConfigureTraceProvider,
			otel.ConfigureTraceExporter,
			db.ConfigureDatabase,
			kafka.ConfigureKafkaAdmin,
		),
		fx.Invoke(
			db.RunResetDatabase,
			kafka.RunDeleteTopic,
			db.RunMigrations,
			kafka.RunCreateTopic,
			app.Shutdown,
		),
	)
	appFx.Run()
}

func aaaClientFactory(ctrl *gomock.Controller) func() client.AaaRestClient {
	return func() client.AaaRestClient {
		return mock_client.NewMockAaaRestClient(ctrl)
	}
}

func runTestFunc(lgr *logger.LoggerWrapper, cfg *config.AppConfig, t *testing.T, testFunc interface{}) {
	var s fx.Shutdowner

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	appTestFx := fxtest.New(
		t,
		fx.Supply(cfg),
		fx.Supply(lgr),
		fx.WithLogger(func(lgr *logger.LoggerWrapper) fxevent.Logger {
			return &fxevent.SlogLogger{Logger: lgr.Logger}
		}),
		fx.Populate(&s),
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
			handlers.NewChatHandler,
			handlers.NewParticipantHandler,
			handlers.NewMessageHandler,
			handlers.NewBlogHandler,
			handlers.ConfigureHttpServer,
			kafka.ConfigureSaramaClient,
			client.NewTestRestClient,
			aaaClientFactory(ctrl),
		),
		fx.Invoke(
			cqrs.RunCqrsRouter,
			handlers.RunHttpServer,
			waitForHealthCheck,
			testFunc,
		),
	)
	defer appTestFx.RequireStart().RequireStop()
	assert.NoError(t, s.Shutdown(), "error in app shutdown")
}

func startAppFull(t *testing.T, testFunc interface{}) {
	cfg, err := config.CreateTestTypedConfig()
	if err != nil {
		panic(err)
	}
	baseLogger := logger.NewBaseLogger(os.Stdout, cfg)
	lgr := logger.NewLogger(baseLogger)

	resetInfra(lgr, cfg)

	runTestFunc(lgr, cfg, t, testFunc)
}

func waitForHealthCheck(lgr *logger.LoggerWrapper, restClient *client.TestRestClient, cfg *config.AppConfig) {
	ctx := context.Background()

	i := 0
	const maxAttempts = 60
	success := false
	for ; i <= maxAttempts; i++ {
		err := restClient.HealthCheck(ctx)
		if err != nil {
			lgr.Info("Awaiting while chat have been started")
			time.Sleep(time.Second * 1)
			continue
		} else {
			success = true
			break
		}
	}
	if !success {
		panic("Cannot await for chat will be started")
	}
	lgr.Info("chat have started")
}

func isChatExists(ctx context.Context, co db.CommonOperations, chatId int64) (bool, error) {
	r := co.QueryRowContext(ctx, "select exists(select * from chat_common where id = $1 limit 1)", chatId)
	var exists bool
	err := r.Scan(&exists)
	if err != nil {
		return false, err
	}
	return exists, nil
}

func waitForChatExists(lgr *logger.LoggerWrapper, dba *db.DB, chatId int64) {
	ctx := context.Background()

	i := 0
	const maxAttempts = 120
	success := false
	for ; i <= maxAttempts; i++ {
		exists, err := isChatExists(ctx, dba, chatId)
		if err != nil || !exists {
			lgr.Info("Awaiting while chat appear")
			time.Sleep(time.Second * 1)
			continue
		} else {
			success = true
			break
		}
	}
	if !success {
		panic("Cannot await for chat will appear")
	}
	lgr.Info("chat appeared")
}

func isMessageExists(ctx context.Context, co db.CommonOperations, chatId, messageId int64) (bool, error) {
	r := co.QueryRowContext(ctx, "select exists(select * from message where chat_id = $1 and id = $2 limit 1)", chatId, messageId)
	var exists bool
	err := r.Scan(&exists)
	if err != nil {
		return false, err
	}
	return exists, nil
}

func waitForMessageExists(lgr *logger.LoggerWrapper, dba *db.DB, chatId, messageId int64) {
	ctx := context.Background()

	i := 0
	const maxAttempts = 120
	success := false
	for ; i <= maxAttempts; i++ {
		exists, err := isMessageExists(ctx, dba, chatId, messageId)
		if err != nil || !exists {
			lgr.Info("Awaiting while message appear")
			time.Sleep(time.Second * 1)
			continue
		} else {
			success = true
			break
		}
	}
	if !success {
		panic("Cannot await for message will appear")
	}
	lgr.Info("message appeared")
}
