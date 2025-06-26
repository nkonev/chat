package cmd

import (
	"context"
	"go-cqrs-chat-example/app"
	"go-cqrs-chat-example/client"
	"go-cqrs-chat-example/config"
	"go-cqrs-chat-example/cqrs"
	"go-cqrs-chat-example/db"
	"go-cqrs-chat-example/kafka"
	"go-cqrs-chat-example/logger"
	"go-cqrs-chat-example/otel"
	"os"
	"testing"

	"github.com/IBM/sarama"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/fx"
	"go.uber.org/fx/fxevent"
)

func TestReset(t *testing.T) {
	cfg, err := config.CreateTestTypedConfig()
	if err != nil {
		panic(err)
	}
	baseLogger := logger.NewBaseLogger(os.Stdout, cfg)
	lgr := logger.NewLogger(baseLogger)

	const user1 int64 = 1
	const chat1Name = "new chat 1"
	const message1Text = "new message 1"

	var message1Id int64
	var chat1Id int64

	resetInfra(lgr, cfg)

	// fill with 1 chat and 1 message
	runTestFunc(lgr, cfg, t, func(
		lgr *logger.LoggerWrapper,
		cfg *config.AppConfig,
		restClient *client.TestRestClient,
		saramaClient sarama.Client,
		m *cqrs.CommonProjection,
		lc fx.Lifecycle,
	) {
		ctx := context.Background()

		var err error
		chat1Id, err = restClient.CreateChat(ctx, user1, chat1Name)
		require.NoError(t, err, "error in creating chat")
		assert.True(t, chat1Id > 0)
		require.NoError(t, kafka.WaitForAllEventsProcessed(lgr, cfg, saramaClient, lc), "error in waiting for processing events")

		message1Id, err = restClient.CreateMessage(ctx, user1, chat1Id, message1Text)
		require.NoError(t, err, "error in creating message")

		require.NoError(t, kafka.WaitForAllEventsProcessed(lgr, cfg, saramaClient, lc), "error in waiting for processing events")

		user1Chats, err := restClient.GetChatsByUserId(ctx, user1, nil)
		require.NoError(t, err, "error in getting chats")
		assert.Equal(t, 1, len(user1Chats))
		chat1OfUser1 := user1Chats[0]
		assert.Equal(t, chat1Name, chat1OfUser1.Title)
		assert.Equal(t, int64(0), chat1OfUser1.UnreadMessages)

		chat1Participants, err := restClient.GetChatParticipants(ctx, chat1Id)
		require.NoError(t, err, "error in char participants")
		assert.Equal(t, []int64{user1}, chat1Participants)

		chat1Messages, err := restClient.GetMessages(ctx, user1, chat1Id, nil)
		require.NoError(t, err, "error in getting messages")
		assert.Equal(t, 1, len(chat1Messages))
		message1 := chat1Messages[0]
		assert.Equal(t, message1Id, message1.Id)
		assert.Equal(t, message1Text, message1.Content)
	})

	lgr.Info("Start reset command")
	appExportFx := fx.New(
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
			cqrs.ConfigureCommonProjection,
		),
		fx.Invoke(
			db.RunResetDatabase,
			kafka.RunResetPartitions,
			db.RunMigrations,
			kafka.RunCreateTopic,
			cqrs.SetIsNeedToFastForwardSequences,
			app.Shutdown,
		),
	)
	appExportFx.Run()
	lgr.Info("Exit reset command")

	// normal run after reset
	runTestFunc(lgr, cfg, t, func(
		lgr *logger.LoggerWrapper,
		cfg *config.AppConfig,
		restClient *client.TestRestClient,
		saramaClient sarama.Client,
		m *cqrs.CommonProjection,
		lc fx.Lifecycle,
	) {
		ctx := context.Background()

		require.NoError(t, kafka.WaitForAllEventsProcessed(lgr, cfg, saramaClient, lc), "error in waiting for processing events")

		user1Chats, err := restClient.GetChatsByUserId(ctx, user1, nil)
		require.NoError(t, err, "error in getting chats")
		assert.Equal(t, 1, len(user1Chats))
		chat1OfUser1 := user1Chats[0]
		assert.Equal(t, chat1Name, chat1OfUser1.Title)
		assert.Equal(t, int64(0), chat1OfUser1.UnreadMessages)

		chat1Participants, err := restClient.GetChatParticipants(ctx, chat1Id)
		require.NoError(t, err, "error in char participants")
		assert.Equal(t, []int64{user1}, chat1Participants)

		chat1Messages, err := restClient.GetMessages(ctx, user1, chat1Id, nil)
		require.NoError(t, err, "error in getting messages")
		assert.Equal(t, 1, len(chat1Messages))
		message1 := chat1Messages[0]
		assert.Equal(t, message1Id, message1.Id)
		assert.Equal(t, message1Text, message1.Content)
	})
}
