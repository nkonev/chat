package cmd

import (
	"context"
	"github.com/stretchr/testify/mock"
	"go-cqrs-chat-example/app"
	"go-cqrs-chat-example/client"
	"go-cqrs-chat-example/config"
	"go-cqrs-chat-example/cqrs"
	"go-cqrs-chat-example/db"
	"go-cqrs-chat-example/dto"
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

func TestImport(t *testing.T) {
	cfg, err := config.CreateTestTypedConfig()
	if err != nil {
		panic(err)
	}
	baseLogger := logger.NewBaseLogger(os.Stdout, cfg)
	lgr := logger.NewLogger(baseLogger)

	const user1 int64 = 1
	const user1Login = "admin"
	const chat1Name = "new chat 1"
	const message1Text = "new message 1"

	var message1Id int64
	var chat1Id int64

	mockUser1 := dto.User{
		Id:               user1,
		Login:            user1Login,
		Avatar:           nil,
		ShortInfo:        nil,
		LoginColor:       nil,
		LastSeenDateTime: nil,
		AdditionalData:   nil,
	}

	resetInfra(lgr, cfg)

	// fill with 1 chat and 1 message
	runTestFunc(lgr, cfg, t, func(
		lgr *logger.LoggerWrapper,
		cfg *config.AppConfig,
		testRestClient *client.TestRestClient,
		saramaClient sarama.Client,
		m *cqrs.CommonProjection,
		aaaRestClient client.AaaRestClient,
		lc fx.Lifecycle,
	) {
		mockAaaClient := aaaRestClient.(*client.MockAaaRestClient)
		mockAaaClient.EXPECT().GetUsers(mock.Anything, []int64{user1}).Return([]*dto.User{&mockUser1}, nil)

		ctx := context.Background()

		var err error
		chat1Id, err = testRestClient.CreateChat(ctx, user1, chat1Name)
		require.NoError(t, err, "error in creating chat")
		assert.True(t, chat1Id > 0)
		require.NoError(t, kafka.WaitForAllEventsProcessed(lgr, cfg, saramaClient, lc), "error in waiting for processing events")

		message1Id, err = testRestClient.CreateMessage(ctx, user1, chat1Id, message1Text)
		require.NoError(t, err, "error in creating message")

		require.NoError(t, kafka.WaitForAllEventsProcessed(lgr, cfg, saramaClient, lc), "error in waiting for processing events")

		user1Chats, err := testRestClient.GetChats(ctx, user1)
		require.NoError(t, err, "error in getting chats")
		assert.Equal(t, 1, len(user1Chats))
		chat1OfUser1 := user1Chats[0]
		assert.Equal(t, chat1Name, chat1OfUser1.Title)
		assert.Equal(t, int64(0), chat1OfUser1.UnreadMessages)

		chat1Participants, err := testRestClient.GetChatParticipants(ctx, chat1Id)
		require.NoError(t, err, "error in char participants")
		require.Equal(t, 1, len(chat1Participants))
		assert.Equal(t, user1, chat1Participants[0].Id)
		assert.Equal(t, user1Login, chat1Participants[0].Login)

		chat1Messages, err := testRestClient.GetMessages(ctx, user1, chat1Id)
		require.NoError(t, err, "error in getting messages")
		assert.Equal(t, 1, len(chat1Messages))
		message1 := chat1Messages[0]
		assert.Equal(t, message1Id, message1.Id)
		assert.Equal(t, message1Text, message1.Content)
	})

	lgr.Info("Start export command")
	appExportFx := fx.New(
		fx.Supply(cfg),
		fx.Supply(lgr),
		fx.WithLogger(func(lgr *logger.LoggerWrapper) fxevent.Logger {
			return &fxevent.SlogLogger{Logger: lgr.Logger}
		}),
		fx.Provide(
			kafka.ConfigureSaramaClient,
		),
		fx.Invoke(
			kafka.Export,
			app.Shutdown,
		),
	)
	appExportFx.Run()
	lgr.Info("Exit export command")

	resetInfra(lgr, cfg)

	lgr.Info("Start import command")
	appImportFx := fx.New(
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
			db.RunMigrations,
			kafka.RunCreateTopic,
			kafka.Import,
			cqrs.SetIsNeedToFastForwardSequences,
			app.Shutdown,
		),
	)
	appImportFx.Run()
	lgr.Info("Exit import command")

	runTestFunc(lgr, cfg, t, func(
		lgr *logger.LoggerWrapper,
		cfg *config.AppConfig,
		testRestClient *client.TestRestClient,
		saramaClient sarama.Client,
		m *cqrs.CommonProjection,
		aaaRestClient client.AaaRestClient,
		lc fx.Lifecycle,
	) {
		mockAaaClient := aaaRestClient.(*client.MockAaaRestClient)
		mockAaaClient.EXPECT().GetUsers(mock.Anything, []int64{user1}).Return([]*dto.User{&mockUser1}, nil)

		ctx := context.Background()

		require.NoError(t, kafka.WaitForAllEventsProcessed(lgr, cfg, saramaClient, lc), "error in waiting for processing events")

		user1Chats, err := testRestClient.GetChats(ctx, user1)
		require.NoError(t, err, "error in getting chats")
		assert.Equal(t, 1, len(user1Chats))
		chat1OfUser1 := user1Chats[0]
		assert.Equal(t, chat1Name, chat1OfUser1.Title)
		assert.Equal(t, int64(0), chat1OfUser1.UnreadMessages)

		chat1Participants, err := testRestClient.GetChatParticipants(ctx, chat1Id)
		require.NoError(t, err, "error in char participants")
		require.Equal(t, 1, len(chat1Participants))
		assert.Equal(t, user1, chat1Participants[0].Id)
		assert.Equal(t, user1Login, chat1Participants[0].Login)

		chat1Messages, err := testRestClient.GetMessages(ctx, user1, chat1Id)
		require.NoError(t, err, "error in getting messages")
		assert.Equal(t, 1, len(chat1Messages))
		message1 := chat1Messages[0]
		assert.Equal(t, message1Id, message1.Id)
		assert.Equal(t, message1Text, message1.Content)
	})
}
