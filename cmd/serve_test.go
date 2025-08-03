package cmd

import (
	"context"
	"github.com/stretchr/testify/mock"
	"go-cqrs-chat-example/client"
	"go-cqrs-chat-example/config"
	"go-cqrs-chat-example/cqrs"
	"go-cqrs-chat-example/db"
	"go-cqrs-chat-example/dto"
	"go-cqrs-chat-example/kafka"
	"go-cqrs-chat-example/logger"
	"go-cqrs-chat-example/utils"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/IBM/sarama"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/fx"
)

func TestUnreads(t *testing.T) {
	startAppFull(t, func(
		lgr *logger.LoggerWrapper,
		cfg *config.AppConfig,
		testRestClient *client.TestRestClient,
		saramaClient sarama.Client,
		m *cqrs.CommonProjection,
		aaaRestClient client.AaaRestClient,
		lc fx.Lifecycle,
	) {
		const user1 int64 = 1
		const user2 int64 = 2
		const user3 int64 = 3
		const user1Login = "admin1"
		const user2Login = "admin2"
		const user3Login = "admin3"

		mockUser1 := dto.User{
			Id:               user1,
			Login:            user1Login,
			Avatar:           nil,
			ShortInfo:        nil,
			LoginColor:       nil,
			LastSeenDateTime: nil,
			AdditionalData:   nil,
		}

		mockUser2 := dto.User{
			Id:               user2,
			Login:            user2Login,
			Avatar:           nil,
			ShortInfo:        nil,
			LoginColor:       nil,
			LastSeenDateTime: nil,
			AdditionalData:   nil,
		}

		mockUser3 := dto.User{
			Id:               user3,
			Login:            user3Login,
			Avatar:           nil,
			ShortInfo:        nil,
			LoginColor:       nil,
			LastSeenDateTime: nil,
			AdditionalData:   nil,
		}

		const chat1Name = "new chat 1"

		mockAaaClient := aaaRestClient.(*client.MockAaaRestClient)
		mockAaaClient.EXPECT().GetUsers(mock.Anything, mock.Anything).Return([]*dto.User{&mockUser1, &mockUser2, &mockUser3}, nil)

		ctx := context.Background()

		chat1Id, err := testRestClient.CreateChat(ctx, user1, chat1Name)
		require.NoError(t, err, "error in creating chat")
		assert.True(t, chat1Id > 0)
		require.NoError(t, kafka.WaitForAllEventsProcessed(lgr, cfg, saramaClient, lc), "error in waiting for processing events")

		title, err := m.GetChatByUserIdAndChatId(ctx, user1, chat1Id)
		require.NoError(t, err, "error in getting chat")
		assert.Equal(t, chat1Name, title)

		const message1Text = "new message 1"

		message1Id, err := testRestClient.CreateMessage(ctx, user1, chat1Id, message1Text)
		require.NoError(t, err, "error in creating message")
		require.NoError(t, kafka.WaitForAllEventsProcessed(lgr, cfg, saramaClient, lc), "error in waiting for processing events")

		user1Chats, err := testRestClient.GetChatsByUserId(ctx, user1, nil)
		require.NoError(t, err, "error in getting chats")
		assert.Equal(t, 1, len(user1Chats))
		chat1OfUser1 := user1Chats[0]
		assert.Equal(t, chat1Name, chat1OfUser1.Title)
		assert.Equal(t, int64(0), chat1OfUser1.UnreadMessages)

		user2Chats, err := testRestClient.GetChatsByUserId(ctx, user2, nil)
		require.NoError(t, err, "error in getting chats")
		assert.Equal(t, 0, len(user2Chats))

		user3Chats, err := testRestClient.GetChatsByUserId(ctx, user3, nil)
		require.NoError(t, err, "error in getting chats")
		assert.Equal(t, 0, len(user3Chats))

		chat1Messages, err := testRestClient.GetMessages(ctx, user1, chat1Id, nil)
		require.NoError(t, err, "error in getting messages")
		assert.Equal(t, 1, len(chat1Messages))
		message1 := chat1Messages[0]
		assert.Equal(t, message1Id, message1.Id)
		assert.Equal(t, message1Text, message1.Content)

		// 2 separate calls to guarantee order
		err = testRestClient.AddChatParticipants(ctx, user1, chat1Id, []int64{user2})
		require.NoError(t, err, "error in adding participants")
		err = testRestClient.AddChatParticipants(ctx, user1, chat1Id, []int64{user3})
		require.NoError(t, err, "error in adding participants")
		require.NoError(t, kafka.WaitForAllEventsProcessed(lgr, cfg, saramaClient, lc), "error in waiting for processing events")

		chat1Participants, err := testRestClient.GetChatParticipants(ctx, chat1Id)
		require.NoError(t, err, "error in chat participants")
		require.Equal(t, 3, len(chat1Participants))
		assert.Equal(t, user3, chat1Participants[0].Id)
		assert.Equal(t, user3Login, chat1Participants[0].Login)
		assert.Equal(t, user2, chat1Participants[1].Id)
		assert.Equal(t, user2Login, chat1Participants[1].Login)
		assert.Equal(t, user1, chat1Participants[2].Id)
		assert.Equal(t, user1Login, chat1Participants[2].Login)

		user2ChatsNew, err := testRestClient.GetChatsByUserId(ctx, user2, nil)
		require.NoError(t, err, "error in getting chats")
		assert.Equal(t, 1, len(user2ChatsNew))
		chat1OfUser2 := user2ChatsNew[0]
		assert.Equal(t, chat1Name, chat1OfUser2.Title)
		assert.Equal(t, int64(1), chat1OfUser2.UnreadMessages)

		user3ChatsNew, err := testRestClient.GetChatsByUserId(ctx, user3, nil)
		require.NoError(t, err, "error in getting chats")
		assert.Equal(t, 1, len(user3ChatsNew))
		chat1OfUser3 := user3ChatsNew[0]
		assert.Equal(t, chat1Name, chat1OfUser3.Title)
		assert.Equal(t, int64(1), chat1OfUser3.UnreadMessages)

		err = testRestClient.ReadMessage(ctx, user2, chat1Id, message1.Id)
		require.NoError(t, err, "error in reading message")
		require.NoError(t, kafka.WaitForAllEventsProcessed(lgr, cfg, saramaClient, lc), "error in waiting for processing events")

		user2ChatsNew2, err := testRestClient.GetChatsByUserId(ctx, user2, nil)
		require.NoError(t, err, "error in getting chats")
		assert.Equal(t, 1, len(user2ChatsNew2))
		chat1OfUser22 := user2ChatsNew2[0]
		assert.Equal(t, int64(0), chat1OfUser22.UnreadMessages)

		user3ChatsNew2, err := testRestClient.GetChatsByUserId(ctx, user3, nil)
		require.NoError(t, err, "error in getting chats")
		assert.Equal(t, 1, len(user3ChatsNew2))
		chat1OfUser32 := user3ChatsNew2[0]
		assert.Equal(t, int64(1), chat1OfUser32.UnreadMessages)

		const message2Text = "new message 2"
		_, err = testRestClient.CreateMessage(ctx, user1, chat1Id, message2Text)
		require.NoError(t, err, "error in creating message")

		const message3Text = "new message 3"
		messageId3, err := testRestClient.CreateMessage(ctx, user1, chat1Id, message3Text)
		require.NoError(t, err, "error in creating message")
		assert.True(t, messageId3 > 0)
		require.NoError(t, kafka.WaitForAllEventsProcessed(lgr, cfg, saramaClient, lc), "error in waiting for processing events")

		user2ChatsNew3, err := testRestClient.GetChatsByUserId(ctx, user2, nil)
		require.NoError(t, err, "error in getting chats")
		assert.Equal(t, 1, len(user2ChatsNew3))
		chat1OfUser23 := user2ChatsNew3[0]
		assert.Equal(t, int64(2), chat1OfUser23.UnreadMessages)

		user3ChatsNew3, err := testRestClient.GetChatsByUserId(ctx, user3, nil)
		require.NoError(t, err, "error in getting chats")
		assert.Equal(t, 1, len(user3ChatsNew3))
		chat1OfUser33 := user3ChatsNew3[0]
		assert.Equal(t, int64(3), chat1OfUser33.UnreadMessages)

		err = testRestClient.DeleteMessage(ctx, user1, chat1Id, messageId3)
		require.NoError(t, err, "error in delete message")
		require.NoError(t, kafka.WaitForAllEventsProcessed(lgr, cfg, saramaClient, lc), "error in waiting for processing events")

		user2ChatsNew4, err := testRestClient.GetChatsByUserId(ctx, user2, nil)
		require.NoError(t, err, "error in getting chats")
		assert.Equal(t, 1, len(user2ChatsNew4))
		chat1OfUser24 := user2ChatsNew4[0]
		assert.Equal(t, int64(1), chat1OfUser24.UnreadMessages)

		user3ChatsNew4, err := testRestClient.GetChatsByUserId(ctx, user3, nil)
		require.NoError(t, err, "error in getting chats")
		assert.Equal(t, 1, len(user3ChatsNew4))
		chat1OfUser34 := user3ChatsNew4[0]
		assert.Equal(t, int64(2), chat1OfUser34.UnreadMessages)
	})
}

func TestPinChat(t *testing.T) {
	startAppFull(t, func(
		lgr *logger.LoggerWrapper,
		cfg *config.AppConfig,
		testRestClient *client.TestRestClient,
		saramaClient sarama.Client,
		m *cqrs.CommonProjection,
		aaaRestClient client.AaaRestClient,
		lc fx.Lifecycle,
	) {
		const user1 int64 = 1
		const user2 int64 = 2
		const user1Login = "admin1"
		const user2Login = "admin2"

		mockUser1 := dto.User{
			Id:               user1,
			Login:            user1Login,
			Avatar:           nil,
			ShortInfo:        nil,
			LoginColor:       nil,
			LastSeenDateTime: nil,
			AdditionalData:   nil,
		}

		mockUser2 := dto.User{
			Id:               user2,
			Login:            user2Login,
			Avatar:           nil,
			ShortInfo:        nil,
			LoginColor:       nil,
			LastSeenDateTime: nil,
			AdditionalData:   nil,
		}

		const chat1Name = "new chat 1"

		mockAaaClient := aaaRestClient.(*client.MockAaaRestClient)
		mockAaaClient.EXPECT().GetUsers(mock.Anything, mock.Anything).Return([]*dto.User{&mockUser1, &mockUser2}, nil)

		ctx := context.Background()

		chat1Id, err := testRestClient.CreateChat(ctx, user1, chat1Name)
		require.NoError(t, err, "error in creating chat")
		assert.True(t, chat1Id > 0)
		require.NoError(t, kafka.WaitForAllEventsProcessed(lgr, cfg, saramaClient, lc), "error in waiting for processing events")

		title, err := m.GetChatByUserIdAndChatId(ctx, user1, chat1Id)
		require.NoError(t, err, "error in getting chat")
		assert.Equal(t, chat1Name, title)

		const message1Text = "new message 1"

		message1Id, err := testRestClient.CreateMessage(ctx, user1, chat1Id, message1Text)
		require.NoError(t, err, "error in creating message")
		require.NoError(t, kafka.WaitForAllEventsProcessed(lgr, cfg, saramaClient, lc), "error in waiting for processing events")

		user1Chats, err := testRestClient.GetChatsByUserId(ctx, user1, nil)
		require.NoError(t, err, "error in getting chats")
		assert.Equal(t, 1, len(user1Chats))
		chat1OfUser1 := user1Chats[0]
		assert.Equal(t, false, chat1OfUser1.Pinned)
		assert.Equal(t, chat1Name, chat1OfUser1.Title)
		assert.Equal(t, int64(0), chat1OfUser1.UnreadMessages)

		user2Chats, err := testRestClient.GetChatsByUserId(ctx, user2, nil)
		require.NoError(t, err, "error in getting chats")
		assert.Equal(t, 0, len(user2Chats))

		chat1Messages, err := testRestClient.GetMessages(ctx, user1, chat1Id, nil)
		require.NoError(t, err, "error in getting messages")
		assert.Equal(t, 1, len(chat1Messages))
		message1 := chat1Messages[0]
		assert.Equal(t, message1Id, message1.Id)
		assert.Equal(t, message1Text, message1.Content)

		err = testRestClient.AddChatParticipants(ctx, user1, chat1Id, []int64{user2})
		require.NoError(t, err, "error in adding participants")
		require.NoError(t, kafka.WaitForAllEventsProcessed(lgr, cfg, saramaClient, lc), "error in waiting for processing events")

		chat1Participants, err := testRestClient.GetChatParticipants(ctx, chat1Id)
		require.NoError(t, err, "error in chat participants")
		require.Equal(t, 2, len(chat1Participants))
		assert.Equal(t, user2, chat1Participants[0].Id)
		assert.Equal(t, user1, chat1Participants[1].Id)

		user2ChatsNew, err := testRestClient.GetChatsByUserId(ctx, user2, nil)
		require.NoError(t, err, "error in getting chats")
		assert.Equal(t, 1, len(user2ChatsNew))
		chat1OfUser2 := user2ChatsNew[0]
		assert.Equal(t, false, chat1OfUser2.Pinned)
		assert.Equal(t, chat1Name, chat1OfUser2.Title)
		assert.Equal(t, int64(1), chat1OfUser2.UnreadMessages)

		err = testRestClient.PinChat(ctx, user1, chat1Id, true)
		require.NoError(t, err, "error in pinning chats")
		require.NoError(t, kafka.WaitForAllEventsProcessed(lgr, cfg, saramaClient, lc), "error in waiting for processing events")

		user1ChatsNew2, err := testRestClient.GetChatsByUserId(ctx, user1, nil)
		require.NoError(t, err, "error in getting chats")
		assert.Equal(t, 1, len(user1ChatsNew2))
		chat1OfUser1New2 := user1ChatsNew2[0]
		assert.Equal(t, true, chat1OfUser1New2.Pinned)
		assert.Equal(t, chat1Name, chat1OfUser1New2.Title)
		assert.Equal(t, int64(0), chat1OfUser1New2.UnreadMessages)
		assert.Equal(t, message1.Id, *chat1OfUser1New2.LastMessageId)
		assert.Equal(t, message1.Content, *chat1OfUser1New2.LastMessageContent)

		user2ChatsNew2, err := testRestClient.GetChatsByUserId(ctx, user2, nil)
		require.NoError(t, err, "error in getting chats")
		assert.Equal(t, 1, len(user2ChatsNew2))
		chat1OfUser2New2 := user2ChatsNew2[0]
		assert.Equal(t, false, chat1OfUser2New2.Pinned)
		assert.Equal(t, chat1Name, chat1OfUser2New2.Title)
		assert.Equal(t, int64(1), chat1OfUser2New2.UnreadMessages)
		assert.Equal(t, message1.Id, *chat1OfUser2New2.LastMessageId)
		assert.Equal(t, message1.Content, *chat1OfUser2New2.LastMessageContent)
	})

}

func TestDeleteChat(t *testing.T) {
	startAppFull(t, func(
		lgr *logger.LoggerWrapper,
		cfg *config.AppConfig,
		testRestClient *client.TestRestClient,
		saramaClient sarama.Client,
		m *cqrs.CommonProjection,
		aaaRestClient client.AaaRestClient,
		lc fx.Lifecycle,
	) {
		const user1 int64 = 1
		const user2 int64 = 2
		const user1Login = "admin1"
		const user2Login = "admin2"

		mockUser1 := dto.User{
			Id:               user1,
			Login:            user1Login,
			Avatar:           nil,
			ShortInfo:        nil,
			LoginColor:       nil,
			LastSeenDateTime: nil,
			AdditionalData:   nil,
		}

		mockUser2 := dto.User{
			Id:               user2,
			Login:            user2Login,
			Avatar:           nil,
			ShortInfo:        nil,
			LoginColor:       nil,
			LastSeenDateTime: nil,
			AdditionalData:   nil,
		}

		const chat1Name = "new chat 1"

		mockAaaClient := aaaRestClient.(*client.MockAaaRestClient)
		mockAaaClient.EXPECT().GetUsers(mock.Anything, mock.Anything).Return([]*dto.User{&mockUser1, &mockUser2}, nil)

		ctx := context.Background()

		chat1Id, err := testRestClient.CreateChat(ctx, user1, chat1Name)
		require.NoError(t, err, "error in creating chat")
		assert.True(t, chat1Id > 0)
		require.NoError(t, kafka.WaitForAllEventsProcessed(lgr, cfg, saramaClient, lc), "error in waiting for processing events")

		title, err := m.GetChatByUserIdAndChatId(ctx, user1, chat1Id)
		require.NoError(t, err, "error in getting chat")
		assert.Equal(t, chat1Name, title)

		const message1Text = "new message 1"

		message1Id, err := testRestClient.CreateMessage(ctx, user1, chat1Id, message1Text)
		require.NoError(t, err, "error in creating message")
		require.NoError(t, kafka.WaitForAllEventsProcessed(lgr, cfg, saramaClient, lc), "error in waiting for processing events")

		user1Chats, err := testRestClient.GetChatsByUserId(ctx, user1, nil)
		require.NoError(t, err, "error in getting chats")
		assert.Equal(t, 1, len(user1Chats))
		chat1OfUser1 := user1Chats[0]
		assert.Equal(t, false, chat1OfUser1.Pinned)
		assert.Equal(t, chat1Name, chat1OfUser1.Title)
		assert.Equal(t, int64(0), chat1OfUser1.UnreadMessages)

		user2Chats, err := testRestClient.GetChatsByUserId(ctx, user2, nil)
		require.NoError(t, err, "error in getting chats")
		assert.Equal(t, 0, len(user2Chats))

		chat1Messages, err := testRestClient.GetMessages(ctx, user1, chat1Id, nil)
		require.NoError(t, err, "error in getting messages")
		assert.Equal(t, 1, len(chat1Messages))
		message1 := chat1Messages[0]
		assert.Equal(t, message1Id, message1.Id)
		assert.Equal(t, message1Text, message1.Content)

		err = testRestClient.AddChatParticipants(ctx, user1, chat1Id, []int64{user2})
		require.NoError(t, err, "error in adding participants")
		require.NoError(t, kafka.WaitForAllEventsProcessed(lgr, cfg, saramaClient, lc), "error in waiting for processing events")

		chat1Participants, err := testRestClient.GetChatParticipants(ctx, chat1Id)
		require.NoError(t, err, "error in chat participants")
		require.Equal(t, 2, len(chat1Participants))
		assert.Equal(t, user2, chat1Participants[0].Id)
		assert.Equal(t, user1, chat1Participants[1].Id)

		user2ChatsNew, err := testRestClient.GetChatsByUserId(ctx, user2, nil)
		require.NoError(t, err, "error in getting chats")
		assert.Equal(t, 1, len(user2ChatsNew))
		chat1OfUser2 := user2ChatsNew[0]
		assert.Equal(t, false, chat1OfUser2.Pinned)
		assert.Equal(t, chat1Name, chat1OfUser2.Title)
		assert.Equal(t, int64(1), chat1OfUser2.UnreadMessages)

		err = testRestClient.DeleteChat(ctx, user1, chat1Id)
		require.NoError(t, err, "error in removing chats")
		require.NoError(t, kafka.WaitForAllEventsProcessed(lgr, cfg, saramaClient, lc), "error in waiting for processing events")

		user1ChatsNew2, err := testRestClient.GetChatsByUserId(ctx, user1, nil)
		require.NoError(t, err, "error in getting chats")
		assert.Equal(t, 0, len(user1ChatsNew2))

		user2ChatsNew2, err := testRestClient.GetChatsByUserId(ctx, user2, nil)
		require.NoError(t, err, "error in getting chats")
		assert.Equal(t, 0, len(user2ChatsNew2))
	})

}

func TestAddParticipant(t *testing.T) {
	startAppFull(t, func(
		lgr *logger.LoggerWrapper,
		cfg *config.AppConfig,
		testRestClient *client.TestRestClient,
		saramaClient sarama.Client,
		m *cqrs.CommonProjection,
		aaaRestClient client.AaaRestClient,
		lc fx.Lifecycle,
	) {
		const user1 int64 = 1
		const user2 int64 = 2
		const user1Login = "admin1"
		const user2Login = "admin2"

		mockUser1 := dto.User{
			Id:               user1,
			Login:            user1Login,
			Avatar:           nil,
			ShortInfo:        nil,
			LoginColor:       nil,
			LastSeenDateTime: nil,
			AdditionalData:   nil,
		}

		mockUser2 := dto.User{
			Id:               user2,
			Login:            user2Login,
			Avatar:           nil,
			ShortInfo:        nil,
			LoginColor:       nil,
			LastSeenDateTime: nil,
			AdditionalData:   nil,
		}

		const chat1Name = "new chat 1"

		mockAaaClient := aaaRestClient.(*client.MockAaaRestClient)
		mockAaaClient.EXPECT().GetUsers(mock.Anything, mock.Anything).Return([]*dto.User{&mockUser1, &mockUser2}, nil)

		ctx := context.Background()

		chat1Id, err := testRestClient.CreateChat(ctx, user1, chat1Name)
		require.NoError(t, err, "error in creating chat")
		assert.True(t, chat1Id > 0)
		require.NoError(t, kafka.WaitForAllEventsProcessed(lgr, cfg, saramaClient, lc), "error in waiting for processing events")

		title, err := m.GetChatByUserIdAndChatId(ctx, user1, chat1Id)
		require.NoError(t, err, "error in getting chat")
		assert.Equal(t, chat1Name, title)

		const message1Text = "new message 1"

		message1Id, err := testRestClient.CreateMessage(ctx, user1, chat1Id, message1Text)
		require.NoError(t, err, "error in creating message")
		require.NoError(t, kafka.WaitForAllEventsProcessed(lgr, cfg, saramaClient, lc), "error in waiting for processing events")

		user1Chats, err := testRestClient.GetChatsByUserId(ctx, user1, nil)
		require.NoError(t, err, "error in getting chats")
		assert.Equal(t, 1, len(user1Chats))
		chat1OfUser1 := user1Chats[0]
		assert.Equal(t, chat1Name, chat1OfUser1.Title)
		assert.Equal(t, int64(0), chat1OfUser1.UnreadMessages)
		assert.Equal(t, int64(1), chat1OfUser1.ParticipantsCount)
		assert.Equal(t, []int64{1}, chat1OfUser1.ParticipantIds)

		user2Chats, err := testRestClient.GetChatsByUserId(ctx, user2, nil)
		require.NoError(t, err, "error in getting chats")
		assert.Equal(t, 0, len(user2Chats))

		chat1Messages, err := testRestClient.GetMessages(ctx, user1, chat1Id, nil)
		require.NoError(t, err, "error in getting messages")
		assert.Equal(t, 1, len(chat1Messages))
		message1 := chat1Messages[0]
		assert.Equal(t, message1Id, message1.Id)
		assert.Equal(t, message1Text, message1.Content)

		err = testRestClient.AddChatParticipants(ctx, user1, chat1Id, []int64{user2})
		require.NoError(t, err, "error in adding participants")
		require.NoError(t, kafka.WaitForAllEventsProcessed(lgr, cfg, saramaClient, lc), "error in waiting for processing events")

		chat1Participants, err := testRestClient.GetChatParticipants(ctx, chat1Id)
		require.NoError(t, err, "error in chat participants")
		require.Equal(t, 2, len(chat1Participants))
		assert.Equal(t, user2, chat1Participants[0].Id)
		assert.Equal(t, user1, chat1Participants[1].Id)

		user2ChatsNew, err := testRestClient.GetChatsByUserId(ctx, user2, nil)
		require.NoError(t, err, "error in getting chats")
		assert.Equal(t, 1, len(user2ChatsNew))
		chat1OfUser2 := user2ChatsNew[0]
		assert.Equal(t, chat1Name, chat1OfUser2.Title)
		assert.Equal(t, int64(1), chat1OfUser2.UnreadMessages)
		assert.Equal(t, message1.Id, *chat1OfUser2.LastMessageId)
		assert.Equal(t, message1.Content, *chat1OfUser2.LastMessageContent)
		assert.Equal(t, int64(2), chat1OfUser2.ParticipantsCount)
		assert.Equal(t, []int64{2, 1}, chat1OfUser2.ParticipantIds)

		const chat1NewName = "new chat 1 renamed"
		err = testRestClient.EditChat(ctx, user1, chat1Id, chat1NewName, false)
		require.NoError(t, err, "error in changing chat")
		require.NoError(t, kafka.WaitForAllEventsProcessed(lgr, cfg, saramaClient, lc), "error in waiting for processing events")

		user1ChatsNew2, err := testRestClient.GetChatsByUserId(ctx, user1, nil)
		require.NoError(t, err, "error in getting chats")
		assert.Equal(t, 1, len(user1ChatsNew2))
		chat1OfUser1New2 := user1ChatsNew2[0]
		assert.Equal(t, chat1NewName, chat1OfUser1New2.Title)
		assert.Equal(t, int64(0), chat1OfUser1New2.UnreadMessages)
		assert.Equal(t, message1.Id, *chat1OfUser1New2.LastMessageId)
		assert.Equal(t, message1.Content, *chat1OfUser1New2.LastMessageContent)
		assert.Equal(t, int64(2), chat1OfUser1New2.ParticipantsCount)
		assert.Equal(t, []int64{2, 1}, chat1OfUser1New2.ParticipantIds)

		user2ChatsNew2, err := testRestClient.GetChatsByUserId(ctx, user2, nil)
		require.NoError(t, err, "error in getting chats")
		assert.Equal(t, 1, len(user2ChatsNew2))
		chat1OfUser2New2 := user2ChatsNew2[0]
		assert.Equal(t, chat1NewName, chat1OfUser2New2.Title)
		assert.Equal(t, int64(1), chat1OfUser2New2.UnreadMessages)
		assert.Equal(t, message1.Id, *chat1OfUser2New2.LastMessageId)
		assert.Equal(t, message1.Content, *chat1OfUser2New2.LastMessageContent)
		assert.Equal(t, int64(2), chat1OfUser2New2.ParticipantsCount)
		assert.Equal(t, []int64{2, 1}, chat1OfUser2New2.ParticipantIds)
	})
}

func TestDeleteParticipant(t *testing.T) {
	startAppFull(t, func(
		lgr *logger.LoggerWrapper,
		cfg *config.AppConfig,
		testRestClient *client.TestRestClient,
		saramaClient sarama.Client,
		m *cqrs.CommonProjection,
		aaaRestClient client.AaaRestClient,
		lc fx.Lifecycle,
	) {
		const user1 int64 = 1
		const user2 int64 = 2
		const user1Login = "admin1"
		const user2Login = "admin2"

		mockUser1 := dto.User{
			Id:               user1,
			Login:            user1Login,
			Avatar:           nil,
			ShortInfo:        nil,
			LoginColor:       nil,
			LastSeenDateTime: nil,
			AdditionalData:   nil,
		}

		mockUser2 := dto.User{
			Id:               user2,
			Login:            user2Login,
			Avatar:           nil,
			ShortInfo:        nil,
			LoginColor:       nil,
			LastSeenDateTime: nil,
			AdditionalData:   nil,
		}

		const chat1Name = "new chat 1"

		mockAaaClient := aaaRestClient.(*client.MockAaaRestClient)
		mockAaaClient.EXPECT().GetUsers(mock.Anything, mock.Anything).Return([]*dto.User{&mockUser1, &mockUser2}, nil)

		ctx := context.Background()

		chat1Id, err := testRestClient.CreateChat(ctx, user1, chat1Name)
		require.NoError(t, err, "error in creating chat")
		assert.True(t, chat1Id > 0)
		require.NoError(t, kafka.WaitForAllEventsProcessed(lgr, cfg, saramaClient, lc), "error in waiting for processing events")

		title, err := m.GetChatByUserIdAndChatId(ctx, user1, chat1Id)
		require.NoError(t, err, "error in getting chat")
		assert.Equal(t, chat1Name, title)

		const message1Text = "new message 1"

		message1Id, err := testRestClient.CreateMessage(ctx, user1, chat1Id, message1Text)
		require.NoError(t, err, "error in creating message")
		require.NoError(t, kafka.WaitForAllEventsProcessed(lgr, cfg, saramaClient, lc), "error in waiting for processing events")

		user1Chats, err := testRestClient.GetChatsByUserId(ctx, user1, nil)
		require.NoError(t, err, "error in getting chats")
		assert.Equal(t, 1, len(user1Chats))
		chat1OfUser1 := user1Chats[0]
		assert.Equal(t, chat1Name, chat1OfUser1.Title)
		assert.Equal(t, int64(0), chat1OfUser1.UnreadMessages)
		assert.Equal(t, int64(1), chat1OfUser1.ParticipantsCount)
		assert.Equal(t, []int64{1}, chat1OfUser1.ParticipantIds)

		user2Chats, err := testRestClient.GetChatsByUserId(ctx, user2, nil)
		require.NoError(t, err, "error in getting chats")
		assert.Equal(t, 0, len(user2Chats))

		chat1Messages, err := testRestClient.GetMessages(ctx, user1, chat1Id, nil)
		require.NoError(t, err, "error in getting messages")
		assert.Equal(t, 1, len(chat1Messages))
		message1 := chat1Messages[0]
		assert.Equal(t, message1Id, message1.Id)
		assert.Equal(t, message1Text, message1.Content)

		err = testRestClient.AddChatParticipants(ctx, user1, chat1Id, []int64{user2})
		require.NoError(t, err, "error in adding participants")
		require.NoError(t, kafka.WaitForAllEventsProcessed(lgr, cfg, saramaClient, lc), "error in waiting for processing events")

		chat1Participants, err := testRestClient.GetChatParticipants(ctx, chat1Id)
		require.NoError(t, err, "error in chat participants")
		require.Equal(t, 2, len(chat1Participants))
		assert.Equal(t, user2, chat1Participants[0].Id)
		assert.Equal(t, user1, chat1Participants[1].Id)

		user2ChatsNew, err := testRestClient.GetChatsByUserId(ctx, user2, nil)
		require.NoError(t, err, "error in getting chats")
		assert.Equal(t, 1, len(user2ChatsNew))
		chat1OfUser2 := user2ChatsNew[0]
		assert.Equal(t, chat1Name, chat1OfUser2.Title)
		assert.Equal(t, int64(1), chat1OfUser2.UnreadMessages)
		assert.Equal(t, message1.Id, *chat1OfUser2.LastMessageId)
		assert.Equal(t, message1.Content, *chat1OfUser2.LastMessageContent)
		assert.Equal(t, int64(2), chat1OfUser2.ParticipantsCount)
		assert.Equal(t, []int64{2, 1}, chat1OfUser2.ParticipantIds)

		err = testRestClient.DeleteChatParticipants(ctx, user1, chat1Id, []int64{user2})
		require.NoError(t, err, "error in removing chat participants")
		require.NoError(t, kafka.WaitForAllEventsProcessed(lgr, cfg, saramaClient, lc), "error in waiting for processing events")

		user2ChatsNew2, err := testRestClient.GetChatsByUserId(ctx, user2, nil)
		require.NoError(t, err, "error in getting chats")
		assert.Equal(t, 0, len(user2ChatsNew2))

		chat1Participants2, err := testRestClient.GetChatParticipants(ctx, chat1Id)
		require.NoError(t, err, "error in chat participants")
		require.Equal(t, 1, len(chat1Participants2))
		assert.Equal(t, user1, chat1Participants2[0].Id)

		user1ChatsNew2, err := testRestClient.GetChatsByUserId(ctx, user1, nil)
		require.NoError(t, err, "error in getting chats")
		assert.Equal(t, 1, len(user1ChatsNew2))
		chat1OfUser1New2 := user1ChatsNew2[0]
		assert.Equal(t, int64(1), chat1OfUser1New2.ParticipantsCount)
		assert.Equal(t, []int64{1}, chat1OfUser1New2.ParticipantIds)
	})
}

func TestChangeParticipant(t *testing.T) {
	startAppFull(t, func(
		lgr *logger.LoggerWrapper,
		cfg *config.AppConfig,
		testRestClient *client.TestRestClient,
		saramaClient sarama.Client,
		m *cqrs.CommonProjection,
		aaaRestClient client.AaaRestClient,
		lc fx.Lifecycle,
	) {
		const user1 int64 = 1
		const user2 int64 = 2
		const user1Login = "admin1"
		const user2Login = "tobeAdmin2"

		mockUser1 := dto.User{
			Id:               user1,
			Login:            user1Login,
			Avatar:           nil,
			ShortInfo:        nil,
			LoginColor:       nil,
			LastSeenDateTime: nil,
			AdditionalData:   nil,
		}

		mockUser2 := dto.User{
			Id:               user2,
			Login:            user2Login,
			Avatar:           nil,
			ShortInfo:        nil,
			LoginColor:       nil,
			LastSeenDateTime: nil,
			AdditionalData:   nil,
		}

		const chat1Name = "new chat 1"

		mockAaaClient := aaaRestClient.(*client.MockAaaRestClient)
		mockAaaClient.EXPECT().GetUsers(mock.Anything, mock.Anything).Return([]*dto.User{&mockUser1, &mockUser2}, nil)

		ctx := context.Background()

		chat1Id, err := testRestClient.CreateChat(ctx, user1, chat1Name)
		require.NoError(t, err, "error in creating chat")
		assert.True(t, chat1Id > 0)
		require.NoError(t, kafka.WaitForAllEventsProcessed(lgr, cfg, saramaClient, lc), "error in waiting for processing events")

		err = testRestClient.AddChatParticipants(ctx, user1, chat1Id, []int64{user2})
		require.NoError(t, err, "error in adding participants")
		require.NoError(t, kafka.WaitForAllEventsProcessed(lgr, cfg, saramaClient, lc), "error in waiting for processing events")

		// negative test
		err = testRestClient.ChangeChatParticipant(ctx, user2, chat1Id, user1, false)
		require.NotNil(t, err)
		assert.True(t, strings.Contains(err.Error(), "code: 401"))

		chat1Participants, err := testRestClient.GetChatParticipants(ctx, chat1Id)
		require.NoError(t, err, "error in chat participants")
		require.Equal(t, 2, len(chat1Participants))
		assert.Equal(t, user2, chat1Participants[0].Id)
		assert.Equal(t, false, chat1Participants[0].ChatAdmin)
		assert.Equal(t, user1, chat1Participants[1].Id)
		assert.Equal(t, true, chat1Participants[1].ChatAdmin)

		user2ChatsNew, err := testRestClient.GetChatsByUserId(ctx, user2, nil)
		require.NoError(t, err, "error in getting chats")
		assert.Equal(t, 1, len(user2ChatsNew))
		chat1OfUser2 := user2ChatsNew[0]
		assert.Equal(t, chat1Name, chat1OfUser2.Title)
		assert.Equal(t, int64(2), chat1OfUser2.ParticipantsCount)
		assert.Equal(t, []int64{2, 1}, chat1OfUser2.ParticipantIds)

		err = testRestClient.ChangeChatParticipant(ctx, user1, chat1Id, user2, true)
		require.NoError(t, err, "error in changing chat participants")
		require.NoError(t, kafka.WaitForAllEventsProcessed(lgr, cfg, saramaClient, lc), "error in waiting for processing events")

		chat1Participants2, err := testRestClient.GetChatParticipants(ctx, chat1Id)
		require.NoError(t, err, "error in chat participants")
		require.Equal(t, 2, len(chat1Participants2))
		assert.Equal(t, user2, chat1Participants2[0].Id)
		assert.Equal(t, true, chat1Participants2[0].ChatAdmin)
		assert.Equal(t, user1, chat1Participants2[1].Id)
		assert.Equal(t, true, chat1Participants2[1].ChatAdmin)
	})
}

func TestEditMessage(t *testing.T) {
	startAppFull(t, func(
		lgr *logger.LoggerWrapper,
		cfg *config.AppConfig,
		testRestClient *client.TestRestClient,
		saramaClient sarama.Client,
		m *cqrs.CommonProjection,
		aaaRestClient client.AaaRestClient,
		lc fx.Lifecycle,
	) {
		mockAaaClient := aaaRestClient.(*client.MockAaaRestClient)
		mockAaaClient.EXPECT().GetUsers(mock.Anything, mock.Anything).Return([]*dto.User{}, nil)

		const user1 int64 = 1
		const chat1Name = "new chat 1"

		ctx := context.Background()

		chat1Id, err := testRestClient.CreateChat(ctx, user1, chat1Name)
		require.NoError(t, err, "error in creating chat")
		assert.True(t, chat1Id > 0)
		require.NoError(t, kafka.WaitForAllEventsProcessed(lgr, cfg, saramaClient, lc), "error in waiting for processing events")

		title, err := m.GetChatByUserIdAndChatId(ctx, user1, chat1Id)
		require.NoError(t, err, "error in getting chat")
		assert.Equal(t, chat1Name, title)

		const message1Text = "new message 1"
		message1Id, err := testRestClient.CreateMessage(ctx, user1, chat1Id, message1Text)
		require.NoError(t, err, "error in creating message")

		const message2Text = "new message 2"
		message2Id, err := testRestClient.CreateMessage(ctx, user1, chat1Id, message2Text)
		require.NoError(t, err, "error in creating message")
		require.NoError(t, kafka.WaitForAllEventsProcessed(lgr, cfg, saramaClient, lc), "error in waiting for processing events")

		user1Chats, err := testRestClient.GetChatsByUserId(ctx, user1, nil)
		require.NoError(t, err, "error in getting chats")
		assert.Equal(t, 1, len(user1Chats))
		chat1OfUser1 := user1Chats[0]
		assert.Equal(t, chat1Name, chat1OfUser1.Title)
		assert.Equal(t, int64(0), chat1OfUser1.UnreadMessages)
		assert.Equal(t, message2Text, *chat1OfUser1.LastMessageContent)

		chat1Messages, err := testRestClient.GetMessages(ctx, user1, chat1Id, nil)
		require.NoError(t, err, "error in getting messages")
		assert.Equal(t, 2, len(chat1Messages))
		message1 := chat1Messages[0]
		message2 := chat1Messages[1]
		assert.Equal(t, message1Id, message1.Id)
		assert.Equal(t, message1Text, message1.Content)
		assert.Equal(t, message2Id, message2.Id)
		assert.Equal(t, message2Text, message2.Content)

		const message1TextNew = "new message 1 edited"
		err = testRestClient.EditMessage(ctx, user1, chat1Id, message1.Id, message1TextNew)
		require.NoError(t, err, "error in creating message")
		require.NoError(t, kafka.WaitForAllEventsProcessed(lgr, cfg, saramaClient, lc), "error in waiting for processing events")

		user1ChatsNew, err := testRestClient.GetChatsByUserId(ctx, user1, nil)
		require.NoError(t, err, "error in getting chats")
		assert.Equal(t, 1, len(user1ChatsNew))
		chat1OfUser1New := user1ChatsNew[0]
		assert.Equal(t, chat1Name, chat1OfUser1New.Title)
		assert.Equal(t, int64(0), chat1OfUser1New.UnreadMessages)
		assert.Equal(t, message2Text, *chat1OfUser1New.LastMessageContent)

		chat1MessagesNew, err := testRestClient.GetMessages(ctx, user1, chat1Id, nil)
		require.NoError(t, err, "error in getting messages")
		assert.Equal(t, 2, len(chat1MessagesNew))
		message1New := chat1MessagesNew[0]
		message2New := chat1MessagesNew[1]
		assert.Equal(t, message1Id, message1New.Id)
		assert.Equal(t, message1TextNew, message1New.Content)
		assert.Equal(t, message2Id, message2New.Id)
		assert.Equal(t, message2Text, message2New.Content)

		const message2TextNew = "new message 1 edited"
		err = testRestClient.EditMessage(ctx, user1, chat1Id, message2.Id, message2TextNew)
		require.NoError(t, err, "error in creating message")
		require.NoError(t, kafka.WaitForAllEventsProcessed(lgr, cfg, saramaClient, lc), "error in waiting for processing events")

		user1ChatsNew2, err := testRestClient.GetChatsByUserId(ctx, user1, nil)
		require.NoError(t, err, "error in getting chats")
		assert.Equal(t, 1, len(user1ChatsNew2))
		chat1OfUser1New2 := user1ChatsNew2[0]
		assert.Equal(t, chat1Name, chat1OfUser1New2.Title)
		assert.Equal(t, int64(0), chat1OfUser1New2.UnreadMessages)
		assert.Equal(t, message2TextNew, *chat1OfUser1New2.LastMessageContent)

		chat1MessagesNew2, err := testRestClient.GetMessages(ctx, user1, chat1Id, nil)
		require.NoError(t, err, "error in getting messages")
		assert.Equal(t, 2, len(chat1MessagesNew2))
		message1New2 := chat1MessagesNew2[0]
		message2New2 := chat1MessagesNew2[1]
		assert.Equal(t, message1Id, message1New2.Id)
		assert.Equal(t, message1TextNew, message1New2.Content)
		assert.Equal(t, message2Id, message2New2.Id)
		assert.Equal(t, message2TextNew, message2New2.Content)
	})
}

func TestBlog(t *testing.T) {
	startAppFull(t, func(
		lgr *logger.LoggerWrapper,
		cfg *config.AppConfig,
		testRestClient *client.TestRestClient,
		saramaClient sarama.Client,
		m *cqrs.CommonProjection,
		aaaRestClient client.AaaRestClient,
		lc fx.Lifecycle,
	) {
		const user1 int64 = 1
		const user1Login = "admin1"

		mockUser1 := dto.User{
			Id:               user1,
			Login:            user1Login,
			Avatar:           nil,
			ShortInfo:        nil,
			LoginColor:       nil,
			LastSeenDateTime: nil,
			AdditionalData:   nil,
		}

		const chat1Name = "new chat 1"

		mockAaaClient := aaaRestClient.(*client.MockAaaRestClient)
		mockAaaClient.EXPECT().GetUsers(mock.Anything, mock.Anything).Return([]*dto.User{&mockUser1}, nil)

		ctx := context.Background()

		chat1Id, err := testRestClient.CreateChat(ctx, user1, chat1Name)
		require.NoError(t, err, "error in creating chat")
		assert.True(t, chat1Id > 0)

		// await before chat editing
		require.NoError(t, kafka.WaitForAllEventsProcessed(lgr, cfg, saramaClient, lc), "error in waiting for processing events")

		err = testRestClient.EditChat(ctx, user1, chat1Id, chat1Name, false)
		require.NoError(t, err)
		require.NoError(t, kafka.WaitForAllEventsProcessed(lgr, cfg, saramaClient, lc), "error in waiting for processing events")

		const message1Text = "new message 1"
		message1Id, err := testRestClient.CreateMessage(ctx, user1, chat1Id, message1Text)
		require.NoError(t, err, "error in creating message")

		const message2Text = "new message 2"
		message2Id, err := testRestClient.CreateMessage(ctx, user1, chat1Id, message2Text)
		require.NoError(t, err, "error in creating message")

		err = testRestClient.MakeMessageBlogPost(ctx, user1, chat1Id, message1Id)
		require.NoError(t, err, "error in making message blog post")
		require.NoError(t, kafka.WaitForAllEventsProcessed(lgr, cfg, saramaClient, lc), "error in waiting for processing events")

		blogs, err := testRestClient.SearchBlogs(ctx)
		require.NoError(t, err, "error in searching blog posts")
		assert.Equal(t, 1, len(blogs))
		assert.Equal(t, chat1Id, blogs[0].Id)
		assert.Equal(t, chat1Name, blogs[0].Title)

		comments, err := testRestClient.SearchBlogComments(ctx, chat1Id)
		require.NoError(t, err, "error in searching blog comments")
		assert.Equal(t, 1, len(comments))
		assert.Equal(t, message2Id, comments[0].Id)
		assert.Equal(t, message2Text, comments[0].Content)
	})
}

func TestChatPaginate(t *testing.T) {
	startAppFull(t, func(
		lgr *logger.LoggerWrapper,
		cfg *config.AppConfig,
		testRestClient *client.TestRestClient,
		saramaClient sarama.Client,
		dba *db.DB,
		aaaRestClient client.AaaRestClient,
		lc fx.Lifecycle,
	) {
		mockAaaClient := aaaRestClient.(*client.MockAaaRestClient)
		mockAaaClient.EXPECT().GetUsers(mock.Anything, mock.Anything).Return([]*dto.User{}, nil)

		const user1 int64 = 1
		const num = 1000
		const chatPrefix = "generated_chat"

		ctx := context.Background()

		var lastChatId int64
		var err error
		for i := 1; i <= num; i++ {
			lastChatId, err = testRestClient.CreateChat(ctx, user1, chatPrefix+utils.ToString(i))
			require.NoError(t, err, "error in creating chat")
			assert.True(t, lastChatId > 0)
		}
		waitForChatExists(lgr, dba, lastChatId)
		require.NoError(t, kafka.WaitForAllEventsProcessed(lgr, cfg, saramaClient, lc), "error in waiting for processing events")

		// get initial page
		query1 := url.Values{
			dto.SizeParam: []string{utils.ToString(40)},
		}
		resp1, err := testRestClient.GetChatsByUserId(ctx, user1, &query1)
		require.NoError(t, err)
		assert.Equal(t, 40, len(resp1))
		assert.Equal(t, "generated_chat1000", resp1[0].Title)
		assert.Equal(t, "generated_chat999", resp1[1].Title)
		assert.Equal(t, "generated_chat998", resp1[2].Title)
		assert.Equal(t, "generated_chat961", resp1[39].Title)

		lastPinned := resp1[len(resp1)-1].Pinned
		lastId := resp1[len(resp1)-1].Id
		lastLastUpdateDateTime := resp1[len(resp1)-1].UpdateDateTime

		// get second page
		query2 := url.Values{
			dto.SizeParam: []string{utils.ToString(40)},

			dto.PinnedParam:             []string{utils.ToString(lastPinned)},
			dto.LastUpdateDateTimeParam: []string{lastLastUpdateDateTime.Format(time.RFC3339Nano)},
			dto.ChatIdParam:             []string{utils.ToString(lastId)},
		}
		resp2, err := testRestClient.GetChatsByUserId(ctx, user1, &query2)
		require.NoError(t, err)
		assert.Equal(t, 40, len(resp2))
		assert.Equal(t, "generated_chat960", resp2[0].Title)
		assert.Equal(t, "generated_chat959", resp2[1].Title)
		assert.Equal(t, "generated_chat958", resp2[2].Title)
		assert.Equal(t, "generated_chat921", resp2[39].Title)
	})
}

func TestMessagePaginate(t *testing.T) {
	startAppFull(t, func(
		lgr *logger.LoggerWrapper,
		cfg *config.AppConfig,
		testRestClient *client.TestRestClient,
		saramaClient sarama.Client,
		dba *db.DB,
		aaaRestClient client.AaaRestClient,
		lc fx.Lifecycle,
	) {
		mockAaaClient := aaaRestClient.(*client.MockAaaRestClient)
		mockAaaClient.EXPECT().GetUsers(mock.Anything, mock.Anything).Return([]*dto.User{}, nil)

		const user1 int64 = 1
		const chat1Name = "new chat 1"
		const num = 500

		ctx := context.Background()

		chat1Id, err := testRestClient.CreateChat(ctx, user1, chat1Name)
		require.NoError(t, err, "error in creating chat")
		assert.True(t, chat1Id > 0)
		require.NoError(t, kafka.WaitForAllEventsProcessed(lgr, cfg, saramaClient, lc), "error in waiting for processing events")

		const messagePrefix = "generated_message"

		var lastMessageId int64
		for i := 1; i <= num; i++ {
			lastMessageId, err = testRestClient.CreateMessage(ctx, user1, chat1Id, messagePrefix+utils.ToString(i))
			require.NoError(t, err, "error in creating message")
		}
		waitForMessageExists(lgr, dba, chat1Id, lastMessageId)
		require.NoError(t, kafka.WaitForAllEventsProcessed(lgr, cfg, saramaClient, lc), "error in waiting for processing events")

		// get first page
		query1 := url.Values{
			dto.SizeParam:          []string{utils.ToString(3)},
			dto.StartingFromItemId: []string{utils.ToString(6)},
		}
		resp1, err := testRestClient.GetMessages(ctx, user1, chat1Id, &query1)
		require.NoError(t, err)

		assert.Equal(t, 3, len(resp1))
		assert.True(t, strings.HasPrefix(resp1[0].Content, "generated_message7")) // different from chat because of different way of generating test data
		assert.True(t, strings.HasPrefix(resp1[1].Content, "generated_message8"))
		assert.True(t, strings.HasPrefix(resp1[2].Content, "generated_message9"))
		assert.Equal(t, int64(7), resp1[0].Id)
		assert.Equal(t, int64(8), resp1[1].Id)
		assert.Equal(t, int64(9), resp1[2].Id)

		lastId := resp1[len(resp1)-1].Id

		// get second page
		query2 := url.Values{
			dto.SizeParam:          []string{utils.ToString(3)},
			dto.StartingFromItemId: []string{utils.ToString(lastId)},
		}
		resp2, err := testRestClient.GetMessages(ctx, user1, chat1Id, &query2)
		require.NoError(t, err)

		assert.Equal(t, 3, len(resp2))
		assert.True(t, strings.HasPrefix(resp2[0].Content, "generated_message10"))
		assert.True(t, strings.HasPrefix(resp2[1].Content, "generated_message11"))
		assert.True(t, strings.HasPrefix(resp2[2].Content, "generated_message12"))
		assert.Equal(t, int64(10), resp2[0].Id)
		assert.Equal(t, int64(11), resp2[1].Id)
		assert.Equal(t, int64(12), resp2[2].Id)
	})
}
