package cmd

import (
	"context"
	"github.com/IBM/sarama"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go-cqrs-chat-example/client"
	"go-cqrs-chat-example/config"
	"go-cqrs-chat-example/cqrs"
	"go-cqrs-chat-example/db"
	"go-cqrs-chat-example/dto"
	"go-cqrs-chat-example/kafka"
	"go-cqrs-chat-example/logger"
	"go-cqrs-chat-example/utils"
	"go.uber.org/fx"
	"strings"
	"testing"
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

		avatar := "http://example.com/avatar.jpg"
		avatarBig := "http://example.com/avatar-big.jpg"

		chat1Id, err := testRestClient.CreateChat(ctx, user1, chat1Name, client.NewChatOptionAvatar(&avatar, &avatarBig))
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

		user1Chats, err := testRestClient.GetChats(ctx, user1)
		require.NoError(t, err, "error in getting chats")
		assert.Equal(t, 1, len(user1Chats))
		chat1OfUser1 := user1Chats[0]
		assert.Equal(t, chat1Name, chat1OfUser1.Title)
		assert.Equal(t, int64(0), chat1OfUser1.UnreadMessages)
		assert.Equal(t, avatar, *chat1OfUser1.Avatar)
		assert.Equal(t, avatarBig, *chat1OfUser1.AvatarBig)

		user1HasUnreadMessages, err := testRestClient.GetHasUnreadMessages(ctx, user1)
		require.NoError(t, err, "error in getting has unread messages")
		assert.Equal(t, false, user1HasUnreadMessages)

		user2Chats, err := testRestClient.GetChats(ctx, user2)
		require.NoError(t, err, "error in getting chats")
		assert.Equal(t, 0, len(user2Chats))

		user2HasUnreadMessages, err := testRestClient.GetHasUnreadMessages(ctx, user2)
		require.NoError(t, err, "error in getting has unread messages")
		assert.Equal(t, false, user2HasUnreadMessages)

		user3Chats, err := testRestClient.GetChats(ctx, user3)
		require.NoError(t, err, "error in getting chats")
		assert.Equal(t, 0, len(user3Chats))

		chat1Messages, err := testRestClient.GetMessages(ctx, user1, chat1Id)
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

		user2ChatsNew, err := testRestClient.GetChats(ctx, user2)
		require.NoError(t, err, "error in getting chats")
		assert.Equal(t, 1, len(user2ChatsNew))
		chat1OfUser2 := user2ChatsNew[0]
		assert.Equal(t, chat1Name, chat1OfUser2.Title)
		assert.Equal(t, int64(1), chat1OfUser2.UnreadMessages)

		user2HasUnreadMessagesNew, err := testRestClient.GetHasUnreadMessages(ctx, user2)
		require.NoError(t, err, "error in getting has unread messages")
		assert.Equal(t, true, user2HasUnreadMessagesNew)

		user3ChatsNew, err := testRestClient.GetChats(ctx, user3)
		require.NoError(t, err, "error in getting chats")
		assert.Equal(t, 1, len(user3ChatsNew))
		chat1OfUser3 := user3ChatsNew[0]
		assert.Equal(t, chat1Name, chat1OfUser3.Title)
		assert.Equal(t, int64(1), chat1OfUser3.UnreadMessages)

		user3HasUnreadMessagesNew, err := testRestClient.GetHasUnreadMessages(ctx, user3)
		require.NoError(t, err, "error in getting has unread messages")
		assert.Equal(t, true, user3HasUnreadMessagesNew)

		err = testRestClient.ReadMessage(ctx, user2, chat1Id, message1.Id)
		require.NoError(t, err, "error in reading message")
		require.NoError(t, kafka.WaitForAllEventsProcessed(lgr, cfg, saramaClient, lc), "error in waiting for processing events")

		user2ChatsNew2, err := testRestClient.GetChats(ctx, user2)
		require.NoError(t, err, "error in getting chats")
		assert.Equal(t, 1, len(user2ChatsNew2))
		chat1OfUser22 := user2ChatsNew2[0]
		assert.Equal(t, int64(0), chat1OfUser22.UnreadMessages)

		user2HasUnreadMessagesNew2, err := testRestClient.GetHasUnreadMessages(ctx, user2)
		require.NoError(t, err, "error in getting has unread messages")
		assert.Equal(t, false, user2HasUnreadMessagesNew2)

		user3ChatsNew2, err := testRestClient.GetChats(ctx, user3)
		require.NoError(t, err, "error in getting chats")
		assert.Equal(t, 1, len(user3ChatsNew2))
		chat1OfUser32 := user3ChatsNew2[0]
		assert.Equal(t, int64(1), chat1OfUser32.UnreadMessages)

		user3HasUnreadMessagesNew2, err := testRestClient.GetHasUnreadMessages(ctx, user3)
		require.NoError(t, err, "error in getting has unread messages")
		assert.Equal(t, true, user3HasUnreadMessagesNew2)

		const message2Text = "new message 2"
		messageId2, err := testRestClient.CreateMessage(ctx, user1, chat1Id, message2Text)
		require.NoError(t, err, "error in creating message")

		const message3Text = "new message 3"
		messageId3, err := testRestClient.CreateMessage(ctx, user1, chat1Id, message3Text)
		require.NoError(t, err, "error in creating message")
		assert.True(t, messageId3 > 0)
		require.NoError(t, kafka.WaitForAllEventsProcessed(lgr, cfg, saramaClient, lc), "error in waiting for processing events")

		user2ChatsNew3, err := testRestClient.GetChats(ctx, user2)
		require.NoError(t, err, "error in getting chats")
		assert.Equal(t, 1, len(user2ChatsNew3))
		chat1OfUser23 := user2ChatsNew3[0]
		assert.Equal(t, int64(2), chat1OfUser23.UnreadMessages)

		user2HasUnreadMessagesNew3, err := testRestClient.GetHasUnreadMessages(ctx, user2)
		require.NoError(t, err, "error in getting has unread messages")
		assert.Equal(t, true, user2HasUnreadMessagesNew3)

		user3ChatsNew3, err := testRestClient.GetChats(ctx, user3)
		require.NoError(t, err, "error in getting chats")
		assert.Equal(t, 1, len(user3ChatsNew3))
		chat1OfUser33 := user3ChatsNew3[0]
		assert.Equal(t, int64(3), chat1OfUser33.UnreadMessages)

		user3HasUnreadMessagesNew3, err := testRestClient.GetHasUnreadMessages(ctx, user3)
		require.NoError(t, err, "error in getting has unread messages")
		assert.Equal(t, true, user3HasUnreadMessagesNew3)

		err = testRestClient.DeleteMessage(ctx, user1, chat1Id, messageId3)
		require.NoError(t, err, "error in delete message")
		require.NoError(t, kafka.WaitForAllEventsProcessed(lgr, cfg, saramaClient, lc), "error in waiting for processing events")

		user2ChatsNew4, err := testRestClient.GetChats(ctx, user2)
		require.NoError(t, err, "error in getting chats")
		assert.Equal(t, 1, len(user2ChatsNew4))
		chat1OfUser24 := user2ChatsNew4[0]
		assert.Equal(t, int64(1), chat1OfUser24.UnreadMessages)

		user2HasUnreadMessagesNew4, err := testRestClient.GetHasUnreadMessages(ctx, user2)
		require.NoError(t, err, "error in getting has unread messages")
		assert.Equal(t, true, user2HasUnreadMessagesNew4)

		user3ChatsNew4, err := testRestClient.GetChats(ctx, user3)
		require.NoError(t, err, "error in getting chats")
		assert.Equal(t, 1, len(user3ChatsNew4))
		chat1OfUser34 := user3ChatsNew4[0]
		assert.Equal(t, int64(2), chat1OfUser34.UnreadMessages)

		user3HasUnreadMessagesNew4, err := testRestClient.GetHasUnreadMessages(ctx, user3)
		require.NoError(t, err, "error in getting has unread messages")
		assert.Equal(t, true, user3HasUnreadMessagesNew4)

		err = testRestClient.PutUserChatNotificationSettings(ctx, user2, chat1Id, false)
		require.NoError(t, err, "error in setting contribute into has new messages")
		require.NoError(t, kafka.WaitForAllEventsProcessed(lgr, cfg, saramaClient, lc), "error in waiting for processing events")

		// this message should not contribute into user 2's new messages because user 2 disabled them for chat 1
		messageId4, err := testRestClient.CreateMessage(ctx, user1, chat1Id, "msg 4")
		require.NoError(t, err, "error in creating message")
		assert.True(t, messageId4 > 0)
		require.NoError(t, kafka.WaitForAllEventsProcessed(lgr, cfg, saramaClient, lc), "error in waiting for processing events")

		user2ChatsNew41, err := testRestClient.GetChats(ctx, user2)
		require.NoError(t, err, "error in getting chats")
		assert.Equal(t, 1, len(user2ChatsNew41))
		chat1OfUser241 := user2ChatsNew41[0]
		assert.Equal(t, int64(2), chat1OfUser241.UnreadMessages)
		assert.Equal(t, false, chat1OfUser241.ConsiderMessagesAsUnread)

		user2HasUnreadMessagesNew41, err := testRestClient.GetHasUnreadMessages(ctx, user2)
		require.NoError(t, err, "error in getting has unread messages")
		assert.Equal(t, false, user2HasUnreadMessagesNew41)

		// assert that one more message won't erase existing status
		user3HasUnreadMessagesNew41, err := testRestClient.GetHasUnreadMessages(ctx, user3)
		require.NoError(t, err, "error in getting has unread messages")
		assert.Equal(t, true, user3HasUnreadMessagesNew41)

		err = testRestClient.PutUserChatNotificationSettings(ctx, user2, chat1Id, true)
		require.NoError(t, err, "error in setting contribute into has new messages")
		require.NoError(t, kafka.WaitForAllEventsProcessed(lgr, cfg, saramaClient, lc), "error in waiting for processing events")

		user2ChatsNew42, err := testRestClient.GetChats(ctx, user2)
		require.NoError(t, err, "error in getting chats")
		assert.Equal(t, 1, len(user2ChatsNew42))
		chat1OfUser242 := user2ChatsNew42[0]
		assert.Equal(t, int64(2), chat1OfUser242.UnreadMessages)
		assert.Equal(t, true, chat1OfUser242.ConsiderMessagesAsUnread)

		user2HasUnreadMessagesNew42, err := testRestClient.GetHasUnreadMessages(ctx, user2)
		require.NoError(t, err, "error in getting has unread messages")
		assert.Equal(t, true, user2HasUnreadMessagesNew42)

		err = testRestClient.DeleteMessage(ctx, user1, chat1Id, messageId4)
		require.NoError(t, err, "error in delete message")
		err = testRestClient.DeleteMessage(ctx, user1, chat1Id, messageId2)
		require.NoError(t, err, "error in delete message")
		err = testRestClient.DeleteMessage(ctx, user1, chat1Id, message1Id)
		require.NoError(t, err, "error in delete message")
		require.NoError(t, kafka.WaitForAllEventsProcessed(lgr, cfg, saramaClient, lc), "error in waiting for processing events")

		user1HasUnreadMessagesNew5, err := testRestClient.GetHasUnreadMessages(ctx, user1)
		require.NoError(t, err, "error in getting has unread messages")
		assert.Equal(t, false, user1HasUnreadMessagesNew5)

		user2HasUnreadMessagesNew5, err := testRestClient.GetHasUnreadMessages(ctx, user2)
		require.NoError(t, err, "error in getting has unread messages")
		assert.Equal(t, false, user2HasUnreadMessagesNew5)

		user3HasUnreadMessagesNew5, err := testRestClient.GetHasUnreadMessages(ctx, user3)
		require.NoError(t, err, "error in getting has unread messages")
		assert.Equal(t, false, user3HasUnreadMessagesNew5)
	})
}

func TestReadAll(t *testing.T) {
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
		const chat2Name = "new chat 2"

		mockAaaClient := aaaRestClient.(*client.MockAaaRestClient)
		mockAaaClient.EXPECT().GetUsers(mock.Anything, mock.Anything).Return([]*dto.User{&mockUser1, &mockUser2}, nil)

		ctx := context.Background()

		chat1Id, err := testRestClient.CreateChat(ctx, user1, chat1Name)
		require.NoError(t, err, "error in creating chat")
		assert.True(t, chat1Id > 0)
		require.NoError(t, kafka.WaitForAllEventsProcessed(lgr, cfg, saramaClient, lc), "error in waiting for processing events")

		const message11Text = "new message 11"
		const message12Text = "new message 12"

		message1Id, err := testRestClient.CreateMessage(ctx, user1, chat1Id, message11Text)
		require.NoError(t, err, "error in creating message")
		require.NoError(t, kafka.WaitForAllEventsProcessed(lgr, cfg, saramaClient, lc), "error in waiting for processing events")
		assert.True(t, message1Id > 0)

		chat2Id, err := testRestClient.CreateChat(ctx, user1, chat2Name)
		require.NoError(t, err, "error in creating chat")
		assert.True(t, chat2Id > 0)
		require.NoError(t, kafka.WaitForAllEventsProcessed(lgr, cfg, saramaClient, lc), "error in waiting for processing events")

		message2Id, err := testRestClient.CreateMessage(ctx, user1, chat2Id, message12Text)
		require.NoError(t, err, "error in creating message")
		require.NoError(t, kafka.WaitForAllEventsProcessed(lgr, cfg, saramaClient, lc), "error in waiting for processing events")
		assert.True(t, message2Id > 0)

		err = testRestClient.AddChatParticipants(ctx, user1, chat1Id, []int64{user2})
		require.NoError(t, err, "error in adding participants")
		err = testRestClient.AddChatParticipants(ctx, user1, chat2Id, []int64{user2})
		require.NoError(t, err, "error in adding participants")
		require.NoError(t, kafka.WaitForAllEventsProcessed(lgr, cfg, saramaClient, lc), "error in waiting for processing events")

		user2Chats, err := testRestClient.GetChats(ctx, user2)
		require.NoError(t, err, "error in getting chats")
		assert.Equal(t, 2, len(user2Chats))
		chat1OfUser2 := user2Chats[0]
		chat2OfUser2 := user2Chats[1]
		assert.Equal(t, int64(1), chat1OfUser2.UnreadMessages)
		assert.Equal(t, int64(1), chat2OfUser2.UnreadMessages)

		user2HasUnreadMessages, err := testRestClient.GetHasUnreadMessages(ctx, user2)
		require.NoError(t, err, "error in getting has unread messages")
		assert.Equal(t, true, user2HasUnreadMessages)

		err = testRestClient.ReadAllMessages(ctx, user2)
		require.NoError(t, err, "error in read all messages")
		require.NoError(t, kafka.WaitForAllEventsProcessed(lgr, cfg, saramaClient, lc), "error in waiting for processing events")

		user2ChatsNew, err := testRestClient.GetChats(ctx, user2)
		require.NoError(t, err, "error in getting chats")
		assert.Equal(t, 2, len(user2ChatsNew))
		chat1OfUser2New := user2ChatsNew[0]
		chat2OfUser2New := user2ChatsNew[1]
		assert.Equal(t, int64(0), chat1OfUser2New.UnreadMessages)
		assert.Equal(t, int64(0), chat2OfUser2New.UnreadMessages)

		user2HasUnreadMessagesNew, err := testRestClient.GetHasUnreadMessages(ctx, user2)
		require.NoError(t, err, "error in getting has unread messages")
		assert.Equal(t, false, user2HasUnreadMessagesNew)
	})
}

func TestCreateTetATetChat(t *testing.T) {
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

		avatar2 := "http://example.com/avatar-admin2.jpg"

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
			Avatar:           &avatar2,
			ShortInfo:        nil,
			LoginColor:       nil,
			LastSeenDateTime: nil,
			AdditionalData:   nil,
		}

		mockAaaClient := aaaRestClient.(*client.MockAaaRestClient)
		mockAaaClient.EXPECT().GetUsers(mock.Anything, mock.Anything).Return([]*dto.User{&mockUser1, &mockUser2}, nil)

		ctx := context.Background()

		chat1Id, err := testRestClient.CreateTetATetChat(ctx, user1, user2)
		require.NoError(t, err, "error in creating chat")
		assert.True(t, chat1Id > 0)
		require.NoError(t, kafka.WaitForAllEventsProcessed(lgr, cfg, saramaClient, lc), "error in waiting for processing events")

		user1Chats, err := testRestClient.GetChats(ctx, user1)
		require.NoError(t, err, "error in getting chats")
		assert.Equal(t, 1, len(user1Chats))
		chat1OfUser1 := user1Chats[0]
		assert.Equal(t, user2Login, chat1OfUser1.Title)
		assert.Equal(t, avatar2, *chat1OfUser1.Avatar)

		assert.Equal(t, []int64{2, 1}, chat1OfUser1.ParticipantIds)
		assert.Equal(t, user2Login, chat1OfUser1.Participants[0].Login)
		assert.Equal(t, user1Login, chat1OfUser1.Participants[1].Login)
	})
}

func TestResendMessage(t *testing.T) {
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

		const chat1Name = "new chat 1 src"
		const chat2Name = "new chat 1 dst"

		mockAaaClient := aaaRestClient.(*client.MockAaaRestClient)
		mockAaaClient.EXPECT().GetUsers(mock.Anything, mock.Anything).Return([]*dto.User{&mockUser1, &mockUser2}, nil)

		ctx := context.Background()

		chat1Id, err := testRestClient.CreateChat(ctx, user1, chat1Name, client.NewChatOptionResend(true))
		require.NoError(t, err, "error in creating chat")
		assert.True(t, chat1Id > 0)

		chat2Id, err := testRestClient.CreateChat(ctx, user2, chat2Name)
		require.NoError(t, err, "error in creating chat")
		assert.True(t, chat2Id > 0)
		require.NoError(t, kafka.WaitForAllEventsProcessed(lgr, cfg, saramaClient, lc), "error in waiting for processing events")

		// user 2 adds user 1 to chat 2
		err = testRestClient.AddChatParticipants(ctx, user2, chat2Id, []int64{user1})
		require.NoError(t, err, "error in adding participant")
		require.NoError(t, kafka.WaitForAllEventsProcessed(lgr, cfg, saramaClient, lc), "error in waiting for processing events")

		// assert user 1 sees chat 2
		user1ChatsNew, err := testRestClient.GetChats(ctx, user1)
		require.NoError(t, err, "error in getting chats")
		assert.Equal(t, 2, len(user1ChatsNew))
		chat1OfUser1 := user1ChatsNew[0]
		chat2OfUser1 := user1ChatsNew[1]
		assert.Equal(t, chat1Name, chat2OfUser1.Title)
		assert.Equal(t, chat2Name, chat1OfUser1.Title)

		const message1Text = "message 1 from chat 1"

		message1Id, err := testRestClient.CreateMessage(ctx, user1, chat1Id, message1Text)
		require.NoError(t, err, "error in creating message")
		require.NoError(t, kafka.WaitForAllEventsProcessed(lgr, cfg, saramaClient, lc), "error in waiting for processing events")

		// user 1 resends the message from chat 1 to chat 2
		message1ResentId, err := testRestClient.CreateMessage(ctx, user1, chat2Id, dto.NoMessageContent, client.NewMessageCreateOptionResend(chat1Id, message1Id))
		require.NoError(t, err, "error in resending message")
		require.NoError(t, kafka.WaitForAllEventsProcessed(lgr, cfg, saramaClient, lc), "error in waiting for processing events")

		// assert that chat 2 contains the embed message
		chat2Messages, err := testRestClient.GetMessages(ctx, user1, chat2Id)
		require.NoError(t, err, "error in getting messages")
		assert.Equal(t, 1, len(chat2Messages))
		resentMessage1 := chat2Messages[0]
		assert.Equal(t, message1ResentId, resentMessage1.Id)
		require.NotNil(t, resentMessage1.EmbedMessage)
		assert.Equal(t, dto.EmbedMessageTypeResend, resentMessage1.EmbedMessage.EmbedType)
		assert.Equal(t, message1Text, resentMessage1.EmbedMessage.Text)
		assert.Equal(t, message1Id, resentMessage1.EmbedMessage.Id)
		assert.Equal(t, chat1Id, *resentMessage1.EmbedMessage.ChatId)
		assert.Equal(t, chat1Name, *resentMessage1.EmbedMessage.ChatName)
		assert.Equal(t, user1, resentMessage1.EmbedMessage.Owner.Id)
		assert.Equal(t, user1Login, resentMessage1.EmbedMessage.Owner.Login)
	})
}

func TestReplyMessage(t *testing.T) {
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

		// user 1 adds user 2 to chat 1
		err = testRestClient.AddChatParticipants(ctx, user1, chat1Id, []int64{user2})
		require.NoError(t, err, "error in adding participant")
		require.NoError(t, kafka.WaitForAllEventsProcessed(lgr, cfg, saramaClient, lc), "error in waiting for processing events")

		// assert user 2 sees chat 1
		user2ChatsNew, err := testRestClient.GetChats(ctx, user2)
		require.NoError(t, err, "error in getting chats")
		assert.Equal(t, 1, len(user2ChatsNew))
		chat1OfUser2 := user2ChatsNew[0]
		assert.Equal(t, chat1Name, chat1OfUser2.Title)

		const message1Text = "new message 1"

		message1Id, err := testRestClient.CreateMessage(ctx, user2, chat1Id, message1Text)
		require.NoError(t, err, "error in creating message")
		require.NoError(t, kafka.WaitForAllEventsProcessed(lgr, cfg, saramaClient, lc), "error in waiting for processing events")

		const message2Text = "It is a reply"

		// user 1 replies on the message of user 2
		message1ResentId, err := testRestClient.CreateMessage(ctx, user1, chat1Id, message2Text, client.NewMessageCreateOptionReply(message1Id))
		require.NoError(t, err, "error in resending message")
		require.NoError(t, kafka.WaitForAllEventsProcessed(lgr, cfg, saramaClient, lc), "error in waiting for processing events")

		// assert that chat 1 contains the embed message
		{
			chat1Messages, err := testRestClient.GetMessages(ctx, user1, chat1Id)
			require.NoError(t, err, "error in getting messages")
			assert.Equal(t, 2, len(chat1Messages))
			repliedMessage2 := chat1Messages[1]
			assert.Equal(t, message1ResentId, repliedMessage2.Id)
			assert.Equal(t, message2Text, repliedMessage2.Content)
			require.NotNil(t, repliedMessage2.EmbedMessage)
			assert.Equal(t, dto.EmbedMessageTypeReply, repliedMessage2.EmbedMessage.EmbedType)
			assert.Equal(t, message1Text, repliedMessage2.EmbedMessage.Text)
			assert.Equal(t, message1Id, repliedMessage2.EmbedMessage.Id)
			assert.Equal(t, user2, repliedMessage2.EmbedMessage.Owner.Id)
			assert.Equal(t, user2Login, repliedMessage2.EmbedMessage.Owner.Login)
		}

		const message2TextNew = "It is a reply new"
		err = testRestClient.EditMessage(ctx, user1, chat1Id, message1ResentId, message2TextNew, client.NewMessageCreateOptionReply(message1Id))
		require.NoError(t, err, "error in resending message")
		require.NoError(t, kafka.WaitForAllEventsProcessed(lgr, cfg, saramaClient, lc), "error in waiting for processing events")

		// assert that chat 1 contains the embed message
		{
			chat1Messages, err := testRestClient.GetMessages(ctx, user1, chat1Id)
			require.NoError(t, err, "error in getting messages")
			assert.Equal(t, 2, len(chat1Messages))
			repliedMessage2 := chat1Messages[1]
			assert.Equal(t, message1ResentId, repliedMessage2.Id)
			assert.Equal(t, message2TextNew, repliedMessage2.Content)
			require.NotNil(t, repliedMessage2.EmbedMessage)
			assert.Equal(t, dto.EmbedMessageTypeReply, repliedMessage2.EmbedMessage.EmbedType)
			assert.Equal(t, message1Text, repliedMessage2.EmbedMessage.Text)
			assert.Equal(t, message1Id, repliedMessage2.EmbedMessage.Id)
			assert.Equal(t, user2, repliedMessage2.EmbedMessage.Owner.Id)
			assert.Equal(t, user2Login, repliedMessage2.EmbedMessage.Owner.Login)
		}

		// remove reply
		const message2TextNewest = "It is a view without reply"
		err = testRestClient.EditMessage(ctx, user1, chat1Id, message1ResentId, message2TextNewest)
		require.NoError(t, err, "error in resending message")
		require.NoError(t, kafka.WaitForAllEventsProcessed(lgr, cfg, saramaClient, lc), "error in waiting for processing events")

		// assert that chat 1 contains the embed message
		{
			chat1Messages, err := testRestClient.GetMessages(ctx, user1, chat1Id)
			require.NoError(t, err, "error in getting messages")
			assert.Equal(t, 2, len(chat1Messages))
			repliedMessage2 := chat1Messages[1]
			assert.Equal(t, message1ResentId, repliedMessage2.Id)
			assert.Nil(t, repliedMessage2.EmbedMessage)
			assert.Equal(t, message2TextNewest, repliedMessage2.Content)
		}

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

		user1Chats, err := testRestClient.GetChats(ctx, user1)
		require.NoError(t, err, "error in getting chats")
		assert.Equal(t, 1, len(user1Chats))
		chat1OfUser1 := user1Chats[0]
		assert.Equal(t, false, chat1OfUser1.Pinned)
		assert.Equal(t, chat1Name, chat1OfUser1.Title)
		assert.Equal(t, int64(0), chat1OfUser1.UnreadMessages)

		user2Chats, err := testRestClient.GetChats(ctx, user2)
		require.NoError(t, err, "error in getting chats")
		assert.Equal(t, 0, len(user2Chats))

		chat1Messages, err := testRestClient.GetMessages(ctx, user1, chat1Id)
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

		user2ChatsNew, err := testRestClient.GetChats(ctx, user2)
		require.NoError(t, err, "error in getting chats")
		assert.Equal(t, 1, len(user2ChatsNew))
		chat1OfUser2 := user2ChatsNew[0]
		assert.Equal(t, false, chat1OfUser2.Pinned)
		assert.Equal(t, chat1Name, chat1OfUser2.Title)
		assert.Equal(t, int64(1), chat1OfUser2.UnreadMessages)

		err = testRestClient.PinChat(ctx, user1, chat1Id, true)
		require.NoError(t, err, "error in pinning chats")
		require.NoError(t, kafka.WaitForAllEventsProcessed(lgr, cfg, saramaClient, lc), "error in waiting for processing events")

		user1ChatsNew2, err := testRestClient.GetChats(ctx, user1)
		require.NoError(t, err, "error in getting chats")
		assert.Equal(t, 1, len(user1ChatsNew2))
		chat1OfUser1New2 := user1ChatsNew2[0]
		assert.Equal(t, true, chat1OfUser1New2.Pinned)
		assert.Equal(t, chat1Name, chat1OfUser1New2.Title)
		assert.Equal(t, int64(0), chat1OfUser1New2.UnreadMessages)
		assert.Equal(t, message1.Id, *chat1OfUser1New2.LastMessageId)
		assert.Equal(t, message1.Content, *chat1OfUser1New2.LastMessageContent)

		user2ChatsNew2, err := testRestClient.GetChats(ctx, user2)
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

		user1Chats, err := testRestClient.GetChats(ctx, user1)
		require.NoError(t, err, "error in getting chats")
		assert.Equal(t, 1, len(user1Chats))
		chat1OfUser1 := user1Chats[0]
		assert.Equal(t, false, chat1OfUser1.Pinned)
		assert.Equal(t, chat1Name, chat1OfUser1.Title)
		assert.Equal(t, int64(0), chat1OfUser1.UnreadMessages)

		user2Chats, err := testRestClient.GetChats(ctx, user2)
		require.NoError(t, err, "error in getting chats")
		assert.Equal(t, 0, len(user2Chats))

		chat1Messages, err := testRestClient.GetMessages(ctx, user1, chat1Id)
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

		user2ChatsNew, err := testRestClient.GetChats(ctx, user2)
		require.NoError(t, err, "error in getting chats")
		assert.Equal(t, 1, len(user2ChatsNew))
		chat1OfUser2 := user2ChatsNew[0]
		assert.Equal(t, false, chat1OfUser2.Pinned)
		assert.Equal(t, chat1Name, chat1OfUser2.Title)
		assert.Equal(t, int64(1), chat1OfUser2.UnreadMessages)

		err = testRestClient.DeleteChat(ctx, user1, chat1Id)
		require.NoError(t, err, "error in removing chats")
		require.NoError(t, kafka.WaitForAllEventsProcessed(lgr, cfg, saramaClient, lc), "error in waiting for processing events")

		user1ChatsNew2, err := testRestClient.GetChats(ctx, user1)
		require.NoError(t, err, "error in getting chats")
		assert.Equal(t, 0, len(user1ChatsNew2))

		user2ChatsNew2, err := testRestClient.GetChats(ctx, user2)
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

		user1Chats, err := testRestClient.GetChats(ctx, user1)
		require.NoError(t, err, "error in getting chats")
		assert.Equal(t, 1, len(user1Chats))
		chat1OfUser1 := user1Chats[0]
		assert.Equal(t, chat1Name, chat1OfUser1.Title)
		assert.Equal(t, int64(0), chat1OfUser1.UnreadMessages)
		assert.Equal(t, int64(1), chat1OfUser1.ParticipantsCount)
		assert.Equal(t, []int64{1}, chat1OfUser1.ParticipantIds)

		user2Chats, err := testRestClient.GetChats(ctx, user2)
		require.NoError(t, err, "error in getting chats")
		assert.Equal(t, 0, len(user2Chats))

		chat1Messages, err := testRestClient.GetMessages(ctx, user1, chat1Id)
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

		user2ChatsNew, err := testRestClient.GetChats(ctx, user2)
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
		avatar := "http://example.com/avatar.jpg"
		avatarBig := "http://example.com/avatar-big.jpg"

		err = testRestClient.EditChat(ctx, user1, chat1Id, chat1NewName, client.NewChatOptionAvatar(&avatar, &avatarBig))
		require.NoError(t, err, "error in changing chat")
		require.NoError(t, kafka.WaitForAllEventsProcessed(lgr, cfg, saramaClient, lc), "error in waiting for processing events")

		user1ChatsNew2, err := testRestClient.GetChats(ctx, user1)
		require.NoError(t, err, "error in getting chats")
		assert.Equal(t, 1, len(user1ChatsNew2))
		chat1OfUser1New2 := user1ChatsNew2[0]
		assert.Equal(t, chat1NewName, chat1OfUser1New2.Title)
		assert.Equal(t, avatar, *chat1OfUser1New2.Avatar)
		assert.Equal(t, avatarBig, *chat1OfUser1New2.AvatarBig)
		assert.Equal(t, int64(0), chat1OfUser1New2.UnreadMessages)
		assert.Equal(t, message1.Id, *chat1OfUser1New2.LastMessageId)
		assert.Equal(t, message1.Content, *chat1OfUser1New2.LastMessageContent)
		assert.Equal(t, int64(2), chat1OfUser1New2.ParticipantsCount)
		assert.Equal(t, []int64{2, 1}, chat1OfUser1New2.ParticipantIds)

		user2ChatsNew2, err := testRestClient.GetChats(ctx, user2)
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

		user1Chats, err := testRestClient.GetChats(ctx, user1)
		require.NoError(t, err, "error in getting chats")
		assert.Equal(t, 1, len(user1Chats))
		chat1OfUser1 := user1Chats[0]
		assert.Equal(t, chat1Name, chat1OfUser1.Title)
		assert.Equal(t, int64(0), chat1OfUser1.UnreadMessages)
		assert.Equal(t, int64(1), chat1OfUser1.ParticipantsCount)
		assert.Equal(t, []int64{1}, chat1OfUser1.ParticipantIds)

		user2Chats, err := testRestClient.GetChats(ctx, user2)
		require.NoError(t, err, "error in getting chats")
		assert.Equal(t, 0, len(user2Chats))

		chat1Messages, err := testRestClient.GetMessages(ctx, user1, chat1Id)
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

		user2ChatsNew, err := testRestClient.GetChats(ctx, user2)
		require.NoError(t, err, "error in getting chats")
		assert.Equal(t, 1, len(user2ChatsNew))
		chat1OfUser2 := user2ChatsNew[0]
		assert.Equal(t, chat1Name, chat1OfUser2.Title)
		assert.Equal(t, int64(1), chat1OfUser2.UnreadMessages)
		assert.Equal(t, message1.Id, *chat1OfUser2.LastMessageId)
		assert.Equal(t, message1.Content, *chat1OfUser2.LastMessageContent)
		assert.Equal(t, int64(2), chat1OfUser2.ParticipantsCount)
		assert.Equal(t, []int64{2, 1}, chat1OfUser2.ParticipantIds)

		user2HasUnreadMessages, err := testRestClient.GetHasUnreadMessages(ctx, user2)
		require.NoError(t, err, "error in getting has unread messages")
		assert.Equal(t, true, user2HasUnreadMessages)

		err = testRestClient.DeleteChatParticipants(ctx, user1, chat1Id, []int64{user2})
		require.NoError(t, err, "error in removing chat participants")
		require.NoError(t, kafka.WaitForAllEventsProcessed(lgr, cfg, saramaClient, lc), "error in waiting for processing events")

		user2ChatsNew2, err := testRestClient.GetChats(ctx, user2)
		require.NoError(t, err, "error in getting chats")
		assert.Equal(t, 0, len(user2ChatsNew2))

		// after removing from chat user 2 got no unread messages
		user2HasUnreadMessagesNew2, err := testRestClient.GetHasUnreadMessages(ctx, user2)
		require.NoError(t, err, "error in getting has unread messages")
		assert.Equal(t, false, user2HasUnreadMessagesNew2)

		chat1Participants2, err := testRestClient.GetChatParticipants(ctx, chat1Id)
		require.NoError(t, err, "error in chat participants")
		require.Equal(t, 1, len(chat1Participants2))
		assert.Equal(t, user1, chat1Participants2[0].Id)

		user1ChatsNew2, err := testRestClient.GetChats(ctx, user1)
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

		user2ChatsNew, err := testRestClient.GetChats(ctx, user2)
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

		user1Chats, err := testRestClient.GetChats(ctx, user1)
		require.NoError(t, err, "error in getting chats")
		assert.Equal(t, 1, len(user1Chats))
		chat1OfUser1 := user1Chats[0]
		assert.Equal(t, chat1Name, chat1OfUser1.Title)
		assert.Equal(t, int64(0), chat1OfUser1.UnreadMessages)
		assert.Equal(t, message2Text, *chat1OfUser1.LastMessageContent)

		chat1Messages, err := testRestClient.GetMessages(ctx, user1, chat1Id)
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

		user1ChatsNew, err := testRestClient.GetChats(ctx, user1)
		require.NoError(t, err, "error in getting chats")
		assert.Equal(t, 1, len(user1ChatsNew))
		chat1OfUser1New := user1ChatsNew[0]
		assert.Equal(t, chat1Name, chat1OfUser1New.Title)
		assert.Equal(t, int64(0), chat1OfUser1New.UnreadMessages)
		assert.Equal(t, message2Text, *chat1OfUser1New.LastMessageContent)

		chat1MessagesNew, err := testRestClient.GetMessages(ctx, user1, chat1Id)
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

		user1ChatsNew2, err := testRestClient.GetChats(ctx, user1)
		require.NoError(t, err, "error in getting chats")
		assert.Equal(t, 1, len(user1ChatsNew2))
		chat1OfUser1New2 := user1ChatsNew2[0]
		assert.Equal(t, chat1Name, chat1OfUser1New2.Title)
		assert.Equal(t, int64(0), chat1OfUser1New2.UnreadMessages)
		assert.Equal(t, message2TextNew, *chat1OfUser1New2.LastMessageContent)

		chat1MessagesNew2, err := testRestClient.GetMessages(ctx, user1, chat1Id)
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

		// actually not needed
		// dummy old behaviour. just check for backward compatibility
		// actually just marking message as blog is enough
		err = testRestClient.EditChat(ctx, user1, chat1Id, chat1Name, client.NewChatOptionBlog(true))
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

		err = testRestClient.EditChat(ctx, user1, chat1Id, chat1Name, client.NewChatOptionBlog(false))
		require.NoError(t, err, "error in unmaking message blog post")
		require.NoError(t, kafka.WaitForAllEventsProcessed(lgr, cfg, saramaClient, lc), "error in waiting for processing events")
		blogsNew, err := testRestClient.SearchBlogs(ctx)
		require.NoError(t, err, "error in searching blog posts")
		assert.Equal(t, 0, len(blogsNew))
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
		resp1, err := testRestClient.GetChats(ctx, user1, client.NewChatGetOptionWithSize(40))
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
		resp2, err := testRestClient.GetChats(ctx, user1, client.NewChatGetOptionWithSize(40), client.NewChatGetOptionWithStartsFromChatPinned(lastPinned), client.NewChatGetOptionWithStartsFromChatLastUpdateDateTime(lastLastUpdateDateTime), client.NewChatGetOptionWithStartsFromChatId(lastId))
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
		resp1, err := testRestClient.GetMessages(ctx, user1, chat1Id, client.NewMessageGetOptionWithSize(3), client.NewMessageGetOptionWithStartsFromItemId(6))
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
		resp2, err := testRestClient.GetMessages(ctx, user1, chat1Id, client.NewMessageGetOptionWithSize(3), client.NewMessageGetOptionWithStartsFromItemId(lastId))
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
