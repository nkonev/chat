package cqrs

import (
	"go-cqrs-chat-example/utils"
	"time"
)

type AdditionalData struct {
	CreatedAt time.Time `json:"createdAt"`
}

type ChatCreated struct {
	AdditionalData *AdditionalData `json:"additionalData"`
	ChatId         int64           `json:"chatId"`
	Title          string          `json:"title"`
	CanResend      bool            `json:"canResend"`
	TetATet        bool            `json:"tetATet"`
	Blog           bool            `json:"blog"`
	Avatar         *string         `json:"avatar"`
	AvatarBig      *string         `json:"avatarBig"`
}

type ChatEdited struct {
	AdditionalData *AdditionalData `json:"additionalData"`
	ChatId         int64           `json:"chatId"`
	Title          string          `json:"title"`
	Blog           bool            `json:"blog"`
	BehalfUserId   int64           `json:"behalfUserId"`
	CanResend      bool            `json:"canResend"`
	Avatar         *string         `json:"avatar"`
	AvatarBig      *string         `json:"avatarBig"`
}

type ChatDeleted struct {
	AdditionalData *AdditionalData `json:"additionalData"`
	ChatId         int64           `json:"chatId"`
	BehalfUserId   int64           `json:"behalfUserId"`
}

type ParticipantsAdded struct {
	AdditionalData     *AdditionalData        `json:"additionalData"`
	Participants       []ParticipantWithAdmin `json:"participants"`
	Admins             bool                   `json:"admins"`
	ChatId             int64                  `json:"chatId"`
	BehalfUserId       int64                  `json:"behalfUserId"`
	SkipChatAdminCheck bool                   `json:"skipChatAdminCheck"`
}

type ParticipantDeleted struct {
	AdditionalData *AdditionalData `json:"additionalData"`
	ParticipantIds []int64         `json:"participantIds"`
	ChatId         int64           `json:"chatId"`
	BehalfUserId   int64           `json:"behalfUserId"`
}

type ParticipantChanged struct {
	AdditionalData *AdditionalData `json:"additionalData"`
	ParticipantId  int64           `json:"participantId"`
	ChatId         int64           `json:"chatId"`
	BehalfUserId   int64           `json:"behalfUserId"`
	NewAdmin       bool            `json:"newAdmin"`
}

type ChatPinned struct {
	AdditionalData *AdditionalData `json:"additionalData"`
	ParticipantId  int64           `json:"participantId"`
	ChatId         int64           `json:"chatId"`
	Pinned         bool            `json:"pinned"`
}

type ChatNotificationSettingsSetted struct {
	AdditionalData *AdditionalData `json:"additionalData"`
	ParticipantId  int64           `json:"participantId"`
	ChatId         int64           `json:"chatId"`
	Setted         bool            `json:"setted"`
}

type MessageCommoned struct {
	Id      int64  `json:"id"` // message id
	ChatId  int64  `json:"chatId"`
	Content string `json:"content"`

	EmbedMessageId      *int64  `json:"embedMessageId"`
	EmbedMessageType    *string `json:"embedMessageType"`
	EmbedMessageChatId  *int64  `json:"embedMessageChatId"`
	EmbedMessageOwnerId *int64  `json:"embedMessageOwnerId"`
}

type MessageCreated struct {
	MessageCommoned
	AdditionalData *AdditionalData `json:"additionalData"`
	OwnerId        int64           `json:"ownerId"`
}

type MessageEdited struct {
	MessageCommoned
	AdditionalData *AdditionalData `json:"additionalData"`
}

type MessageEmbedded struct {
	Id        int64  `json:"id"`
	ChatId    int64  `json:"chatId"`
	EmbedType string `json:"embedType"`
}

type UnreadMessagesAction int16

const (
	UnreadMessagesActionRefresh = iota + 1
	UnreadMessagesActionIncrease
)

type LastMessageAction int16

const (
	LastMessageActionRefresh = iota + 1
)

type ParticipantsAction int16

const (
	// represents action to update participants
	// see also OnParticipantAdd - there chat_user_view is created
	ParticipantsActionRefresh = iota + 1
)

type ChatViewRefreshed struct {
	AdditionalData       *AdditionalData      `json:"additionalData"`
	ParticipantIds       []int64              `json:"participantIds"`
	ChatId               int64                `json:"chatId"`
	UnreadMessagesAction UnreadMessagesAction `json:"unreadMessagesAction"`
	LastMessageAction    LastMessageAction    `json:"lastMessageAction"`
	ParticipantsAction   ParticipantsAction   `json:"participantsAction"`
	IncreaseOn           int                  `json:"increaseOn"`
	OwnerId              int64                `json:"ownerId"` // owner of message
}

type MessageReaded struct {
	AdditionalData *AdditionalData `json:"additionalData"`
	ParticipantId  int64           `json:"participantId"`
	ChatId         int64           `json:"chatId"`
	MessageId      int64           `json:"messageId"`
}

type MessageBlogPostMade struct {
	AdditionalData *AdditionalData `json:"additionalData"`
	ChatId         int64           `json:"chatId"`
	MessageId      int64           `json:"messageId"`
	BlogPost       bool            `json:"blogPost"`
	BehalfUserId   int64           `json:"behalfUserId"`
}

type MessageDeleted struct {
	AdditionalData *AdditionalData `json:"additionalData"`
	ChatId         int64           `json:"chatId"`
	MessageId      int64           `json:"messageId"`
}

func GenerateMessageAdditionalData() *AdditionalData {
	return &AdditionalData{
		CreatedAt: time.Now().UTC(),
	}
}

func (s *ChatCreated) GetPartitionKey() string {
	return utils.ToString(s.ChatId)
}

func (s *ChatEdited) GetPartitionKey() string {
	return utils.ToString(s.ChatId)
}

func (s *ChatDeleted) GetPartitionKey() string {
	return utils.ToString(s.ChatId)
}

func (s *ParticipantsAdded) GetPartitionKey() string {
	return utils.ToString(s.ChatId)
}

func (s *ParticipantDeleted) GetPartitionKey() string {
	return utils.ToString(s.ChatId)
}

func (s *ParticipantChanged) GetPartitionKey() string {
	return utils.ToString(s.ChatId)
}

func (s *ChatPinned) GetPartitionKey() string {
	return utils.ToString(s.ChatId)
}

func (s *ChatNotificationSettingsSetted) GetPartitionKey() string {
	return utils.ToString(s.ChatId)
}

func (s *MessageCreated) GetPartitionKey() string {
	return utils.ToString(s.ChatId)
}

func (s *MessageEdited) GetPartitionKey() string {
	return utils.ToString(s.ChatId)
}

func (s *ChatViewRefreshed) GetPartitionKey() string {
	return utils.ToString(s.ChatId)
}

func (s *MessageReaded) GetPartitionKey() string {
	return utils.ToString(s.ChatId)
}

func (s *MessageBlogPostMade) GetPartitionKey() string {
	return utils.ToString(s.ChatId)
}

func (s *MessageDeleted) GetPartitionKey() string {
	return utils.ToString(s.ChatId)
}

func (s *ChatCreated) Name() string {
	return "chatCreated"
}

func (s *ChatEdited) Name() string {
	return "chatEdited"
}

func (s *ChatDeleted) Name() string {
	return "chatDeleted"
}

func (s *ParticipantsAdded) Name() string {
	return "participantsAdded"
}

func (s *ParticipantDeleted) Name() string {
	return "participantDeleted"
}

func (s *ParticipantChanged) Name() string {
	return "participantChanged"
}

func (s *ChatPinned) Name() string {
	return "chatPinned"
}

func (s *ChatNotificationSettingsSetted) Name() string {
	return "chatNotificationSettingsSetted"
}

func (s *MessageCreated) Name() string {
	return "messageCreated"
}

func (s *MessageEdited) Name() string {
	return "messageEdited"
}

func (s *ChatViewRefreshed) Name() string {
	return "chatViewRefreshed"
}

func (s *MessageReaded) Name() string {
	return "messageReaded"
}

func (s *MessageBlogPostMade) Name() string {
	return "messageBlogPostMade"
}

func (s *MessageDeleted) Name() string {
	return "messageDeleted"
}
