package dto

import (
	"time"
)

const NoMessageContent = ""
const NoOwner = -1

type EmbedMessageType string

const EmbedMessageTypeReply = "reply"
const EmbedMessageTypeResend = "resend"

type MessageDto struct {
	Id             int64
	OwnerId        int64
	Content        string
	BlogPost       bool
	Embed          Embeddable
	CreateDateTime time.Time
	UpdateDateTime *time.Time
	FileItemUuid   *string
	UserId         int64
}

type EmbedTyper struct {
	Type EmbedMessageType `json:"embedMessageType"`
}

type Embeddable interface {
	GetType() EmbedMessageType
}

type EmbedReply struct {
	EmbedTyper
	MessageId      int64  `json:"embedMessageId"`
	MessageContent string `json:"embedMessageContent"`
	OwnerId        int64  `json:"embedOwnerId"`
}

func NewEmbedReply(messageId int64, messageContent string, ownerId int64) *EmbedReply {
	return &EmbedReply{
		EmbedTyper: EmbedTyper{
			Type: EmbedMessageTypeReply,
		},
		MessageId:      messageId,
		MessageContent: messageContent,
		OwnerId:        ownerId,
	}
}

func (p *EmbedReply) GetType() EmbedMessageType {
	return EmbedMessageTypeReply
}

type EmbedResend struct {
	EmbedTyper
	MessageId      int64  `json:"embedMessageId"`
	MessageContent string `json:"embedMessageContent"`
	OwnerId        int64  `json:"embedOwnerId"`
	ChatId         int64  `json:"embedChatId"`
}

func NewEmbedResend(messageId int64, messageContent string, ownerId int64, chatId int64) *EmbedResend {
	return &EmbedResend{
		EmbedTyper: EmbedTyper{
			EmbedMessageTypeResend,
		},
		MessageId:      messageId,
		MessageContent: messageContent,
		OwnerId:        ownerId,
		ChatId:         chatId,
	}
}

func (p *EmbedResend) GetType() EmbedMessageType {
	return EmbedMessageTypeResend
}

type EmbedMessageResponse struct {
	Id            int64   `json:"id"`
	ChatId        *int64  `json:"chatId"`
	ChatName      *string `json:"chatName"`
	Text          string  `json:"text"`
	Owner         *User   `json:"owner"`
	EmbedType     string  `json:"embedType"`
	IsParticipant bool    `json:"isParticipant"`
}

type MessageViewEnrichedDto struct {
	Id             int64                 `json:"id"`
	ChatId         int64                 `json:"chatId"`
	OwnerId        int64                 `json:"ownerId"`
	Content        string                `json:"text"` // for sake compatibility
	BlogPost       bool                  `json:"blogPost"`
	EmbedMessage   *EmbedMessageResponse `json:"embedMessage"`
	CreateDateTime time.Time             `json:"createDateTime"`
	UpdateDateTime *time.Time            `json:"editDateTime"` // for sake compatibility
	FileItemUuid   *string               `json:"fileItemUuid"`

	Owner     *User             `json:"owner"`
	Reactions []ReactionViewDto `json:"reactions"`

	CanEdit      bool `json:"canEdit"`
	CanSyncEmbed bool `json:"canSyncEmbed"`
	CanDelete    bool `json:"canDelete"`
	CanPublish   bool `json:"canPublish"`
	CanPin       bool `json:"canPin"`

	UserId int64 `json:"-"` // behalf user id
}

type MessagesResponseDto struct {
	Items   []MessageViewEnrichedDto `json:"items"`
	HasNext bool                     `json:"hasNext"`
}

type MessageDeletedDto struct {
	Id     int64 `json:"id"`
	ChatId int64 `json:"chatId"`
}

type MessageBroadcastNotification struct {
	Login  string `json:"login"`
	UserId int64  `json:"userId"`
	Text   string `json:"text"`
}

type PinnedMessageEvent struct {
	Message    PinnedMessageDto `json:"message"`
	TotalCount int64            `json:"totalCount"`
}

type PublishedMessageEvent struct {
	Message    PublishedMessageDto `json:"message"`
	TotalCount int64               `json:"totalCount"`
}

type ReactionChangedEvent struct {
	MessageId int64    `json:"messageId"`
	Reaction  Reaction `json:"reaction"`
}

type PinnedMessageDto struct {
	Id             int64     `json:"id"`
	Text           string    `json:"text"`
	ChatId         int64     `json:"chatId"`
	OwnerId        int64     `json:"ownerId"`
	Owner          *User     `json:"owner"`
	PinnedPromoted bool      `json:"pinnedPromoted"`
	CreateDateTime time.Time `json:"createDateTime"`
	CanPin         bool      `json:"canPin"`
}

type Reaction struct {
	Count    int64   `json:"count"`
	Users    []*User `json:"users"`
	Reaction string  `json:"reaction"`
}

type PublishedMessageDto struct {
	Id             int64     `json:"id"`
	Text           string    `json:"text"`
	ChatId         int64     `json:"chatId"`
	OwnerId        int64     `json:"ownerId"`
	Owner          *User     `json:"owner"`
	CanPublish     bool      `json:"canPublish"`
	CreateDateTime time.Time `json:"createDateTime"`
}

type MessageBasic struct {
	Id           int64   `db:"id"`
	OwnerId      int64   `db:"owner_id"`
	Content      string  `db:"content"`
	BlogPost     bool    `db:"blog_post"`
	Published    bool    `db:"published"`
	FileItemUuid *string `db:"file_item_uuid"`
}

type ReactionPutDto struct {
	Reaction string `json:"reaction"`
}

type ReactionDto struct {
	MessageId int64
	UserIds   []int64
	Reaction  string
	Count     int64
}

type ReactionViewDto struct {
	Count    int64   `json:"count"`
	Users    []*User `json:"users"`
	Reaction string  `json:"reaction"`
}

type HasUnreadMessagesChanged struct {
	HasUnreadMessages bool `json:"hasUnreadMessages"`
}

type ChatUnreadMessageChanged struct {
	ChatId             int64     `json:"chatId"`
	UnreadMessages     int64     `json:"unreadMessages"`
	LastUpdateDateTime time.Time `json:"lastUpdateDateTime"` // it's need to lift the chat in case adding new unread messages
}

type BroadcastDto struct {
	Text string `json:"text"`
}

type UserTypingNotification struct {
	Login         string `json:"login"`
	ParticipantId int64  `json:"participantId"`
	ChatId        int64  `json:"chatId"`
}

type MessageFilterDto struct {
	SearchString string `json:"searchString"`
	MessageId    int64  `json:"messageId"`
}

type CleanHtmlTagsRequestDto struct {
	Text  string `json:"text"`
	Login string `json:"login"`
}

type CleanHtmlTagsResponseDto struct {
	Text string `json:"text"`
}

type EmbedMessageRequest struct {
	Id        int64  `json:"id"`
	ChatId    int64  `json:"chatId"` // chat from (src)
	EmbedType string `json:"embedType"`
}

type MessageCreateDto struct {
	Content             string               `json:"text"`
	EmbedMessageRequest *EmbedMessageRequest `json:"embedMessage"`
	FileItemUuid        *string              `json:"fileItemUuid"`
}

type MessageEditDto struct {
	Id int64 `json:"id"`
	MessageCreateDto
}

type ParticipantsWrapper struct {
	Data  []*User `json:"participants"`
	Count int64   `json:"participantsCount"` // for paginating purposes
}

type MessageReadResponse struct {
	ParticipantsWrapper
	Text string `json:"text"`
}

type MessageAuthorizationData struct {
	IsParticipant       bool `db:"is_chat_participant"`
	IsChatAdmin         bool `db:"is_chat_admin"`
	ChatCanWriteMessage bool `db:"chat_can_write_message"`
}
