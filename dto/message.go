package dto

import "time"

const NoMessageContent = ""

type MessageViewDto struct {
	Id       int64
	OwnerId  int64
	Content  string
	BlogPost bool

	ResponseEmbeddedMessageType          *string
	ResponseEmbeddedMessageReplyId       *int64
	ResponseEmbeddedMessageReplyText     *string
	ResponseEmbeddedMessageReplyOwnerId  *int64
	ResponseEmbeddedMessageResendId      *int64
	ResponseEmbeddedMessageResendChatId  *int64
	ResponseEmbeddedMessageResendOwnerId *int64

	CreateDateTime time.Time  `json:"createDateTime"`
	UpdateDateTime *time.Time `json:"editDateTime"` // for sake compatibility
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
	OwnerId        int64                 `json:"ownerId"`
	Content        string                `json:"text"` // for sake compatibility
	BlogPost       bool                  `json:"blogPost"`
	EmbedMessage   *EmbedMessageResponse `json:"embedMessage"`
	CreateDateTime time.Time             `json:"createDateTime"`
	UpdateDateTime *time.Time            `json:"editDateTime"` // for sake compatibility

	Owner *User `json:"owner"`
}

type MessageBasic struct {
	Id      int64
	OwnerId int64
	Content string
}
