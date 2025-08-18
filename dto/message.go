package dto

import "time"

const NoMessageContent = ""

type MessageViewDto struct {
	Id       int64  `db:"id"`
	OwnerId  int64  `db:"owner_id"`
	Content  string `db:"content"`
	BlogPost bool   `db:"blog_post"`

	ResponseEmbeddedMessageType          *string `db:"embed_message_type"`
	ResponseEmbeddedMessageReplyId       *int64  `db:"embed_message_reply_id"`
	ResponseEmbeddedMessageReplyText     *string `db:"embed_message_reply_text"`
	ResponseEmbeddedMessageReplyOwnerId  *int64  `db:"embed_message_reply_owner_id"`
	ResponseEmbeddedMessageResendId      *int64  `db:"embed_message_resend_id"`
	ResponseEmbeddedMessageResendChatId  *int64  `db:"embed_message_resend_chat_id"`
	ResponseEmbeddedMessageResendOwnerId *int64  `db:"embed_message_resend_owner_id"`

	CreateDateTime time.Time  `db:"create_date_time"`
	UpdateDateTime *time.Time `db:"update_date_time"`
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

	Owner     *User             `json:"owner"`
	Reactions []ReactionViewDto `json:"reactions"`
}

type MessageBasic struct {
	Id      int64  `db:"id"`
	OwnerId int64  `db:"owner_id"`
	Content string `db:"content"`
}

type ReactionPutDto struct {
	Reaction string `json:"reaction"`
}

type ReactionDto struct {
	MessageId int64  `db:"message_id"`
	UserId    int64  `db:"user_id"`
	Reaction  string `db:"reaction"`
}

type ReactionViewDto struct {
	Count    int64   `json:"count"`
	Users    []*User `json:"users"`
	Reaction string  `json:"reaction"`
}
