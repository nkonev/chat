package dto

import (
	validation "github.com/go-ozzo/ozzo-validation/v4"
)

const ReservedPublicallyAvailableForSearchChats = "__AVAILABLE_FOR_SEARCH"

type IdResponse struct {
	Id int64 `json:"id"`
}

type ChatCreateDto struct {
	Title          string  `json:"title"`
	ParticipantIds []int64 `json:"participantIds"`
}

func (cc *ChatCreateDto) IsValidatabale() bool {
	return true
}

func (a *ChatCreateDto) Validate() error {
	return validation.ValidateStruct(a,
		validation.Field(&a.Title, validation.Required, validation.Length(minChatNameLen, maxChatNameLen), validation.NotIn(ReservedPublicallyAvailableForSearchChats)),
	)
}

type ChatEditDto struct {
	Id int64 `json:"id"`
	ChatCreateDto
	Blog bool `json:"blog"`
}

func (cc *ChatEditDto) IsValidatabale() bool {
	return true
}

func (a *ChatEditDto) Validate() error {
	return validation.ValidateStruct(a,
		validation.Field(&a.Title, validation.Required, validation.Length(minChatNameLen, maxChatNameLen), validation.NotIn(ReservedPublicallyAvailableForSearchChats)),
		validation.Field(&a.Id, validation.Required),
	)
}

const minChatNameLen = 1
const maxChatNameLen = 256

const EmbedMessageTypeResend = "resend"
const EmbedMessageTypeReply = "reply"

const maxMessageLen = 1024 * 1024
const minMessageLen = 1

type EmbedMessageRequest struct {
	Id        int64  `json:"id"`
	ChatId    int64  `json:"chatId"`
	EmbedType string `json:"embedType"`
}

type MessageCreateDto struct {
	Content             string               `json:"content"`
	EmbedMessageRequest *EmbedMessageRequest `json:"embedMessage"`
}

func (a *MessageCreateDto) Validate() error {
	return validation.ValidateStruct(a,
		validation.Field(&a.Content, validation.Required, validation.Length(minMessageLen, maxMessageLen)),
	)
}

func (mcd *MessageCreateDto) IsValidatabale() bool {
	return mcd.EmbedMessageRequest == nil || (mcd.EmbedMessageRequest != nil && mcd.EmbedMessageRequest.EmbedType == EmbedMessageTypeReply)
}

type MessageEditDto struct {
	Id int64 `json:"id"`
	MessageCreateDto
}

func (a *MessageEditDto) Validate() error {
	return validation.ValidateStruct(a,
		validation.Field(&a.Content, validation.Required, validation.Length(minMessageLen, maxMessageLen)),
		validation.Field(&a.Id, validation.Required),
	)
}

func (mcd *MessageEditDto) IsValidatabale() bool {
	return true
}

type ParticipantAddDto struct {
	ParticipantIds []int64 `json:"participantIds"`
}

type ParticipantDeleteDto struct {
	ParticipantIds []int64 `json:"participantIds"`
}
