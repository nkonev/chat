package dto

type IdResponse struct {
	Id int64 `json:"id"`
}

type ChatCreateDto struct {
	Title          string  `json:"title"`
	ParticipantIds []int64 `json:"participantIds"`
	CanResend      bool    `json:"canResend"`
}

type ChatEditDto struct {
	Id int64 `json:"id"`
	ChatCreateDto
	Blog bool `json:"blog"`
}

const EmbedMessageTypeResend = "resend"
const EmbedMessageTypeReply = "reply"

type EmbedMessageRequest struct {
	Id        int64  `json:"id"`
	ChatId    int64  `json:"chatId"` // chat from (src)
	EmbedType string `json:"embedType"`
}

type MessageCreateDto struct {
	Content             string               `json:"content"`
	EmbedMessageRequest *EmbedMessageRequest `json:"embedMessage"`
}

type MessageEditDto struct {
	Id int64 `json:"id"`
	MessageCreateDto
}

type ParticipantAddDto struct {
	ParticipantIds []int64 `json:"participantIds"`
}

type ParticipantDeleteDto struct {
	ParticipantIds []int64 `json:"participantIds"`
}
