package dto

type IdResponse struct {
	Id int64 `json:"id"`
}

type ErrorMessageDto struct {
	Message string `json:"message"`
}

type ChatCreateDto struct {
	Title          string  `json:"title"`
	ParticipantIds []int64 `json:"participantIds"`
	CanResend      bool    `json:"canResend"`
	Blog           bool    `json:"blog"`
	Avatar         *string `json:"avatar"`
	AvatarBig      *string `json:"avatarBig"`
}

type ChatEditDto struct {
	Id int64 `json:"id"`
	ChatCreateDto
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

type HasUnreadMessages struct {
	HasUnreadMessages bool `json:"hasUnreadMessages"`
}

type PutChatNotificationSettingsDto struct {
	ConsiderMessagesOfThisChatAsUnread bool `json:"considerMessagesOfThisChatAsUnread"`
}
