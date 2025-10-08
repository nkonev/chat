package dto

const NonExistentUser = -65000

type IdResponse struct {
	Id int64 `json:"id"`
}

type ErrorMessageDto struct {
	Message string `json:"message"`
}

type ChatCreateDto struct {
	Title          string  `json:"name"`
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

const ReservedPublicallyAvailableForSearchChats = "__AVAILABLE_FOR_SEARCH"

const EmbedMessageTypeResend = "resend"
const EmbedMessageTypeReply = "reply"

type EmbedMessageRequest struct {
	Id        int64  `json:"id"`
	ChatId    int64  `json:"chatId"` // chat from (src)
	EmbedType string `json:"embedType"`
}

type MessageCreateDto struct {
	Content             string               `json:"text"`
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

type SearchUsersRequestDto struct {
	Page         int64   `json:"page"`
	Size         int32   `json:"size"`
	UserIds      []int64 `json:"userIds"`
	SearchString string  `json:"searchString"`
	Including    bool    `json:"including"`
}

type SearchUsersResponseDto struct {
	Users []*User `json:"users"`
	Count int64   `json:"count"`
}

type FreshDto struct {
	Ok bool `json:"ok"`
}

type FilterDto struct {
	Found bool `json:"found"`
}
