package dto

type IdResponse struct {
	Id int64 `json:"id"`
}

type ChatCreateDto struct {
	Title          string  `json:"title"`
	ParticipantIds []int64 `json:"participantIds"`
}

type ChatEditDto struct {
	Id int64 `json:"id"`
	ChatCreateDto
	Blog bool `json:"blog"`
}

type MessageCreateDto struct {
	Content string `json:"content"`
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
