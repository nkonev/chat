package dto

import (
	"time"
)

type ChatViewDto struct {
	Id                 int64      `json:"id"`
	Title              string     `json:"title"`
	Pinned             bool       `json:"pinned"`
	UnreadMessages     int64      `json:"unreadMessages"`
	LastMessageId      *int64     `json:"lastMessageId"`
	LastMessageOwnerId *int64     `json:"lastMessageOwnerId"`
	LastMessageContent *string    `json:"lastMessageContent"`
	ParticipantsCount  int64      `json:"participantsCount"`
	ParticipantIds     []int64    `json:"participantIds"` // ids of last N participants
	Blog               bool       `json:"blog"`
	UpdateDateTime     *time.Time `json:"lastUpdateDateTime"` // for sake compatibility
}

type ChatId struct {
	Pinned             bool
	LastUpdateDateTime time.Time
	Id                 int64
}

type ChatViewEnrichedDto struct {
	ChatViewDto
	Participants []User `json:"participants"`
}

type ChatBasic struct {
	Id        int64
	Title     string
	CanResend bool
	TetATet   bool
}

type BasicChatDtoExtended struct {
	ChatBasic
	BehalfUserIsParticipant bool
}
