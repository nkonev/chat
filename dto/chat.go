package dto

import (
	"time"
)

type ChatViewDto struct {
	Id                 int64      `json:"id" db:"id"`
	Title              string     `json:"title" db:"title"`
	Pinned             bool       `json:"pinned" db:"pinned"`
	UnreadMessages     int64      `json:"unreadMessages" db:"unread_messages"`
	LastMessageId      *int64     `json:"lastMessageId" db:"last_message_id"`
	LastMessageOwnerId *int64     `json:"lastMessageOwnerId" db:"last_message_owner_id"`
	LastMessageContent *string    `json:"lastMessageContent" db:"last_message_content"`
	ParticipantsCount  int64      `json:"participantsCount" db:"participants_count"`
	ParticipantIds     []int64    `json:"participantIds" db:"participant_ids"` // ids of last N participants
	Blog               bool       `json:"blog" db:"blog"`
	UpdateDateTime     *time.Time `json:"lastUpdateDateTime" db:"update_date_time"` // for sake compatibility
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
	Id        int64  `db:"id"`
	Title     string `db:"title"`
	CanResend bool   `db:"can_resend"`
	TetATet   bool   `db:"tet_a_tet"`
}

type BasicChatDtoExtended struct {
	ChatBasic
	BehalfUserIsParticipant bool `db:"behalf_user_is_participant"`
}
