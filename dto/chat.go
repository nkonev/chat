package dto

import (
	"time"
)

const NoChatTitle = ""
const NoSearchString = ""

type ChatViewDto struct {
	Id                                int64      `json:"id"`
	UserId                            int64      `json:"-"` // behalf user id
	Title                             string     `json:"title"`
	Pinned                            bool       `json:"pinned"`
	UnreadMessages                    int64      `json:"unreadMessages"`
	LastMessageId                     *int64     `json:"lastMessageId"`
	LastMessageOwnerId                *int64     `json:"lastMessageOwnerId"`
	LastMessageContent                *string    `json:"lastMessageContent"`
	ParticipantsCount                 int64      `json:"participantsCount"`
	ParticipantIds                    []int64    `json:"participantIds"` // ids of last N participants
	Blog                              bool       `json:"blog"`
	UpdateDateTime                    *time.Time `json:"lastUpdateDateTime"` // for sake compatibility
	TetATet                           bool       `json:"tetATet"`
	Avatar                            *string    `json:"avatar"`
	AvatarBig                         *string    `json:"avatarBig"`
	ConsiderMessagesAsUnread          bool       `json:"considerMessagesAsUnread"`
	RegularParticipantCanWriteMessage bool       `json:"regularParticipantCanWriteMessage"`
}

type ChatId struct {
	Pinned             bool
	LastUpdateDateTime time.Time
	Id                 int64
}

type ChatDeletedDto struct {
	Id int64 `json:"id"`
}

type ChatViewEnrichedDto struct {
	ChatViewDto
	Participants        []User `json:"participants"`
	CanEdit             *bool  `json:"canEdit"`
	CanDelete           *bool  `json:"canDelete"`
	CanLeave            *bool  `json:"canLeave"`
	CanBroadcast        bool   `json:"canBroadcast"`
	CanVideoKick        bool   `json:"canVideoKick"`
	CanAudioMute        bool   `json:"canAudioMute"`
	CanChangeChatAdmins bool   `json:"canChangeChatAdmins"`
	IsResultFromSearch  *bool  `json:"isResultFromSearch"`
	CanWriteMessage     bool   `json:"canWriteMessage"`
}

type ChatBasic struct {
	Id        int64  `db:"id"`
	Title     string `db:"title"`
	CanResend bool   `db:"can_resend"`
	TetATet   bool   `db:"tet_a_tet"`
}

type BasicChatDtoExtended struct {
	ChatBasic
	BehalfUserIsParticipant             bool `db:"behalf_user_is_participant"`
	RegularParticipantCanPublishMessage bool `db:"regular_participant_can_publish_message"`
	RegularParticipantCanPinMessage     bool `db:"regular_participant_can_pin_message"`
	RegularCanWriteMessage              bool `db:"regular_participant_can_write_message"`
}

type UserChatNotificationSettings struct {
	ConsiderMessagesOfThisChatAsUnread bool `json:"considerMessagesOfThisChatAsUnread" db:"consider_messages_as_unread"`
}

type ChatUnreadResetDto struct {
	ChatId         int64     `db:"id"`
	UpdateDateTime time.Time `db:"update_date_time"`
}

type ChatUserViewBasic struct {
	ChatId         int64     `json:"id"`
	UpdateDateTime time.Time `json:"update_date_time"`
	UnreadMessages int64     `json:"unread_messages"`
}
