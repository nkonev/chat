package cqrs

import (
	"encoding/json"
	"fmt"
	"go-cqrs-chat-example/dto"
	"go-cqrs-chat-example/utils"
	"time"
)

type AdditionalData struct {
	CreatedAt     time.Time `json:"createdAt"`
	CorrelationId *string   `json:"correlationId"`
	BehalfUserId  int64     `json:"behalfUserId"`
}

func (p *AdditionalData) GetCorrelationId() *string {
	if p == nil {
		return nil
	}

	return p.CorrelationId
}

type ChatCreated struct {
	AdditionalData                      *AdditionalData `json:"additionalData"`
	ChatId                              int64           `json:"chatId"`
	Title                               string          `json:"title"`
	TetATet                             bool            `json:"tetATet"`
	TetATetOppositeUserId               *int64          `json:"tetATetOppositeUserId"` // not nil in case tet-a-tet
	TetATetSingle                       bool            `json:"tetATetSingle"`
	Blog                                bool            `json:"blog"`
	BlogAbout                           bool            `json:"blogAbout"`
	Avatar                              *string         `json:"avatar"`
	AvatarBig                           *string         `json:"avatarBig"`
	CanResend                           bool            `json:"canResend"`
	CanReact                            bool            `json:"canReact"`
	AvailableToSearch                   bool            `json:"availableToSearch"`
	RegularParticipantCanPublishMessage bool            `json:"regularParticipantCanPublishMessage"`
	RegularParticipantCanPinMessage     bool            `json:"regularParticipantCanPinMessage"`
	RegularParticipantCanWriteMessage   bool            `json:"regularParticipantCanWriteMessage"`
	RegularParticipantCanAddParticipant bool            `json:"regularParticipantCanAddParticipant"`
}

type ChatEdited struct {
	AdditionalData                      *AdditionalData `json:"additionalData"`
	ChatId                              int64           `json:"chatId"`
	Title                               string          `json:"title"`
	Blog                                bool            `json:"blog"`
	BlogAbout                           bool            `json:"blogAbout"`
	Avatar                              *string         `json:"avatar"`
	AvatarBig                           *string         `json:"avatarBig"`
	CanResend                           bool            `json:"canResend"`
	CanReact                            bool            `json:"canReact"`
	AvailableToSearch                   bool            `json:"availableToSearch"`
	RegularParticipantCanPublishMessage bool            `json:"regularParticipantCanPublishMessage"`
	RegularParticipantCanPinMessage     bool            `json:"regularParticipantCanPinMessage"`
	RegularParticipantCanWriteMessage   bool            `json:"regularParticipantCanWriteMessage"`
	RegularParticipantCanAddParticipant bool            `json:"regularParticipantCanAddParticipant"`
}

type ChatDeleted struct {
	AdditionalData *AdditionalData `json:"additionalData"`
	ChatId         int64           `json:"chatId"`
}

type ParticipantsAdded struct {
	AdditionalData *AdditionalData        `json:"additionalData"`
	Participants   []ParticipantWithAdmin `json:"participants"`
	ChatId         int64                  `json:"chatId"`
	IsChatCreating bool                   `json:"isChatCreating"`
	IsJoining      bool                   `json:"isJoining"`
	TetATet        bool                   `json:"tetATet"`
}

func (p *ParticipantsAdded) GetParticipantIds() []int64 {
	res := []int64{}
	if p == nil {
		return res
	}
	for _, pa := range p.Participants {
		res = append(res, pa.ParticipantId)
	}
	return res
}

type GetParticipantsType int16

const (
	GetParticipantsTypeUnspecified = iota
	GetParticipantsTypeNormal
	GetParticipantsTypeAllInChatExcepting
	GetParticipantsTypeAllInAllChats // test only
)

type ParticipantDeleted struct {
	AdditionalData             *AdditionalData     `json:"additionalData"`
	GetParticipantsType        GetParticipantsType `json:"getParticipantsType"`
	ParticipantIds             []int64             `json:"participantIds"`
	AllParticipantIdsExcepting []int64             `json:"allParticipantIdsExcepting"`
	ChatId                     int64               `json:"chatId"`
	IsLeaving                  bool                `json:"isLeaving"`
	IsChatRemoving             bool                `json:"isChatRemoving"`
	WereRemovedUsersFromAaa    bool                `json:"wereRemovedUsersFromAaa"`
}

type ParticipantChanged struct {
	AdditionalData *AdditionalData `json:"additionalData"`
	ParticipantId  int64           `json:"participantId"`
	ChatId         int64           `json:"chatId"`
	NewAdmin       bool            `json:"newAdmin"`
}

type ProjectionsTruncated struct {
}

type ChatPinned struct {
	AdditionalData *AdditionalData `json:"additionalData"`
	ChatId         int64           `json:"chatId"`
	Pinned         bool            `json:"pinned"`
}

type ChatNotificationSettingsSetted struct {
	AdditionalData *AdditionalData `json:"additionalData"`
	ChatId         int64           `json:"chatId"`
	Setted         bool            `json:"setted"`
}

type MessageCommoned struct {
	Id           int64   `json:"id"` // message id
	ChatId       int64   `json:"chatId"`
	Content      string  `json:"content"`
	FileItemUuid *string `json:"fileItemUuid"`

	Embed    dto.Embeddable  `json:"-"` // seem marshalling below
	RawEmbed json.RawMessage `json:"embed"`
}

func (f *MessageCommoned) UnmarshalJSON(b []byte) error {
	type cp MessageCommoned
	err := json.Unmarshal(b, (*cp)(f))
	if err != nil {
		return err
	}

	if f.RawEmbed != nil && string(f.RawEmbed) != "null" {
		var v dto.EmbedTyper
		err = json.Unmarshal(f.RawEmbed, &v)
		if err != nil {
			return err
		}

		var i dto.Embeddable
		switch v.Type {
		case dto.EmbedMessageTypeReply:
			i = &dto.EmbedReply{}
		case dto.EmbedMessageTypeResend:
			i = &dto.EmbedResend{}
		default:
			return fmt.Errorf("Unknown type in unmarshalling: %s", v.Type)
		}

		err = json.Unmarshal(f.RawEmbed, i)
		if err != nil {
			return err
		}
		f.Embed = i
	}
	return nil
}

func (f *MessageCommoned) MarshalJSON() ([]byte, error) {
	type res MessageCommoned
	if f.Embed != nil {
		switch typed := f.Embed.(type) {
		case *dto.EmbedReply:
			b, err := json.Marshal(typed)
			if err != nil {
				return nil, err
			}
			f.RawEmbed = b
		case *dto.EmbedResend:
			b, err := json.Marshal(typed)
			if err != nil {
				return nil, err
			}
			f.RawEmbed = b
		default:
			return nil, fmt.Errorf("Unknown type in marshalling:%T", f.Embed)
		}
	}

	return json.Marshal((*res)(f))
}

type MessageCreated struct {
	MessageCommoned MessageCommoned `json:"messageCommoned"`
	AdditionalData  *AdditionalData `json:"additionalData"`
}

type MessageEdited struct {
	MessageCommoned MessageCommoned `json:"messageCommoned"`
	AdditionalData  *AdditionalData `json:"additionalData"`
	IsEmbedSync     bool            `json:"isEmbedSync"`
}

type MessageEmbedded struct {
	Id        int64  `json:"id"`
	ChatId    int64  `json:"chatId"`
	EmbedType string `json:"embedType"`
}

type UnreadMessagesAction int16

const (
	UnreadMessagesActionUnspecified = iota
	UnreadMessagesActionRefresh
	UnreadMessagesActionIncrease
)

type LastMessageAction int16

const (
	LastMessageActionUnspecified = iota
	LastMessageActionRefresh
)

type ChatAction int16

const (
	ChatActionUnspecified = iota
	ChatActionRefresh
)

type ReadMessagesAction int16

const (
	ReadMessagesActionUnspecified = iota
	ReadMessagesActionOneMessage
	ReadMessagesActionAllMessagesInOneChat
	ReadMessagesActionAllChats
)

type ParticipantsMode int16

const (
	ParticipantsModeUnspecified = iota
	ParticipantsModeAllParticipantIdsExcepting
	ParticipantsModeOnlyParticipantIds
)

type ChatViewRefreshed struct {
	AdditionalData             *AdditionalData      `json:"additionalData"`
	ParticipantsMode           ParticipantsMode     `json:"participantsMode"`
	AllParticipantIdsExcepting []int64              `json:"allParticipantIdsExcepting"`
	OnlyParticipantIds         []int64              `json:"onlyParticipantIds"`
	ChatId                     int64                `json:"chatId"`
	UnreadMessagesAction       UnreadMessagesAction `json:"unreadMessagesAction"`
	LastMessageAction          LastMessageAction    `json:"lastMessageAction"`
	IncreaseOn                 int                  `json:"increaseOn"`
	ChatAction                 ChatAction           `json:"chatAction"`
}

type MessageReaded struct {
	AdditionalData     *AdditionalData    `json:"additionalData"`
	ChatId             int64              `json:"chatId"`
	MessageId          int64              `json:"messageId"`
	ReadMessagesAction ReadMessagesAction `json:"readMessagesAction"`
}

type MessageBlogPostMade struct {
	AdditionalData *AdditionalData `json:"additionalData"`
	ChatId         int64           `json:"chatId"`
	MessageId      int64           `json:"messageId"`
	BlogPost       bool            `json:"blogPost"`
}

type MessageDeleted struct {
	AdditionalData *AdditionalData `json:"additionalData"`
	ChatId         int64           `json:"chatId"`
	MessageId      int64           `json:"messageId"`
}

type MessagePinned struct {
	AdditionalData *AdditionalData `json:"additionalData"`
	ChatId         int64           `json:"chatId"`
	MessageId      int64           `json:"messageId"`
	Pinned         bool            `json:"pinned"`
}

type MessageReactionFlipped struct {
	AdditionalData *AdditionalData `json:"additionalData"`
	ChatId         int64           `json:"chatId"`
	MessageId      int64           `json:"messageId"`
	Reaction       string          `json:"reaction"`
}

type TechnicalAbandonedChatRemoved struct {
	ChatId int64 `json:"chatId"`
}

func GenerateMessageAdditionalData(correlationId *string, behalfUserId int64) *AdditionalData {
	return &AdditionalData{
		CreatedAt:     time.Now().UTC(),
		CorrelationId: correlationId,
		BehalfUserId:  behalfUserId,
	}
}

func (s *ChatCreated) GetPartitionKey() string {
	return utils.ToString(s.ChatId)
}

func (s *ChatEdited) GetPartitionKey() string {
	return utils.ToString(s.ChatId)
}

func (s *ChatDeleted) GetPartitionKey() string {
	return utils.ToString(s.ChatId)
}

func (s *ParticipantsAdded) GetPartitionKey() string {
	return utils.ToString(s.ChatId)
}

func (s *ParticipantDeleted) GetPartitionKey() string {
	return utils.ToString(s.ChatId)
}

func (s *ParticipantChanged) GetPartitionKey() string {
	return utils.ToString(s.ChatId)
}

func (s *ProjectionsTruncated) GetPartitionKey() string {
	return utils.ToString(0)
}

func (s *ChatPinned) GetPartitionKey() string {
	return utils.ToString(s.ChatId)
}

func (s *ChatNotificationSettingsSetted) GetPartitionKey() string {
	return utils.ToString(s.ChatId)
}

func (s *MessageCreated) GetPartitionKey() string {
	return utils.ToString(s.MessageCommoned.ChatId)
}

func (s *MessageEdited) GetPartitionKey() string {
	return utils.ToString(s.MessageCommoned.ChatId)
}

func (s *ChatViewRefreshed) GetPartitionKey() string {
	return utils.ToString(s.ChatId)
}

func (s *MessageReaded) GetPartitionKey() string {
	return utils.ToString(s.ChatId)
}

func (s *MessageBlogPostMade) GetPartitionKey() string {
	return utils.ToString(s.ChatId)
}

func (s *MessageDeleted) GetPartitionKey() string {
	return utils.ToString(s.ChatId)
}

func (s *MessagePinned) GetPartitionKey() string {
	return utils.ToString(s.ChatId)
}

func (s *MessageReactionFlipped) GetPartitionKey() string {
	return utils.ToString(s.ChatId)
}

func (s *TechnicalAbandonedChatRemoved) GetPartitionKey() string {
	return utils.ToString(s.ChatId)
}

func (s *ChatCreated) Name() string {
	return "chatCreated"
}

func (s *ChatEdited) Name() string {
	return "chatEdited"
}

func (s *ChatDeleted) Name() string {
	return "chatDeleted"
}

func (s *ParticipantsAdded) Name() string {
	return "participantsAdded"
}

func (s *ParticipantDeleted) Name() string {
	return "participantDeleted"
}

func (s *ParticipantChanged) Name() string {
	return "participantChanged"
}

func (s *ProjectionsTruncated) Name() string {
	return "projectionsResetted"
}

func (s *ChatPinned) Name() string {
	return "chatPinned"
}

func (s *ChatNotificationSettingsSetted) Name() string {
	return "chatNotificationSettingsSetted"
}

func (s *MessageCreated) Name() string {
	return "messageCreated"
}

func (s *MessageEdited) Name() string {
	return "messageEdited"
}

func (s *ChatViewRefreshed) Name() string {
	return "chatViewRefreshed"
}

func (s *MessageReaded) Name() string {
	return "messageReaded"
}

func (s *MessageBlogPostMade) Name() string {
	return "messageBlogPostMade"
}

func (s *MessageDeleted) Name() string {
	return "messageDeleted"
}

func (s *MessagePinned) Name() string {
	return "messagePinned"
}

func (s *MessageReactionFlipped) Name() string {
	return "messageReactionFlipped"
}

func (s *TechnicalAbandonedChatRemoved) Name() string {
	return "technicalAbandonedChatRemoved"
}
