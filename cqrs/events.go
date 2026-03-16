package cqrs

import (
	"encoding/json"
	"fmt"
	"go-cqrs-chat-example/dto"
	"go-cqrs-chat-example/utils"
	"time"

	"github.com/google/uuid"
)

const (
	EventChatCreated                        = "chatCreated"
	EventChatEdited                         = "chatEdited"
	EventChatDeleted                        = "chatDeleted"
	EventParticipantsAdded                  = "participantsAdded"
	EventParticipantsDeleted                = "participantDeleted"
	EventParticipantsChanged                = "participantChanged"
	EventProjectionsResetted                = "projectionsResetted"
	EventUserChatPinned                     = "userChatPinned"
	EventChatPinned                         = "chatPinned"
	EventUserChatNotificationSettingsSetted = "userChatNotificationSettingsSetted"
	EventChatNotificationSettingsSetted     = "chatNotificationSettingsSetted"
	EventMessageCreated                     = "messageCreated"
	EventMessageEdited                      = "messageEdited"
	EventChatViewRefreshed                  = "chatViewRefreshed"
	EventUserMessageReaded                  = "userMessageReaded"
	EventMessageReaded                      = "messageReaded"
	EventMessageBlogPostMade                = "messageBlogPostMade"
	EventMessageDeleted                     = "messageDeleted"
	EventMessagePinned                      = "messagePinned"
	EventMessagePublished                   = "messagePublished"
	EventMessageReactionFlipped             = "messageReactionFlipped"
	EventTechnicalAbandonedChatRemoved      = "technicalAbandonedChatRemoved"
	EventUserChatViewCreated                = "userChatViewCreated"
	EventUserChatViewUpdated                = "userChatViewUpdated"
	EventUserChatViewRemoved                = "userChatViewRemoved"
)

type CqrsEvent interface {
	GetPartitionKey() string
	GetEventType() string
	GetMetadata() *Metadata
	SetMetadata(*Metadata)
	GetEventPartitioningBy() EventPartitioningBy
}

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

// Kafka headers
type Metadata struct {
	EventId   string
	EventType string
}

func NewMetadata(eventType string) *Metadata {
	uv7, err := uuid.NewV7()
	if err != nil {
		panic(err)
	}

	return &Metadata{
		EventId:   uv7.String(),
		EventType: eventType,
	}
}

type ChatCreated struct {
	AdditionalData                      *AdditionalData `json:"additionalData"`
	Metadata                            *Metadata       `json:"-"`
	ChatId                              int64           `json:"chatId"`
	Title                               string          `json:"title"`
	TetATet                             bool            `json:"tetATet"`
	TetATetOppositeUserId               *int64          `json:"tetATetOppositeUserId"`
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
	Metadata                            *Metadata       `json:"-"`
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
	Metadata       *Metadata       `json:"-"`
	ChatId         int64           `json:"chatId"`
}

type ParticipantsAdded struct {
	AdditionalData *AdditionalData        `json:"additionalData"`
	Metadata       *Metadata              `json:"-"`
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
	GetParticipantsTypeUnspecified GetParticipantsType = iota
	GetParticipantsTypeNormal
	GetParticipantsTypeAllInChatExcepting
	GetParticipantsTypeAllInAllChats // test only
)

type ParticipantDeleted struct {
	AdditionalData             *AdditionalData     `json:"additionalData"`
	Metadata                   *Metadata           `json:"-"`
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
	Metadata       *Metadata       `json:"-"`
	ParticipantId  int64           `json:"participantId"`
	ChatId         int64           `json:"chatId"`
	NewAdmin       bool            `json:"newAdmin"`
}

type ProjectionsTruncated struct {
	Metadata *Metadata `json:"-"`
}

type UserChatPinned struct {
	AdditionalData *AdditionalData `json:"additionalData"`
	Metadata       *Metadata       `json:"-"`
	ChatId         int64           `json:"chatId"`
	Pinned         bool            `json:"pinned"`
}

type UserChatNotificationSettingsSetted struct {
	AdditionalData *AdditionalData `json:"additionalData"`
	Metadata       *Metadata       `json:"-"`
	ChatId         int64           `json:"chatId"`
	Setted         bool            `json:"setted"`
}

type ChatPinned struct {
	AdditionalData *AdditionalData `json:"additionalData"`
	Metadata       *Metadata       `json:"-"`
	ChatId         int64           `json:"chatId"`
	Pinned         bool            `json:"pinned"`
}

type ChatNotificationSettingsSetted struct {
	AdditionalData *AdditionalData `json:"additionalData"`
	Metadata       *Metadata       `json:"-"`
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
	Metadata        *Metadata       `json:"-"`
}

type MessageEdited struct {
	MessageCommoned MessageCommoned `json:"messageCommoned"`
	AdditionalData  *AdditionalData `json:"additionalData"`
	IsEmbedSync     bool            `json:"isEmbedSync"`
	Metadata        *Metadata       `json:"-"`
}

type MessageEmbedded struct {
	Id        int64  `json:"id"`
	ChatId    int64  `json:"chatId"`
	EmbedType string `json:"embedType"`
}

type UnreadMessagesAction int16

const (
	UnreadMessagesActionUnspecified UnreadMessagesAction = iota
	UnreadMessagesActionRefresh
	UnreadMessagesActionIncrease
	UnreadMessagesActionDecrease
)

type LastMessageAction int16

const (
	LastMessageActionUnspecified LastMessageAction = iota
	LastMessageActionRefresh
)

type ChatAction int16

const (
	ChatActionUnspecified ChatAction = iota
	ChatActionRefresh
)

type ReadMessagesAction int16

const (
	ReadMessagesActionUnspecified ReadMessagesAction = iota
	ReadMessagesActionOneMessage
	ReadMessagesActionAllMessagesInOneChat
	ReadMessagesActionAllChats
)

type ParticipantsMode int16

const (
	ParticipantsModeUnspecified ParticipantsMode = iota
	ParticipantsModeAllParticipantIdsExcepting
	ParticipantsModeOnlyParticipantIds
)

type MessageWithOwner struct {
	MessageId int64 `json:"messageId"`
	OwnerId   int64 `json:"ownerId"`
}

type ChatViewRefreshed struct {
	ParticipantsMode           ParticipantsMode     `json:"participantsMode"`
	AllParticipantIdsExcepting []int64              `json:"allParticipantIdsExcepting"`
	OnlyParticipantIds         []int64              `json:"onlyParticipantIds"`
	ChatId                     int64                `json:"chatId"`
	UnreadMessagesAction       UnreadMessagesAction `json:"unreadMessagesAction"`
	MessagesDelta              []MessageWithOwner   `json:"messagesDelta"` // for UnreadMessagesActionIncrease or UnreadMessagesActionDecrease
	LastMessageAction          LastMessageAction    `json:"lastMessageAction"`
	ChatAction                 ChatAction           `json:"chatAction"`
	EventTime                  time.Time            `json:"eventTime"`
	CorrelationId              *string              `json:"correlationId"`
	Metadata                   *Metadata            `json:"-"`
}

type UserMessageReaded struct {
	AdditionalData     *AdditionalData    `json:"additionalData"`
	Metadata           *Metadata          `json:"-"`
	ChatId             int64              `json:"chatId"`
	MessageId          int64              `json:"messageId"`
	ReadMessagesAction ReadMessagesAction `json:"readMessagesAction"`
}

type MessageReaded struct {
	AdditionalData     *AdditionalData    `json:"additionalData"`
	Metadata           *Metadata          `json:"-"`
	ChatId             int64              `json:"chatId"`
	MessageId          int64              `json:"messageId"`
	ReadMessagesAction ReadMessagesAction `json:"readMessagesAction"`
}

type MessageBlogPostMade struct {
	AdditionalData *AdditionalData `json:"additionalData"`
	Metadata       *Metadata       `json:"-"`
	ChatId         int64           `json:"chatId"`
	MessageId      int64           `json:"messageId"`
	BlogPost       bool            `json:"blogPost"`
}

type MessageDeleted struct {
	AdditionalData *AdditionalData `json:"additionalData"`
	Metadata       *Metadata       `json:"-"`
	ChatId         int64           `json:"chatId"`
	MessageId      int64           `json:"messageId"`
}

type MessagePinned struct {
	AdditionalData *AdditionalData `json:"additionalData"`
	Metadata       *Metadata       `json:"-"`
	ChatId         int64           `json:"chatId"`
	MessageId      int64           `json:"messageId"`
	Pinned         bool            `json:"pinned"`
}

type MessagePublished struct {
	AdditionalData *AdditionalData `json:"additionalData"`
	Metadata       *Metadata       `json:"-"`
	ChatId         int64           `json:"chatId"`
	MessageId      int64           `json:"messageId"`
	Published      bool            `json:"published"`
}

type MessageReactionFlipped struct {
	AdditionalData *AdditionalData `json:"additionalData"`
	Metadata       *Metadata       `json:"-"`
	ChatId         int64           `json:"chatId"`
	MessageId      int64           `json:"messageId"`
	Reaction       string          `json:"reaction"`
}

type TechnicalAbandonedChatRemoved struct {
	ChatId   int64     `json:"chatId"`
	Metadata *Metadata `json:"-"`
}

type UserChatViewCreated struct {
	AdditionalData *AdditionalData `json:"additionalData"`
	Metadata       *Metadata       `json:"-"`
	ChatId         int64           `json:"chatId"`
	UserId         int64           `json:"userId"`
	TetATet        bool            `json:"tetATet"`
}

type UserChatViewUpdated struct {
	ChatId               int64                `json:"chatId"`
	UserId               int64                `json:"userId"`
	UnreadMessagesAction UnreadMessagesAction `json:"unreadMessagesAction"`
	MessagesDelta        []MessageWithOwner   `json:"messagesDelta"` // for UnreadMessagesActionIncrease or UnreadMessagesActionDecrease
	LastMessageAction    LastMessageAction    `json:"lastMessageAction"`
	ChatAction           ChatAction           `json:"chatAction"`
	EventTime            time.Time            `json:"eventTime"`
	CorrelationId        *string              `json:"correlationId"`
	Metadata             *Metadata            `json:"-"`
}

type UserChatViewRemoved struct {
	AdditionalData          *AdditionalData `json:"additionalData"`
	Metadata                *Metadata       `json:"-"`
	ChatId                  int64           `json:"chatId"`
	UserId                  int64           `json:"userId"`
	WereRemovedUsersFromAaa bool            `json:"wereRemovedUsersFromAaa"`
	IsChatPubliclyAvailable bool            `json:"isChatPubliclyAvailable"`
}

func GenerateMessageAdditionalData(correlationId *string, behalfUserId int64) *AdditionalData {
	return &AdditionalData{
		CreatedAt:     time.Now().UTC(),
		CorrelationId: correlationId,
		BehalfUserId:  behalfUserId,
	}
}

type EventPartitioningBy int16

const (
	EventPartitioningByUnspecified EventPartitioningBy = iota
	EventPartitioningByChatId
	EventPartitioningByUserId
)

func (k EventPartitioningBy) String() string {
	switch k {
	case EventPartitioningByUnspecified:
		return "unspecified"
	case EventPartitioningByChatId:
		return "chat"
	case EventPartitioningByUserId:
		return "user"
	}
	return "unknown"
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

func (s *UserChatPinned) GetPartitionKey() string {
	return utils.ToString(s.AdditionalData.BehalfUserId)
}

func (s *ChatPinned) GetPartitionKey() string {
	return utils.ToString(s.ChatId)
}

func (s *UserChatNotificationSettingsSetted) GetPartitionKey() string {
	return utils.ToString(s.AdditionalData.BehalfUserId)
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

func (s *UserMessageReaded) GetPartitionKey() string {
	return utils.ToString(s.AdditionalData.BehalfUserId)
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

func (s *MessagePublished) GetPartitionKey() string {
	return utils.ToString(s.ChatId)
}

func (s *MessageReactionFlipped) GetPartitionKey() string {
	return utils.ToString(s.ChatId)
}

func (s *TechnicalAbandonedChatRemoved) GetPartitionKey() string {
	return utils.ToString(s.ChatId)
}

func (s *UserChatViewCreated) GetPartitionKey() string {
	return utils.ToString(s.UserId)
}

func (s *UserChatViewUpdated) GetPartitionKey() string {
	return utils.ToString(s.UserId)
}

func (s *UserChatViewRemoved) GetPartitionKey() string {
	return utils.ToString(s.UserId)
}

func (s *ChatCreated) GetEventType() string {
	return EventChatCreated
}

func (s *ChatEdited) GetEventType() string {
	return EventChatEdited
}

func (s *ChatDeleted) GetEventType() string {
	return EventChatDeleted
}

func (s *ParticipantsAdded) GetEventType() string {
	return EventParticipantsAdded
}

func (s *ParticipantDeleted) GetEventType() string {
	return EventParticipantsDeleted
}

func (s *ParticipantChanged) GetEventType() string {
	return EventParticipantsChanged
}

func (s *ProjectionsTruncated) GetEventType() string {
	return EventProjectionsResetted
}

func (s *UserChatPinned) GetEventType() string {
	return EventUserChatPinned
}

func (s *ChatPinned) GetEventType() string {
	return EventChatPinned
}

func (s *UserChatNotificationSettingsSetted) GetEventType() string {
	return EventUserChatNotificationSettingsSetted
}

func (s *ChatNotificationSettingsSetted) GetEventType() string {
	return EventChatNotificationSettingsSetted
}

func (s *MessageCreated) GetEventType() string {
	return EventMessageCreated
}

func (s *MessageEdited) GetEventType() string {
	return EventMessageEdited
}

func (s *ChatViewRefreshed) GetEventType() string {
	return EventChatViewRefreshed
}

func (s *UserMessageReaded) GetEventType() string {
	return EventUserMessageReaded
}

func (s *MessageReaded) GetEventType() string {
	return EventMessageReaded
}

func (s *MessageBlogPostMade) GetEventType() string {
	return EventMessageBlogPostMade
}

func (s *MessageDeleted) GetEventType() string {
	return EventMessageDeleted
}

func (s *MessagePinned) GetEventType() string {
	return EventMessagePinned
}

func (s *MessagePublished) GetEventType() string {
	return EventMessagePublished
}

func (s *MessageReactionFlipped) GetEventType() string {
	return EventMessageReactionFlipped
}

func (s *TechnicalAbandonedChatRemoved) GetEventType() string {
	return EventTechnicalAbandonedChatRemoved
}

func (s *UserChatViewCreated) GetEventType() string {
	return EventUserChatViewCreated
}

func (s *UserChatViewUpdated) GetEventType() string {
	return EventUserChatViewUpdated
}

func (s *UserChatViewRemoved) GetEventType() string {
	return EventUserChatViewRemoved
}

func (s *ChatCreated) GetMetadata() *Metadata {
	return s.Metadata
}

func (s *ChatEdited) GetMetadata() *Metadata {
	return s.Metadata
}

func (s *ChatDeleted) GetMetadata() *Metadata {
	return s.Metadata
}

func (s *ParticipantsAdded) GetMetadata() *Metadata {
	return s.Metadata
}

func (s *ParticipantDeleted) GetMetadata() *Metadata {
	return s.Metadata
}

func (s *ParticipantChanged) GetMetadata() *Metadata {
	return s.Metadata
}

func (s *ProjectionsTruncated) GetMetadata() *Metadata {
	return s.Metadata
}

func (s *UserChatPinned) GetMetadata() *Metadata {
	return s.Metadata
}

func (s *ChatPinned) GetMetadata() *Metadata {
	return s.Metadata
}

func (s *UserChatNotificationSettingsSetted) GetMetadata() *Metadata {
	return s.Metadata
}

func (s *ChatNotificationSettingsSetted) GetMetadata() *Metadata {
	return s.Metadata
}

func (s *MessageCreated) GetMetadata() *Metadata {
	return s.Metadata
}

func (s *MessageEdited) GetMetadata() *Metadata {
	return s.Metadata
}

func (s *ChatViewRefreshed) GetMetadata() *Metadata {
	return s.Metadata
}

func (s *UserMessageReaded) GetMetadata() *Metadata {
	return s.Metadata
}

func (s *MessageReaded) GetMetadata() *Metadata {
	return s.Metadata
}

func (s *MessageBlogPostMade) GetMetadata() *Metadata {
	return s.Metadata
}

func (s *MessageDeleted) GetMetadata() *Metadata {
	return s.Metadata
}

func (s *MessagePinned) GetMetadata() *Metadata {
	return s.Metadata
}

func (s *MessagePublished) GetMetadata() *Metadata {
	return s.Metadata
}

func (s *MessageReactionFlipped) GetMetadata() *Metadata {
	return s.Metadata
}

func (s *TechnicalAbandonedChatRemoved) GetMetadata() *Metadata {
	return s.Metadata
}

func (s *UserChatViewCreated) GetMetadata() *Metadata {
	return s.Metadata
}

func (s *UserChatViewUpdated) GetMetadata() *Metadata {
	return s.Metadata
}

func (s *UserChatViewRemoved) GetMetadata() *Metadata {
	return s.Metadata
}

func (s *ChatCreated) SetMetadata(m *Metadata) {
	s.Metadata = m
}

func (s *ChatEdited) SetMetadata(m *Metadata) {
	s.Metadata = m
}

func (s *ChatDeleted) SetMetadata(m *Metadata) {
	s.Metadata = m
}

func (s *ParticipantsAdded) SetMetadata(m *Metadata) {
	s.Metadata = m
}

func (s *ParticipantDeleted) SetMetadata(m *Metadata) {
	s.Metadata = m
}

func (s *ParticipantChanged) SetMetadata(m *Metadata) {
	s.Metadata = m
}

func (s *ProjectionsTruncated) SetMetadata(m *Metadata) {
	s.Metadata = m
}

func (s *UserChatPinned) SetMetadata(m *Metadata) {
	s.Metadata = m
}

func (s *ChatPinned) SetMetadata(m *Metadata) {
	s.Metadata = m
}

func (s *UserChatNotificationSettingsSetted) SetMetadata(m *Metadata) {
	s.Metadata = m
}

func (s *ChatNotificationSettingsSetted) SetMetadata(m *Metadata) {
	s.Metadata = m
}

func (s *MessageCreated) SetMetadata(m *Metadata) {
	s.Metadata = m
}

func (s *MessageEdited) SetMetadata(m *Metadata) {
	s.Metadata = m
}

func (s *ChatViewRefreshed) SetMetadata(m *Metadata) {
	s.Metadata = m
}

func (s *UserMessageReaded) SetMetadata(m *Metadata) {
	s.Metadata = m
}

func (s *MessageReaded) SetMetadata(m *Metadata) {
	s.Metadata = m
}

func (s *MessageBlogPostMade) SetMetadata(m *Metadata) {
	s.Metadata = m
}

func (s *MessageDeleted) SetMetadata(m *Metadata) {
	s.Metadata = m
}

func (s *MessagePinned) SetMetadata(m *Metadata) {
	s.Metadata = m
}

func (s *MessagePublished) SetMetadata(m *Metadata) {
	s.Metadata = m
}

func (s *MessageReactionFlipped) SetMetadata(m *Metadata) {
	s.Metadata = m
}

func (s *TechnicalAbandonedChatRemoved) SetMetadata(m *Metadata) {
	s.Metadata = m
}

func (s *UserChatViewCreated) SetMetadata(m *Metadata) {
	s.Metadata = m
}

func (s *UserChatViewUpdated) SetMetadata(m *Metadata) {
	s.Metadata = m
}

func (s *UserChatViewRemoved) SetMetadata(m *Metadata) {
	s.Metadata = m
}

func (s *ChatCreated) GetEventPartitioningBy() EventPartitioningBy {
	return EventPartitioningByChatId
}

func (s *ChatEdited) GetEventPartitioningBy() EventPartitioningBy {
	return EventPartitioningByChatId
}

func (s *ChatDeleted) GetEventPartitioningBy() EventPartitioningBy {
	return EventPartitioningByChatId
}

func (s *ParticipantsAdded) GetEventPartitioningBy() EventPartitioningBy {
	return EventPartitioningByChatId
}

func (s *ParticipantDeleted) GetEventPartitioningBy() EventPartitioningBy {
	return EventPartitioningByChatId
}

func (s *ParticipantChanged) GetEventPartitioningBy() EventPartitioningBy {
	return EventPartitioningByChatId
}

func (s *ProjectionsTruncated) GetEventPartitioningBy() EventPartitioningBy {
	return EventPartitioningByChatId
}

func (s *UserChatPinned) GetEventPartitioningBy() EventPartitioningBy {
	return EventPartitioningByUserId
}

func (s *UserChatNotificationSettingsSetted) GetEventPartitioningBy() EventPartitioningBy {
	return EventPartitioningByUserId
}

func (s *ChatPinned) GetEventPartitioningBy() EventPartitioningBy {
	return EventPartitioningByChatId
}

func (s *ChatNotificationSettingsSetted) GetEventPartitioningBy() EventPartitioningBy {
	return EventPartitioningByChatId
}

func (s *MessageCreated) GetEventPartitioningBy() EventPartitioningBy {
	return EventPartitioningByChatId
}

func (s *MessageEdited) GetEventPartitioningBy() EventPartitioningBy {
	return EventPartitioningByChatId
}

func (s *ChatViewRefreshed) GetEventPartitioningBy() EventPartitioningBy {
	return EventPartitioningByChatId
}

func (s *UserMessageReaded) GetEventPartitioningBy() EventPartitioningBy {
	return EventPartitioningByUserId
}

func (s *MessageReaded) GetEventPartitioningBy() EventPartitioningBy {
	return EventPartitioningByChatId
}

func (s *MessageBlogPostMade) GetEventPartitioningBy() EventPartitioningBy {
	return EventPartitioningByChatId
}

func (s *MessageDeleted) GetEventPartitioningBy() EventPartitioningBy {
	return EventPartitioningByChatId
}

func (s *MessagePinned) GetEventPartitioningBy() EventPartitioningBy {
	return EventPartitioningByChatId
}

func (s *MessagePublished) GetEventPartitioningBy() EventPartitioningBy {
	return EventPartitioningByChatId
}

func (s *MessageReactionFlipped) GetEventPartitioningBy() EventPartitioningBy {
	return EventPartitioningByChatId
}

func (s *TechnicalAbandonedChatRemoved) GetEventPartitioningBy() EventPartitioningBy {
	return EventPartitioningByChatId
}

func (s *UserChatViewCreated) GetEventPartitioningBy() EventPartitioningBy {
	return EventPartitioningByUserId
}

func (s *UserChatViewUpdated) GetEventPartitioningBy() EventPartitioningBy {
	return EventPartitioningByUserId
}

func (s *UserChatViewRemoved) GetEventPartitioningBy() EventPartitioningBy {
	return EventPartitioningByUserId
}
