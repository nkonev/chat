package dto

type GlobalUserEvent struct {
	EventType                  string                    `json:"eventType"`
	UserId                     int64                     `json:"userId"`
	ChatNotification           *ChatViewEnrichedDto      `json:"chatNotification"`
	ChatDeletedDto             *ChatDeletedDto           `json:"chatDeletedNotification"`
	HasUnreadMessagesChanged   *HasUnreadMessagesChanged `json:"hasUnreadMessagesChanged"`
	UnreadMessagesNotification *ChatUnreadMessageChanged `json:"unreadMessagesNotification"`
}

type ChatEvent struct {
	EventType                    string                        `json:"eventType"`
	ChatId                       int64                         `json:"chatId"`
	UserId                       int64                         `json:"userId"`
	MessageNotification          *MessageViewEnrichedDto       `json:"messageNotification"`
	MessageDeletedNotification   *MessageDeletedDto            `json:"messageDeletedNotification"`
	MessageBroadcastNotification *MessageBroadcastNotification `json:"messageBroadcastNotification"`
	Participants                 *[]*UserWithAdmin             `json:"participants"`
	PromoteMessageNotification   *PinnedMessageEvent           `json:"promoteMessageNotification"`
	PublishedMessageNotification *PublishedMessageEvent        `json:"publishedMessageEvent"`
	ReactionChangedEvent         *ReactionChangedEvent         `json:"reactionChangedEvent"`
}
