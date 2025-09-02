package dto

type GlobalUserEvent struct {
	EventType        string               `json:"eventType"`
	UserId           int64                `json:"userId"`
	ChatNotification *ChatViewEnrichedDto `json:"chatNotification"`
	ChatDeletedDto   *ChatDeletedDto      `json:"chatDeletedNotification"`
}
