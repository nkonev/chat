package dto

import "time"

type MessageViewDto struct {
	Id             int64      `json:"id"`
	OwnerId        int64      `json:"ownerId"`
	Content        string     `json:"text"` // for sake compatibility
	BlogPost       bool       `json:"blogPost"`
	CreateDateTime time.Time  `json:"createDateTime"`
	UpdateDateTime *time.Time `json:"editDateTime"` // for sake compatibility
}

type MessageViewEnrichedDto struct {
	MessageViewDto
	Owner *User `json:"owner"`
}

type MessageBasic struct {
	Id      int64
	OwnerId int64
	Content string
}
