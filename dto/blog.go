package dto

import "time"

// list view
type BlogViewDto struct {
	Id             int64     `json:"id"`
	OwnerId        *int64    `json:"ownerId"`
	Title          string    `json:"title"`
	Preview        *string   `json:"preview"`
	CreateDateTime time.Time `json:"createDateTime"`
}

type BlogDto struct {
	Id             int64     `json:"id"`
	OwnerId        *int64    `json:"ownerId"`
	Title          string    `json:"title"`
	Post           *string   `json:"post"`
	CreateDateTime time.Time `json:"createDateTime"`
}

type CommentViewDto struct {
	Id             int64      `json:"id"`
	OwnerId        int64      `json:"ownerId"`
	Content        string     `json:"content"`
	CreateDateTime time.Time  `json:"createDateTime"`
	UpdateDateTime *time.Time `json:"editDateTime"` // for sake compatibility
}
