package dto

import "time"

// list view
type BlogViewDto struct {
	Id             int64     `json:"id" db:"id"`
	OwnerId        *int64    `json:"ownerId" db:"owner_id"`
	Title          string    `json:"title" db:"title"`
	Preview        *string   `json:"preview" db:"preview"`
	CreateDateTime time.Time `json:"createDateTime" db:"create_date_time"`
}

type BlogViewEnrichedDto struct {
	BlogViewDto
	Owner *User `json:"owner"`
}

type BlogDto struct {
	Id             int64     `json:"id" db:"id"`
	OwnerId        *int64    `json:"ownerId" db:"owner_id"`
	Title          string    `json:"title" db:"title"`
	Post           *string   `json:"post" db:"post"`
	CreateDateTime time.Time `json:"createDateTime" db:"create_date_time"`
}

type BlogEnrichedDto struct {
	BlogDto
	Owner *User `json:"owner"`
}

type CommentViewDto struct {
	Id             int64      `json:"id" db:"id"`
	OwnerId        int64      `json:"ownerId" db:"owner_id"`
	Content        string     `json:"content" db:"content"`
	CreateDateTime time.Time  `json:"createDateTime" db:"create_date_time"`
	UpdateDateTime *time.Time `json:"editDateTime" db:"update_date_time"` // for sake compatibility
}

type CommentViewEnrichedDto struct {
	CommentViewDto
	Owner *User `json:"owner"`
}
