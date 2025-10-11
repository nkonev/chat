package dto

import "time"

const NoId = -1

type User struct {
	Id               int64           `json:"id"`
	Login            string          `json:"login"`
	Avatar           *string         `json:"avatar"`
	ShortInfo        *string         `json:"shortInfo"`
	LoginColor       *string         `json:"loginColor"`
	LastSeenDateTime *time.Time      `json:"lastSeenDateTime"`
	AdditionalData   *AdditionalData `json:"additionalData"`
}

func (u *User) GetId() int64 {
	if u != nil {
		return u.Id
	} else {
		return NoId
	}
}

type UserWithAdmin struct {
	User
	ChatAdmin bool `json:"admin"`
}

type AdditionalData struct {
	Enabled   bool     `json:"enabled"`
	Expired   bool     `json:"expired"`
	Locked    bool     `json:"locked"`
	Confirmed bool     `json:"confirmed"`
	Roles     []string `json:"roles"`
}

type ParticipantsWithAdminWrapper struct {
	Data  []*UserWithAdmin `json:"items"`
	Count int64            `json:"count"` // for paginating purposes
}

type CountRequestDto struct {
	SearchString string `json:"searchString"`
}
