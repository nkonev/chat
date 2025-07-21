package cqrs

import (
	"context"
	"database/sql"
	"fmt"
	"go-cqrs-chat-example/db"
	"go-cqrs-chat-example/dto"
	"go-cqrs-chat-example/utils"
	"slices"

	"github.com/jackc/pgtype"
)

func (m *CommonProjection) GetChatIds(ctx context.Context, tx *db.Tx, size int32, offset int64) ([]int64, error) {
	ma := []int64{}
	rows, err := tx.QueryContext(ctx, `
		select c.id
		from chat_common c
		order by c.id asc 
		limit $1 offset $2
	`, size, offset)
	if err != nil {
		return ma, err
	}
	defer rows.Close()
	for rows.Next() {
		var ii int64
		err = rows.Scan(&ii)
		if err != nil {
			return ma, err
		}
		ma = append(ma, ii)
	}
	return ma, nil
}

func (m *CommonProjection) OnChatCreated(ctx context.Context, event *ChatCreated) error {
	_, err := m.db.ExecContext(ctx, `
		insert into chat_common(id, title, create_date_time) values ($1, $2, $3)
		on conflict(id) do update set title = excluded.title, create_date_time = excluded.create_date_time
	`, event.ChatId, event.Title, event.AdditionalData.CreatedAt)
	if err != nil {
		return err
	}
	m.lgr.InfoContext(ctx,
		"Common chat created",
		"chat_id", event.ChatId,
		"title", event.Title,
	)

	return nil
}

func (m *CommonProjection) OnChatEdited(ctx context.Context, event *ChatEdited) error {
	errOuter := db.Transact(ctx, m.db, func(tx *db.Tx) error {
		chatExists, err := m.checkChatExists(ctx, tx, event.ChatId)
		if err != nil {
			return err
		}
		if !chatExists {
			m.lgr.InfoContext(ctx, "Skipping ChatEdited because there is no chat", "chat_id", event.ChatId)
			return nil
		}

		admin, err := m.IsChatAdmin(ctx, tx, event.BehalfUserId, event.ChatId)
		if err != nil {
			return err
		}
		if !admin {
			m.lgr.InfoContext(ctx,
				"Participant isn't admin so he cannot change chat",
				"user_id", event.BehalfUserId,
				"chat_id", event.ChatId,
			)
			return nil
		}

		blog, errInner := m.isChatBlog(ctx, tx, event.ChatId)
		if errInner != nil {
			return errInner
		}

		_, errInner = tx.ExecContext(ctx, `
			update chat_common
			set title = $2,
			    blog = $3
			where id = $1
		`, event.ChatId, event.Title, event.Blog)
		if errInner != nil {
			return errInner
		}
		m.lgr.InfoContext(ctx,
			"Common chat edited",
			"chat_id", event.ChatId,
			"title", event.Title,
		)

		if blog && !event.Blog {
			// rm blog
			_, errInner = tx.ExecContext(ctx, `
				delete from blog
				where id = $1
			`, event.ChatId)
			if errInner != nil {
				return errInner
			}
		} else if !blog && event.Blog {
			// add blog
			errInner = m.refreshBlog(ctx, tx, event.ChatId, event.AdditionalData.CreatedAt)
			if errInner != nil {
				return errInner
			}
		} else if blog && event.Blog {
			// update blog
			errInner = m.refreshBlog(ctx, tx, event.ChatId, event.AdditionalData.CreatedAt)
			if errInner != nil {
				return errInner
			}
		}

		return nil
	})

	if errOuter != nil {
		return errOuter
	}

	return nil
}

func (m *CommonProjection) OnChatRemoved(ctx context.Context, event *ChatDeleted) error {
	errOuter := db.Transact(ctx, m.db, func(tx *db.Tx) error {

		admin, err := m.IsChatAdmin(ctx, tx, event.BehalfUserId, event.ChatId)
		if err != nil {
			return err
		}
		if !admin {
			m.lgr.InfoContext(ctx,
				"Participant isn't admin so he cannot delete chat",
				"user_id", event.BehalfUserId,
				"chat_id", event.ChatId,
			)
			return nil
		}

		blog, errInner := m.isChatBlog(ctx, tx, event.ChatId)
		if errInner != nil {
			return errInner
		}

		_, errInner = m.db.ExecContext(ctx, `
			delete from chat_common
			where id = $1
		`, event.ChatId)
		if errInner != nil {
			return errInner
		}

		if blog {
			_, errInner = m.db.ExecContext(ctx, `
			delete from blog
			where id = $1
		`, event.ChatId)
			if errInner != nil {
				return errInner
			}
		}

		m.lgr.InfoContext(ctx,
			"Common chat removed",
			"chat_id", event.ChatId,
		)
		return nil
	})

	if errOuter != nil {
		return errOuter
	}
	return nil
}

func (m *CommonProjection) OnChatPinned(ctx context.Context, event *ChatPinned) error {
	_, err := m.db.ExecContext(ctx, `
		update chat_user_view
		set pinned = $3
		where (id, user_id) = ($1, $2)
	`, event.ChatId, event.ParticipantId, event.Pinned)
	if err != nil {
		return err
	}

	m.lgr.InfoContext(ctx,
		"Chat pinned",
		"user_id", event.ParticipantId,
		"chat_id", event.ChatId,
		"pinned", event.Pinned,
	)

	return nil
}

func (m *CommonProjection) OnChatViewRefreshed(ctx context.Context, event *ChatViewRefreshed) error {
	errOuter := db.Transact(ctx, m.db, func(tx *db.Tx) error {
		// in oder not to have a potential race condition
		// for example "by upserting refresh view we can resurrect view of the newly removed participant in case message add"
		// we shouldn't upsert into chat_user_view
		// we can only update it here

		if event.UnreadMessagesAction == UnreadMessagesActionIncrease {
			participantIdsWithoutOwner := utils.GetSliceWithout(event.OwnerId, event.ParticipantIds)
			var ownerId *int64
			if slices.Contains(event.ParticipantIds, event.OwnerId) { // for batches without owner
				ownerId = &event.OwnerId
			}

			// not owners
			if len(participantIdsWithoutOwner) > 0 {
				_, err := tx.ExecContext(ctx, `
					UPDATE unread_messages_user_view 
					SET unread_messages = unread_messages + $3
					WHERE user_id = any($1) and chat_id = $2;
				`, participantIdsWithoutOwner, event.ChatId, event.IncreaseOn)
				if err != nil {
					return fmt.Errorf("error during increasing unread messages: %w", err)
				}
			}

			// owner
			if ownerId != nil {
				_, err := tx.ExecContext(ctx, `
					UPDATE unread_messages_user_view 
					SET last_message_id = (select max(id) from message where chat_id = $2)
					WHERE (user_id, chat_id) = ($1, $2);
				`, *ownerId, event.ChatId)
				if err != nil {
					return fmt.Errorf("error during increasing unread messages: %w", err)
				}
			}
		} else if event.UnreadMessagesAction == UnreadMessagesActionRefresh {
			err := m.setUnreadMessages(ctx, tx, event.ParticipantIds, event.ChatId, 0, true, true)
			if err != nil {
				return err
			}
		}

		if event.LastMessageAction == LastMessageActionRefresh {
			err := m.setLastMessage(ctx, tx, event.ParticipantIds, event.ChatId)
			if err != nil {
				return err
			}
		}

		if event.ChatCommonAction == ChatCommonActionRefresh {
			_, err := tx.ExecContext(ctx, `
					UPDATE chat_user_view 
					SET title = $3
					WHERE user_id = any($1) and id = $2;
				`, event.ParticipantIds, event.ChatId, event.Title)
			if err != nil {
				return fmt.Errorf("error during increasing unread messages: %w", err)
			}
		}

		if event.ParticipantsAction == ParticipantsActionRefresh {
			_, err := tx.ExecContext(ctx, `
					with
					this_chat_participants as (
						select user_id, create_date_time from chat_participant where chat_id = $2
					),
					chat_participant_count as (
						select count (*) as count from this_chat_participants
					),
					chat_participants_last_n as (
						select user_id from this_chat_participants order by create_date_time desc limit $3
					)
					UPDATE chat_user_view 
					SET 
						participants_count = (select count from chat_participant_count),
						participant_ids = (select array_agg(user_id) from chat_participants_last_n)
					WHERE user_id = any($1) and id = $2;
				`, event.ParticipantIds, event.ChatId, m.chatUserViewConfig.MaxViewableParticipants)
			if err != nil {
				return fmt.Errorf("error during increasing unread messages: %w", err)
			}
		}

		_, err := tx.ExecContext(ctx, `
				update chat_user_view set update_date_time = $3 where user_id = any($1) and id = $2
			`, event.ParticipantIds, event.ChatId, event.AdditionalData.CreatedAt)
		if err != nil {
			return err
		}

		return nil
	})

	if errOuter != nil {
		return errOuter
	}
	return nil
}

func (m *CommonProjection) checkChatExists(ctx context.Context, co db.CommonOperations, chatId int64) (bool, error) {
	rc := co.QueryRowContext(ctx, "select exists (select * from chat_common where id = $1)", chatId)
	if rc.Err() != nil {
		return false, rc.Err()
	}
	var chatExists bool
	err := rc.Scan(&chatExists)
	if err != nil {
		return false, err
	}
	return chatExists, nil
}

func (m *CommonProjection) GetChats(ctx context.Context, participantId int64, size int32, startingFromItemId *dto.ChatId, includeStartingFrom, reverse bool) ([]dto.ChatViewDto, error) {
	ma := []dto.ChatViewDto{}

	queryArgs := []any{participantId, size}

	order := "desc"
	offset := " offset 1" // to make behaviour the same as in users, messages (there is > or <)
	if reverse {
		order = "asc"
	}
	// see also getSafeDefaultUserId() in aaa
	if includeStartingFrom || startingFromItemId == nil {
		offset = ""
	}

	nonEquality := "<="
	if reverse {
		nonEquality = ">="
	}

	paginationKeyset := ""
	if startingFromItemId != nil {
		paginationKeyset = fmt.Sprintf(` and (ch.pinned, ch.update_date_time, ch.id) %s ($3, $4, $5)`, nonEquality)
		queryArgs = append(queryArgs, startingFromItemId.Pinned, startingFromItemId.LastUpdateDateTime, startingFromItemId.Id)
	}

	// it is optimized (all order by in the same table)
	// so querying a page (using keyset) from a large amount of chats is fast
	// it's the root cause why we use cqrs
	rows, err := m.db.QueryContext(ctx, fmt.Sprintf(`
		select 
		    ch.id,
		    ch.title,
		    ch.pinned,
		    coalesce(m.unread_messages, 0),
		    ch.last_message_id,
		    ch.last_message_owner_id,
		    ch.last_message_content,
		    ch.participants_count,
		    ch.participant_ids,
		    b.id is not null as blog,
		    ch.update_date_time
		from chat_user_view ch
		join unread_messages_user_view m on (ch.id = m.chat_id and m.user_id = $1)
		left join blog b on ch.id = b.id
		where ch.user_id = $1 %s
		order by (ch.pinned, ch.update_date_time, ch.id) %s
		limit $2 
		%s
		`, paginationKeyset, order, offset),
		queryArgs...)
	if err != nil {
		return ma, err
	}
	defer rows.Close()
	for rows.Next() {
		var cd dto.ChatViewDto
		var participantIds = pgtype.Int8Array{}
		err = rows.Scan(&cd.Id, &cd.Title, &cd.Pinned, &cd.UnreadMessages, &cd.LastMessageId, &cd.LastMessageOwnerId, &cd.LastMessageContent, &cd.ParticipantsCount, &participantIds, &cd.Blog, &cd.UpdateDateTime)
		if err != nil {
			return ma, err
		}
		cd.ParticipantIds = []int64{}
		for _, aParticipantId := range participantIds.Elements {
			cd.ParticipantIds = append(cd.ParticipantIds, aParticipantId.Int)
		}
		ma = append(ma, cd)
	}
	return ma, nil
}

func (m *CommonProjection) GetChatByUserIdAndChatId(ctx context.Context, userId, chatId int64) (string, error) {
	r := m.db.QueryRowContext(ctx, "select c.title from chat_user_view ch join chat_common c on ch.id = c.id where ch.user_id = $1 and ch.id = $2", userId, chatId)
	if r.Err() != nil {
		return "", r.Err()
	}
	var t string
	err := r.Scan(&t)
	if err != nil {
		return "", err
	}
	return t, nil
}

type ParticipantWithAdmin struct {
	ParticipantId int64 `json:"participantId"`
	ChatAdmin     bool  `json:"chatAdmin"`
}

func GetParticipantIds(participants []ParticipantWithAdmin) []int64 {
	res := make([]int64, 0, len(participants))
	for _, pa := range participants {
		res = append(res, pa.ParticipantId)
	}
	return res
}

func getParticipantChatAdmins(participants []ParticipantWithAdmin) []bool {
	res := make([]bool, 0, len(participants))
	for _, pa := range participants {
		res = append(res, pa.ChatAdmin)
	}
	return res
}

func getParticipantIdsCommon(ctx context.Context, co db.CommonOperations, chatId int64, excluding []int64, participantsSize int32, participantsOffset int64, reverseOrder bool) ([]ParticipantWithAdmin, error) {
	var rows *sql.Rows
	var err error

	order := "asc"
	if reverseOrder {
		order = "desc"
	}

	if len(excluding) > 0 {
		rows, err = co.QueryContext(ctx, fmt.Sprintf("SELECT user_id, chat_admin FROM chat_participant WHERE chat_id = $1 AND user_id not in (select * from unnest(cast ($4 as bigint[]))) order by create_date_time %s LIMIT $2 OFFSET $3", order), chatId, participantsSize, participantsOffset, excluding)
	} else {
		rows, err = co.QueryContext(ctx, fmt.Sprintf("SELECT user_id, chat_admin FROM chat_participant WHERE chat_id = $1 order by create_date_time %s LIMIT $2 OFFSET $3", order), chatId, participantsSize, participantsOffset)
	}

	if err != nil {
		return nil, fmt.Errorf("error during interacting with db: %w", err)
	}
	defer rows.Close()
	list := make([]ParticipantWithAdmin, 0)
	for rows.Next() {
		var pwa ParticipantWithAdmin
		if err = rows.Scan(&pwa.ParticipantId, &pwa.ChatAdmin); err != nil {
			return nil, fmt.Errorf("error during interacting with db: %w", err)
		}
		list = append(list, pwa)
	}
	return list, nil
}
