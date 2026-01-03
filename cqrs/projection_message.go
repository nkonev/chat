package cqrs

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"go-cqrs-chat-example/config"
	"go-cqrs-chat-example/db"
	"go-cqrs-chat-example/dto"
	"go-cqrs-chat-example/logger"
	"go-cqrs-chat-example/preview"
	"go-cqrs-chat-example/sanitizer"
	"go-cqrs-chat-example/utils"
	"slices"
	"strings"
	"time"

	"github.com/PuerkitoBio/goquery"
	"github.com/georgysavva/scany/v2/sqlscan"
	"github.com/jackc/pgtype"
)

func (m *CommonProjection) OnMessageCreated(ctx context.Context, event *MessageCreated) error {
	errOuter := db.Transact(ctx, m.db, func(tx *db.Tx) error {
		chatExists, err := m.checkChatExists(ctx, tx, event.MessageCommoned.ChatId)
		if err != nil {
			return err
		}
		if !chatExists {
			m.lgr.InfoContext(ctx, "Skipping MessageCreated because there is no chat", "chat_id", event.MessageCommoned.ChatId)
			return nil
		}

		var embed pgtype.JSONB
		if event.MessageCommoned.Embed != nil {
			err = embed.Set(event.MessageCommoned.Embed)
			if err != nil {
				return err
			}
		} else {
			embed.Status = pgtype.Null
		}
		_, err = tx.ExecContext(ctx, `
		insert into message(id, chat_id, owner_id, content, embed, create_date_time, update_date_time, file_item_uuid) 
			values ($1, $2, $3, $4, $5, $6, $7, $8)
		on conflict(chat_id, id) do update set 
		    owner_id = excluded.owner_id
		    , content = excluded.content
			, embed = excluded.embed
		    , update_date_time = excluded.update_date_time
			, file_item_uuid = excluded.file_item_uuid
	`, event.MessageCommoned.Id, event.MessageCommoned.ChatId, event.AdditionalData.BehalfUserId, event.MessageCommoned.Content, embed, event.AdditionalData.CreatedAt, nil, event.MessageCommoned.FileItemUuid)
		if err != nil {
			return err
		}
		m.lgr.InfoContext(ctx,
			"Handling message added",
			"id", event.MessageCommoned.Id,
			"user_id", event.AdditionalData.BehalfUserId,
			"chat_id", event.MessageCommoned.ChatId,
		)
		return nil
	})

	return errOuter
}

func (m *CommonProjection) OnMessageEdited(ctx context.Context, event *MessageEdited) (bool, error) {
	pinned, errOuter := db.TransactWithResult(ctx, m.db, func(tx *db.Tx) (bool, error) {

		chatExists, err := m.checkChatExists(ctx, tx, event.MessageCommoned.ChatId)
		if err != nil {
			return false, err
		}
		if !chatExists {
			m.lgr.InfoContext(ctx, "Skipping MessageCreated because there is no chat", "chat_id", event.MessageCommoned.ChatId)
			return false, nil
		}

		messageBlogPost, err := m.isMessageBlogPost(ctx, tx, event.MessageCommoned.ChatId, event.MessageCommoned.Id)
		if err != nil {
			return false, err
		}

		var isMessagePinned bool
		err = sqlscan.Get(ctx, tx, &isMessagePinned, "select exists (select * from message_pinned where chat_id = $1 and message_id = $2)", event.MessageCommoned.ChatId, event.MessageCommoned.Id)
		if err != nil {
			return false, err
		}

		var embed pgtype.JSONB
		if event.MessageCommoned.Embed != nil {
			err = embed.Set(event.MessageCommoned.Embed)
			if err != nil {
				return false, err
			}
		} else {
			embed.Status = pgtype.Null
		}

		_, err = tx.ExecContext(ctx, `
			update message
			set	
			    content = $3
				, embed = $4
				, update_date_time = $5
				, file_item_uuid = $6
			where chat_id = $2 and id = $1 
		`, event.MessageCommoned.Id, event.MessageCommoned.ChatId, event.MessageCommoned.Content, embed, event.AdditionalData.CreatedAt, event.MessageCommoned.FileItemUuid)
		if err != nil {
			return false, err
		}

		if messageBlogPost {
			_, err = m.refreshBlog(ctx, tx, event.MessageCommoned.ChatId, event.AdditionalData.CreatedAt, nil)
			if err != nil {
				return false, err
			}
		}

		if isMessagePinned {
			previewTxt := m.createMessagePinnedText(event.MessageCommoned.Content)

			_, err = tx.ExecContext(ctx, `
			update message_pinned
			set	
			    preview = $3
				, update_date_time = $4
			where chat_id = $2 and message_id = $1 
		`, event.MessageCommoned.Id, event.MessageCommoned.ChatId, previewTxt, event.AdditionalData.CreatedAt)
			if err != nil {
				return false, err
			}
		}

		m.lgr.InfoContext(ctx,
			"Handling message edited",
			"id", event.MessageCommoned.Id,
			"chat_id", event.MessageCommoned.ChatId,
			"message_id", event.MessageCommoned.Id,
		)
		return isMessagePinned, nil
	})

	return pinned, errOuter
}

func (m *CommonProjection) initializeMessageUnreadMultipleParticipants(ctx context.Context, tx *db.Tx, participantIds []int64, chatId int64) error {
	err := m.setUnreadMessages(ctx, tx, participantIds, chatId, 0, true, false)
	if err != nil {
		return err
	}
	return nil
}

func (m *CommonProjection) OnMessageRemoved(ctx context.Context, event *MessageDeleted) error {
	errOuter := db.Transact(ctx, m.db, func(tx *db.Tx) error {

		messageBlogPost, err := m.isMessageBlogPost(ctx, tx, event.ChatId, event.MessageId)
		if err != nil {
			return err
		}

		_, err = tx.ExecContext(ctx, `
			delete from message where (id, chat_id) = ($1, $2)
		`, event.MessageId, event.ChatId)
		if err != nil {
			return err
		}

		if messageBlogPost {
			_, err = m.refreshBlog(ctx, tx, event.ChatId, event.AdditionalData.CreatedAt, nil)
			if err != nil {
				return err
			}
		}

		return nil
	})
	if errOuter != nil {
		return errOuter
	}

	m.lgr.InfoContext(ctx,
		"Message removed from common chat",
		"message_id", event.MessageId,
		"chat_id", event.ChatId,
	)

	return nil
}

func (m *CommonProjection) setMessagePinned(ctx context.Context, tx *db.Tx, chatId, messageId int64, pinned bool) error {
	_, err := tx.ExecContext(ctx, `update message set pinned = $3 where chat_id = $1 and id = $2`, chatId, messageId, pinned)
	if err != nil {
		return err
	}
	return nil
}

func (m *EnrichingProjection) enrichMessagePinned(ctx context.Context, pinnedMessage *pinnedMessage, chatRegularParticipantCanPinMessage bool, chatIsAdmin bool, messageOwnerUsersMap map[int64]*dto.User) *dto.PinnedMessageDto {

	res := dto.PinnedMessageDto{
		Id:             pinnedMessage.Id,
		Text:           pinnedMessage.Text,
		ChatId:         pinnedMessage.ChatId,
		OwnerId:        pinnedMessage.OwnerId,
		Owner:          messageOwnerUsersMap[pinnedMessage.OwnerId],
		PinnedPromoted: pinnedMessage.Promoted,
		CreateDateTime: pinnedMessage.CreateDateTime,
		CanPin:         CanPinMessage(chatRegularParticipantCanPinMessage, chatIsAdmin),
	}

	return &res
}

func (m *EnrichingProjection) GetPinnedPromotedMessage(ctx context.Context, chatId, behalfUserId int64) (*dto.PinnedMessageDto, bool, error) {
	type resDto struct {
		promoted        *dto.PinnedMessageDto
		notAParticipant bool
	}

	res, errOuter := db.TransactWithResult(ctx, m.cp.db, func(tx *db.Tx) (*resDto, error) {
		participant, err := m.cp.IsParticipant(ctx, tx, behalfUserId, chatId)
		if err != nil {
			return nil, err
		}

		if !participant {
			return &resDto{
				notAParticipant: true,
			}, nil
		}

		type promotedDto struct {
			MessageId int64 `db:"message_id"`
			OwnerId   int64 `db:"owner_id"`
		}

		var promoted promotedDto
		var promotedP *promotedDto
		err = sqlscan.Get(ctx, tx, &promoted, "select message_id, owner_id from message_pinned where chat_id = $1 and promoted = true order by create_date_time desc limit 1", chatId)
		if errors.Is(err, sql.ErrNoRows) {
			// there were no rows, but otherwise no error occurred
		} else if err != nil {
			return nil, err
		} else {
			// ok
			promotedP = &promoted
		}

		var pr *dto.PinnedMessageDto
		if promotedP != nil {
			users, err := m.aaaRestClient.GetUsers(ctx, []int64{promotedP.OwnerId})
			if err != nil {
				m.lgr.WarnContext(ctx, "unable to get users")
			}

			usersMap := utils.ToMap(users)

			enricheds, err := m.GetPinnedMessageEnriched(ctx, tx, chatId, promotedP.MessageId, []int64{behalfUserId}, usersMap)
			if err != nil {
				return nil, err
			}

			pr = enricheds[behalfUserId]
		}

		return &resDto{
			promoted: pr,
		}, nil
	})
	if errOuter != nil {
		return nil, false, errOuter
	}

	return res.promoted, res.notAParticipant, nil
}

type pinnedMessage struct {
	Id             int64     `db:"message_id"`
	ChatId         int64     `db:"chat_id"`
	OwnerId        int64     `db:"owner_id"`
	CreateDateTime time.Time `db:"create_date_time"`
	Text           string    `db:"preview"`
	Promoted       bool      `db:"promoted"`
}

func (m *EnrichingProjection) GetPinnedMessageEnriched(ctx context.Context, co db.CommonOperations, chatId, messageId int64, behalfUserIds []int64, messageOwnerUsersMap map[int64]*dto.User) (map[int64]*dto.PinnedMessageDto, error) {
	pinned, err := m.cp.GetPinnedMessage(ctx, co, chatId, messageId)
	if err != nil {
		return nil, err
	}
	if pinned != nil {
		areAdmins, err := m.cp.getAreAdminsOfUserIds(ctx, co, behalfUserIds, chatId)
		if err != nil {
			return nil, err
		}

		cb, err := m.cp.GetChatBasic(ctx, co, chatId)
		if err != nil {
			return nil, err
		}

		resMap := map[int64]*dto.PinnedMessageDto{}

		if cb != nil {
			for _, participantId := range behalfUserIds {
				pinnedEnriched := m.enrichMessagePinned(ctx, pinned, cb.RegularParticipantCanPinMessage, areAdmins[participantId], messageOwnerUsersMap)
				resMap[participantId] = pinnedEnriched
			}
		} else {
			m.lgr.ErrorContext(ctx, "Chat isn't found", "chat_id", chatId)
		}

		return resMap, nil
	} else {
		return nil, nil
	}
}

func (m *CommonProjection) GetPinnedMessage(ctx context.Context, co db.CommonOperations, chatId, messageId int64) (*pinnedMessage, error) {
	var pm pinnedMessage
	err := sqlscan.Get(ctx, co, &pm, `
	select 
		message_id
		,chat_id
		,owner_id
		,create_date_time
		,preview
		,promoted
	from message_pinned 
	where chat_id = $1 and message_id = $2
	`, chatId, messageId)
	if errors.Is(err, sql.ErrNoRows) {
		// there were no rows, but otherwise no error occurred
	} else if err != nil {
		return nil, err
	}

	return &pm, nil
}

func (m *CommonProjection) GetPinnedMessageCount(ctx context.Context, co db.CommonOperations, chatId int64) (int64, error) {
	var count int64
	err := sqlscan.Get(ctx, co, &count, "select count (*) from message_pinned where chat_id = $1", chatId)
	if err != nil {
		return 0, err
	}
	return count, nil
}

func (m *CommonProjection) createMessagePinnedText(content string) string {
	return preview.CreateMessagePreviewWithoutLogin(m.stripTags, m.cfg.Message.PreviewMaxTextSize, m.stripTags.Sanitize(content))
}

func (m *CommonProjection) tryNominatePreviousToPromote(ctx context.Context, co db.CommonOperations, chatId int64) (*int64, error) {

	var previousPinned *int64
	err := sqlscan.Get(ctx, co, &previousPinned, "select message_id from message_pinned where chat_id = $1 order by create_date_time desc limit 1", chatId)
	if errors.Is(err, sql.ErrNoRows) {
		// there were no rows, but otherwise no error occurred
	} else if err != nil {
		return nil, err
	}

	if previousPinned != nil {
		_, err := co.ExecContext(ctx, "update message_pinned set promoted = true where chat_id = $1 and message_id = $2", chatId, *previousPinned)
		if err != nil {
			return nil, err
		}
	}

	return previousPinned, nil
}

func (m *CommonProjection) OnMessagePinned(ctx context.Context, event *MessagePinned) (*int64, int64, error) {
	type resDto struct {
		count             int64
		promotedMessageId *int64
	}
	res, errOuter := db.TransactWithResult(ctx, m.db, func(tx *db.Tx) (*resDto, error) {
		var count int64
		var promotedMessageId *int64
		if event.Pinned {
			mb, err := m.GetMessageBasic(ctx, tx, event.ChatId, event.MessageId)
			if err != nil {
				return nil, err
			}

			if mb != nil {
				previewTxt := m.createMessagePinnedText(mb.Content)

				_, err = tx.ExecContext(ctx, `
					insert into message_pinned (chat_id, message_id, owner_id, create_date_time, update_date_time, preview, promoted)
					values ($1, $2, $3, $4, $5, $6, true)
					on conflict (chat_id, message_id) do update set
					preview = excluded.preview
					,update_date_time = excluded.update_date_time
				`,
					event.ChatId, event.MessageId, mb.OwnerId, event.AdditionalData.CreatedAt, event.AdditionalData.CreatedAt, previewTxt)
				if err != nil {
					return nil, err
				}

				// set pinned
				err = m.setMessagePinned(ctx, tx, event.ChatId, event.MessageId, true)
				if err != nil {
					return nil, err
				}

				// unpromote previous
				_, err = tx.ExecContext(ctx, `update message_pinned set promoted = false where chat_id = $1 and message_id != $2`, event.ChatId, event.MessageId)
				if err != nil {
					return nil, err
				}

				promotedMessageId = &event.MessageId
			} else {
				m.lgr.InfoContext(ctx, "Skipping pinning the mesage because it is not exists", "chat_id", event.ChatId, "message_id", event.MessageId)
			}
		} else {
			// unpin
			_, err := tx.ExecContext(ctx, "delete from message_pinned where chat_id = $1 and message_id = $2", event.ChatId, event.MessageId)
			if err != nil {
				return nil, err
			}

			// set pinned
			err = m.setMessagePinned(ctx, tx, event.ChatId, event.MessageId, false)
			if err != nil {
				return nil, err
			}

			promotedMessageId, err = m.tryNominatePreviousToPromote(ctx, tx, event.ChatId)
			if err != nil {
				return nil, err
			}
		}

		count, err := m.GetPinnedMessageCount(ctx, tx, event.ChatId)
		if err != nil {
			return nil, err
		}

		return &resDto{
			count:             count,
			promotedMessageId: promotedMessageId,
		}, nil
	})
	if errOuter != nil {
		return nil, 0, errOuter
	}
	return res.promotedMessageId, res.count, nil
}

func (m *CommonProjection) setLastMessage(ctx context.Context, tx *db.Tx, chatId int64) error {
	_, err := tx.ExecContext(ctx, `
		with last_message as (
			select 
				m.id,
				m.owner_id, 
				nullif(trim(left(strip_tags(m.content), $2)), '') as content,
				nullif(trim(left(strip_tags(embed ->> 'embedMessageContent'), $2)), '') as embed_content
			from message m 
			where m.chat_id = $1 and m.id = (select max(mm.id) from message mm where mm.chat_id = $1)
		)
		UPDATE chat_common 
		SET 
			last_message_id = (select id from last_message),
			last_message_content = (select coalesce(content, embed_content) from last_message),
			last_message_owner_id = (select owner_id from last_message)
		WHERE id = $1;
	`, chatId, m.cfg.Cqrs.Projections.ChatUserView.LastMessageMaxTextDbPreviewSize)
	if err != nil {
		return fmt.Errorf("error during setting last message: %w", err)
	}
	return nil
}

func (m *CommonProjection) setUnreadMessages(ctx context.Context, tx *db.Tx, participantIds []int64, chatId, messageId int64, needSet, needRefresh bool) error {
	_, err := tx.ExecContext(ctx, `
		with 
		chat_messages as (
			select m.id from message m where m.chat_id = $2
		),
		max_message as (
			select max(m.id) as max from chat_messages m
		),
		normalized_user as (
			select unnest(cast ($1 as bigint[])) as user_id
		),
		last_message as (
			select 
				coalesce(ww.last_message_id, 0) as last_message_id,
				nu.user_id
			from (
				select
					(case
						when exists(select * from chat_user_view uw where uw.id = $2 and uw.user_id = w.user_id and uw.cuv_last_read_message_id > 0)
						then coalesce(
							(select m.id as last_message_id from chat_messages m where m.id = w.cuv_last_read_message_id),
							(select max from max_message where $5 = true) -- allow taking max_message only in case $5 = true
						)
					end) as last_message_id,
					w.user_id
				from chat_user_view w 
				where w.id = $2 and w.user_id = any($1)
			) ww
			right join normalized_user nu on ww.user_id = nu.user_id
		),
		existing_message as (
			select coalesce(
				(select m.id from chat_messages m where m.id = $3),
				(select max from max_message),
				0
			) as normalized_read_message_id
		),
		normalized_given_message as (
			select 
				n.user_id,
				(case 
					when $4 = true then (select l.last_message_id from last_message l where l.user_id = n.user_id)
					else (select normalized_read_message_id from existing_message) 
				end) as normalized_read_message_id
			from normalized_user n
		),
		input_data as (
			select
				ngm.user_id as user_id,
				cast ($2 as bigint) as chat_id,
				(
					SELECT count(m.id) FILTER(WHERE m.id > (select normalized_read_message_id from normalized_given_message n where n.user_id = ngm.user_id))
					FROM chat_messages m
				) as unread_messages,
				ngm.normalized_read_message_id as last_read_message_id
			from normalized_given_message ngm
		)
		merge into chat_user_view cuv
		using input_data idt
		on (idt.chat_id, idt.user_id) = (cuv.id, cuv.user_id)
		when matched then update set 
		   unread_messages = idt.unread_messages
		  ,cuv_last_read_message_id = idt.last_read_message_id
	`, participantIds, chatId, messageId, needSet, needRefresh)
	if err != nil {
		return err
	}

	err = m.updateHasUnreads(ctx, tx, participantIds)
	if err != nil {
		return err
	}

	return nil
}

func (m *CommonProjection) getCommonInputData() string {
	return `
		input_data as (
			select
				uv.id as chat_id
				,uv.user_id
				,cc.last_message_id
			from chat_user_view uv
			join chat_common cc on uv.id = cc.id
			where uv.user_id = $1 
				and uv.unread_messages > 0 -- inn.unread_messages > 0 is required to always return pass pages to uv.id and, consequently, to return the full pages in returning
			order by uv.id 
			limit $2 offset $3
		)
	`
}

func (m *CommonProjection) setNoUnreadsInAllChats(ctx context.Context, co db.CommonOperations, userId int64, size int, offset int64) ([]dto.ChatUserViewBasic, error) {
	updatedChatsPortion := []dto.ChatUserViewBasic{}

	q := fmt.Sprintf(`
		with
		%s
		update chat_user_view cuv 
		set (unread_messages, cuv_last_read_message_id) = (
			select 0, idt.last_message_id 
			from input_data idt
			where (idt.chat_id, idt.user_id) = (cuv.id, cuv.user_id)
		)
		where (cuv.id, cuv.user_id) in (select idtt.chat_id, idtt.user_id from input_data idtt) -- to avoid null. merge with return isn't supported
		returning cuv.id, cuv.unread_messages, cuv.update_date_time
	`, m.getCommonInputData())

	err := sqlscan.Select(ctx, co, &updatedChatsPortion, q, userId, size, offset)
	if err != nil {
		return nil, err
	}

	return updatedChatsPortion, nil
}

func (m *CommonProjection) updateParticipantMessageReadId(ctx context.Context, co db.CommonOperations, userId, chatId, messageId int64, lastReadMessageDateTime time.Time) error {
	_, err := co.ExecContext(ctx, `
		with
		max_message as (
			select max(id) as max from message where chat_id = $2
		),
		max_message_normalized as (
			select coalesce((select max from max_message), 0) as max
		),
		normalized_message as (
			select case 
				when cast($3 as bigint) <= (select max from max_message_normalized) then cast($3 as bigint)
				else (select max from max_message_normalized)
			end
			as id
		)
		UPDATE chat_participant 
		SET 
			 cp_last_read_message_id = (select id from normalized_message)
			,cp_last_read_message_date_time = $4
		WHERE (user_id, chat_id) = ($1, $2);
	`, userId, chatId, messageId, lastReadMessageDateTime)
	return err
}

func (m *CommonProjection) fastForwardParticipantMessageReadId(ctx context.Context, co db.CommonOperations, userId, chatId int64, lastReadMessageDateTime time.Time) error {
	_, err := co.ExecContext(ctx, `
		with 
		curr_message as (
			select coalesce((select cp_last_read_message_id from chat_participant where (user_id, chat_id) = ($1, $2)), 0) as curr
		),
		max_message as (
			select coalesce((select max(id) from message where chat_id = $2), 0) as max
		)
		UPDATE chat_participant 
		SET 
		    cp_last_read_message_id = (select max from max_message)
			,cp_last_read_message_date_time = $3
		WHERE 
			(user_id, chat_id) = ($1, $2)
			and (select curr from curr_message) != (select max from max_message)
	`, userId, chatId, lastReadMessageDateTime)
	return err
}

func (m *CommonProjection) fastForwardParticipantMessageReadIdInAllChats(ctx context.Context, co db.CommonOperations, userId int64, size int, offset int64, lastReadMessageDateTime time.Time) error {
	q := fmt.Sprintf(`
		with
		%s
		update chat_participant cuv 
		set 
		cp_last_read_message_id = (
			select idt.last_message_id 
			from input_data idt
			where (idt.chat_id, idt.user_id) = (cuv.chat_id, cuv.user_id)
		)
		,cp_last_read_message_date_time = $4
		where (cuv.chat_id, cuv.user_id) in (select idtt.chat_id, idtt.user_id from input_data idtt)
	`, m.getCommonInputData())

	_, err := co.ExecContext(ctx, q, userId, size, offset, lastReadMessageDateTime)
	if err != nil {
		return err
	}

	return nil
}

func (m *CommonProjection) fastForwardLastRead(ctx context.Context, co db.CommonOperations, userId, chatId int64) error {
	_, err := co.ExecContext(ctx, `
		UPDATE chat_user_view 
		SET unread_messages = 0, cuv_last_read_message_id = (select max(id) from message where chat_id = $2)
		WHERE (user_id, id) = ($1, $2);
	`, userId, chatId)
	return err
}

// should be called after upserting into unread_messages_user_view otherwise it's going to reset has to false
func (m *CommonProjection) updateHasUnreads(ctx context.Context, tx *db.Tx, participantIds []int64) error {
	_, err := tx.ExecContext(ctx, `
	with
	normalized_user as (
		select unnest(cast ($1 as bigint[])) as user_id
	),	
	users_hases as (
		select 
			uv.user_id, 
			(any_value(uv.unread_messages) filter (where uv.unread_messages > 0 and uv.consider_messages_as_unread)) != 0 as has 
		from chat_user_view uv
		where uv.user_id = any($1)
		group by (uv.user_id)
	),
	input_data as (
		select 
			nu.user_id,
			coalesce(uh.has, false) as has
		from normalized_user nu
		left join users_hases uh on nu.user_id = uh.user_id
	)
	insert into has_unread_messages(user_id, has)
	select user_id, has from input_data
	on conflict (user_id) do update
	set has = excluded.has
	`, participantIds)
	return err
}

func (m *CommonProjection) increaseUnreadsAndSetHasUnreads(ctx context.Context, co db.CommonOperations, participantIds []int64, chatId int64, increaseOn int) error {
	_, err := co.ExecContext(ctx, `
		UPDATE chat_user_view 
		SET unread_messages = unread_messages + $3
		WHERE user_id = any($1) and id = $2;
	`, participantIds, chatId, increaseOn)
	if err != nil {
		return err
	}

	// upsert only for sake using CTE
	_, err = co.ExecContext(ctx, `
		with input_data as (
			SELECT 
				ch.user_id as user_id, 
				true as has
			FROM chat_user_view ch 
			WHERE ch.id = $2 AND ch.user_id = any($1) and ch.consider_messages_as_unread
		)	
		insert into has_unread_messages(user_id, has)
		select idt.user_id, idt.has
		from input_data idt
		on conflict (user_id) do update set
		has = excluded.has
	`, participantIds, chatId)
	if err != nil {
		return err
	}

	return nil
}

func (m *CommonProjection) setHasNoUnreadsInAllChats(ctx context.Context, co db.CommonOperations, userId int64) error {
	_, err := co.ExecContext(ctx, "update has_unread_messages set has = false where user_id = $1", userId)
	if err != nil {
		return err
	}
	return nil
}

func CanReadMessage(isParticipant bool) bool {
	return isParticipant
}

func (m *CommonProjection) OnUnreadMessageReaded(ctx context.Context, event *MessageReaded, allChatsReadedConsumer func([]dto.ChatUserViewBasic)) error {
	if event.ReadMessagesAction == ReadMessagesActionOneMessage || event.ReadMessagesAction == ReadMessagesActionAllMessagesInOneChat {
		errOuter := db.Transact(ctx, m.db, func(tx *db.Tx) error {
			if event.ReadMessagesAction == ReadMessagesActionOneMessage {

				err := m.setUnreadMessages(ctx, tx, []int64{event.AdditionalData.BehalfUserId}, event.ChatId, event.MessageId, false, false) // includes updateHasUnreads()
				if err != nil {
					return err
				}

				err = m.updateParticipantMessageReadId(ctx, tx, event.AdditionalData.BehalfUserId, event.ChatId, event.MessageId, event.AdditionalData.CreatedAt)
				if err != nil {
					return err
				}

				return nil
			} else if event.ReadMessagesAction == ReadMessagesActionAllMessagesInOneChat {

				err := m.fastForwardLastRead(ctx, tx, event.AdditionalData.BehalfUserId, event.ChatId)
				if err != nil {
					return err
				}

				err = m.updateHasUnreads(ctx, tx, []int64{event.AdditionalData.BehalfUserId})
				if err != nil {
					return err
				}

				err = m.fastForwardParticipantMessageReadId(ctx, tx, event.AdditionalData.BehalfUserId, event.ChatId, event.AdditionalData.CreatedAt)
				if err != nil {
					return err
				}

				return nil
			} else {
				return fmt.Errorf("Unknown action: %T", event.ReadMessagesAction)
			}
		})
		if errOuter != nil {
			return fmt.Errorf("error during read messages: %w", errOuter)
		}
	} else if event.ReadMessagesAction == ReadMessagesActionAllChats {
		offset := int64(0)
		for {

			// deliberately don't use transaction in order not to span transaction over all the loop iterations
			updatedChatsPortion, err := m.setNoUnreadsInAllChats(ctx, m.db, event.AdditionalData.BehalfUserId, utils.DefaultSize, offset)
			if err != nil {
				return err
			}

			err = m.fastForwardParticipantMessageReadIdInAllChats(ctx, m.db, event.AdditionalData.BehalfUserId, utils.DefaultSize, offset, event.AdditionalData.CreatedAt)
			if err != nil {
				return err
			}

			allChatsReadedConsumer(updatedChatsPortion)

			if len(updatedChatsPortion) < utils.DefaultSize {
				break
			}

			offset += utils.DefaultSize
		}

		err := m.setHasNoUnreadsInAllChats(ctx, m.db, event.AdditionalData.BehalfUserId)
		if err != nil {
			return err
		}
	} else {
		return fmt.Errorf("Unknown action: %T", event.ReadMessagesAction)
	}
	return nil
}

func (m *CommonProjection) OnMessageReactionFlipped(ctx context.Context, event *MessageReactionFlipped) (bool, error) {
	wasAdded, errOuter := db.TransactWithResult(ctx, m.db, func(tx *db.Tx) (bool, error) {
		var wasAddedInner bool

		messageExists, errInner := m.checkMessageExists(ctx, tx, event.ChatId, event.MessageId)
		if errInner != nil {
			return false, errInner
		}

		if !messageExists {
			m.lgr.InfoContext(ctx, "Skipping MessageReactionFlipped because there is no message", "chat_id", event.ChatId, "message_id", event.MessageId)
			return false, nil
		}

		var reactionExists bool
		errInner = sqlscan.Get(ctx, tx, &reactionExists, "SELECT EXISTS(SELECT 1 FROM message_reaction WHERE chat_id = $1 AND message_id = $2 AND user_id = $3 AND reaction = $4)", event.ChatId, event.MessageId, event.AdditionalData.BehalfUserId, event.Reaction)
		if errInner != nil {
			return false, errInner
		}

		if !reactionExists {
			_, errInner = tx.ExecContext(ctx, `
			insert into message_reaction(chat_id, message_id, user_id, reaction, create_date_time)
			values ($1, $2, $3, $4, $5)
			on conflict (chat_id, message_id, user_id, reaction) do nothing
		`, event.ChatId, event.MessageId, event.AdditionalData.BehalfUserId, event.Reaction, event.AdditionalData.CreatedAt)
			if errInner != nil {
				return false, errInner
			}
			wasAddedInner = true
		} else {
			_, errInner = tx.ExecContext(ctx, "DELETE FROM message_reaction WHERE chat_id = $1 AND message_id = $2 AND user_id = $3 AND reaction = $4", event.ChatId, event.MessageId, event.AdditionalData.BehalfUserId, event.Reaction)
			if errInner != nil {
				return false, errInner
			}
		}
		return wasAddedInner, nil
	})
	return wasAdded, errOuter
}

func (m *CommonProjection) checkMessageExists(ctx context.Context, co db.CommonOperations, chatId, messageId int64) (bool, error) {
	var messageExists bool
	err := sqlscan.Get(ctx, co, &messageExists, "select exists (select * from message where chat_id = $1 and id = $2)", chatId, messageId)
	if err != nil {
		return false, err
	}
	return messageExists, nil
}

func (m *CommonProjection) GetMessageOwner(ctx context.Context, chatId, messageId int64) (int64, error) {
	var ownerId int64
	err := sqlscan.Get(ctx, m.db, &ownerId, "select owner_id from message where (chat_id, id) = ($1, $2)", chatId, messageId)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			// there were no rows, but otherwise no error occurred
			return dto.NoOwner, nil
		} else {
			return 0, err
		}
	}
	return ownerId, nil
}

func (m *CommonProjection) GetLastMessageReaded(ctx context.Context, chatId, userId int64) (int64, bool, int64, error) {
	type lastMessageReaded struct {
		LastReadedMessageId int64 `db:"last_readed_message_id"`
		Has                 bool  `db:"has"`
		MaxMessageId        int64 `db:"max_message_id"`
	}

	res := lastMessageReaded{}

	err := sqlscan.Get(ctx, m.db, &res, `
	with
	chat_messages as (
		select m.id from message m where m.chat_id = $2
	),
	user_last_read_message as (
		select cuv_last_read_message_id as last_read_message_id from chat_user_view um 
		where (um.user_id, um.id) = ($1, $2)
	)
	select 
	    (select last_read_message_id from user_last_read_message) as last_readed_message_id, 
	    exists(select * from chat_messages m where m.id = cc.last_message_id) as has,
	    (select max(m.id) from chat_messages m) as max_message_id
	from chat_common cc 
    where cc.id = $2
	`, userId, chatId)
	if err != nil {
		return 0, false, 0, err
	}
	return res.LastReadedMessageId, res.Has, res.MaxMessageId, nil
}

func (m *CommonProjection) GetLastMessageId(ctx context.Context, chatId int64) (int64, error) {
	var maxMessageId int64
	err := sqlscan.Get(ctx, m.db, &maxMessageId, `
		select coalesce(inn.max_id, 0) 
		from (select max(id) as max_id from message m where m.chat_id = $1) inn
		`, chatId)
	if err != nil {
		return 0, err
	}
	return maxMessageId, nil
}

func (m *EnrichingProjection) GetMessagesEnriched(ctx context.Context, behalfUserIds []int64, needCheckAuth bool, authForUserId *int64, chatId int64, size int32, startingFromItemId *int64, includeStartingFrom, reverse bool, searchString string, messageId *int64, additionalUserIdToFetch []int64) ([]dto.MessageViewEnrichedDto, bool, []*dto.User, error) {
	type resDto struct {
		items           []dto.MessageViewEnrichedDto
		notAparticipant bool
		users           []*dto.User
	}
	res, errOuter := db.TransactWithResult(ctx, m.cp.db, func(tx *db.Tx) (*resDto, error) {
		if needCheckAuth {
			if authForUserId != nil {
				participant, err := m.cp.IsParticipant(ctx, m.cp.db, *authForUserId, chatId)
				if err != nil {
					return nil, err
				}
				if !participant {
					return &resDto{
						items:           nil,
						notAparticipant: true,
					}, nil
				}
			} else {
				return nil, errors.New("Unknown invariant")
			}
		}

		searchString = sanitizer.TrimAmdSanitize(m.policy, searchString)

		messages, err := m.cp.GetMessages(ctx, tx, chatId, size, startingFromItemId, includeStartingFrom, reverse, searchString, messageId)
		if err != nil {
			m.lgr.ErrorContext(ctx, "Error getting messages", "err", err)
			return nil, err
		}

		if messageId != nil {
			if len(messages) > 1 {
				return nil, fmt.Errorf("By id = %d %v messages got", *messageId, len(messages))
			}

			if len(messages) == 1 {
				var messagesTmp []dto.MessageDto
				for _, userId := range behalfUserIds {
					msg := messages[0]
					msg.BehalfUserId = userId
					messagesTmp = append(messagesTmp, msg)
				}
				messages = messagesTmp
			}
		} else if len(behalfUserIds) == 1 {
			for i := range messages {
				messages[i].BehalfUserId = behalfUserIds[0]
			}
		} else {
			return nil, fmt.Errorf("Unknown invariant")
		}

		messageIds := make([]int64, 0)
		for _, message := range messages {
			messageIds = append(messageIds, message.Id)
		}

		reactions, err := m.getReactions(ctx, tx, chatId, messageIds)
		if err != nil {
			return nil, fmt.Errorf("Got error during enriching messages with reactions: %v", err)
		}

		var usersSet = map[int64]bool{}
		var chatsPreSet = map[int64]bool{}
		for _, message := range messages {
			err = populateSets(message.Id, message.OwnerId, additionalUserIdToFetch, message.Embed, usersSet, chatsPreSet, chatId, reactions)
			if err != nil {
				return nil, err
			}
		}
		chatsByUserIdByChatId, err := m.cp.GetChatsBasicExtended(ctx, tx, utils.SetMapIdBoolToSlice(chatsPreSet), behalfUserIds)
		if err != nil {
			m.lgr.ErrorContext(ctx, "Error getting chat basic", "err", err)
			return nil, err
		}

		// it's ok because we have 1 chat in the both cases
		areAdmins, err := m.cp.getAreAdminsOfUserIds(ctx, tx, behalfUserIds, chatId)
		if err != nil {
			return nil, err
		}

		users, err := m.aaaRestClient.GetUsers(ctx, utils.SetMapIdBoolToSlice(usersSet))
		if err != nil {
			m.lgr.WarnContext(ctx, "unable to get users")
		}

		notAparticipant := false

		usersMap := utils.ToMap(users)

		messagesEnriched := make([]dto.MessageViewEnrichedDto, 0, len(messages))
		for _, mm := range messages {
			bloggingAllowed := IsBloggingAllowed(m.cfg, getUserRoles(usersMap, mm.BehalfUserId))

			me, err := enrichMessage(
				ctx, m.lgr,
				m.cfg,
				mm,
				chatId,
				usersMap,
				chatsByUserIdByChatId,
				reactions,
				mm.BehalfUserId,
				areAdmins,
				!notAparticipant,
				bloggingAllowed,
			)
			if err != nil {
				return nil, err
			}
			messagesEnriched = append(messagesEnriched, *me)
		}
		return &resDto{
			items:           messagesEnriched,
			notAparticipant: notAparticipant,
			users:           users,
		}, nil
	})

	if errOuter != nil {
		return nil, false, nil, errOuter
	}

	return res.items, res.notAparticipant, res.users, nil
}

func getUserRoles(usersMap map[int64]*dto.User, behalfUserId int64) []string {
	user := usersMap[behalfUserId]
	if user == nil || user.AdditionalData == nil {
		return []string{}
	}

	return user.AdditionalData.Roles
}

func IsBloggingAllowed(cfg *config.AppConfig, userRoles []string) bool {
	if !cfg.Blog.OnlyAdminCanCreateBlog {
		return true
	}

	return slices.Contains(userRoles, dto.ROLE_ADMIN)
}

func getReactionsCommon(ctx context.Context, co db.CommonOperations, chatId int64, messageIds []int64, reaction *string, maxDisplayableUsers int) ([]dto.ReactionDto, error) {
	type reactionDto struct {
		MessageId int64            `db:"message_id"`
		UserIds   pgtype.Int8Array `db:"user_ids"`
		Reaction  string           `db:"reaction"`
		Count     int64            `db:"count"`
	}

	reactions := []reactionDto{}
	res := []dto.ReactionDto{}

	sqlArgs := []any{chatId, messageIds, maxDisplayableUsers}

	var additionalCondition string
	if reaction != nil {
		sqlArgs = append(sqlArgs, *reaction)
		additionalCondition = fmt.Sprintf("and reaction = $%d", len(sqlArgs))
	}

	q := fmt.Sprintf(`
		with
		requested_message_reactions as (
			select * from message_reaction where chat_id = $1 and message_id = any($2) %s
		),
		reaction_counts as (
			select message_id, reaction, count(user_id) as count
			from requested_message_reactions group by message_id, reaction
		),
		reaction_users_last_n as (
			select
				message_id,
				reaction,
				(array_agg(user_id order by create_date_time))[:$3] as user_ids,
				min(create_date_time) as create_date_time
			from message_reaction group by message_id, reaction
		)
		select
			rc.message_id,
			rc.reaction,
			rn.user_ids,
			rc.count
		from reaction_counts rc
		join reaction_users_last_n rn on (rc.message_id, rc.reaction) = (rn.message_id, rn.reaction)
		order by rn.create_date_time
		`, additionalCondition)

	err := sqlscan.Select(ctx, co, &reactions, q, sqlArgs...)
	if err != nil {
		return res, fmt.Errorf("error during interacting with db: %w", err)
	}

	for i, de := range reactions {
		mapped := dto.ReactionDto{
			MessageId: de.MessageId,
			Reaction:  de.Reaction,
			Count:     de.Count,
		}
		err = de.UserIds.AssignTo(&mapped.UserIds)
		if err != nil {
			return res, fmt.Errorf("error during mapping on index %d: %w", i, err)
		}
		res = append(res, mapped)
	}

	return res, nil
}

func (m *EnrichingProjection) getReactions(ctx context.Context, co db.CommonOperations, chatId int64, messageIds []int64) (map[int64][]dto.ReactionDto, error) {
	ret := map[int64][]dto.ReactionDto{} // messageId:reactionList

	reactions, err := getReactionsCommon(ctx, co, chatId, messageIds, nil, m.cfg.Message.MaxDisplayableReactionUsers)
	if err != nil {
		return ret, fmt.Errorf("error during interacting with db: %w", err)
	}

	for _, reaction := range reactions {
		if _, found := ret[reaction.MessageId]; !found {
			ret[reaction.MessageId] = []dto.ReactionDto{}
		}

		ret[reaction.MessageId] = append(ret[reaction.MessageId], reaction)
	}
	return ret, nil
}

func (m *CommonProjection) GetReaction(ctx context.Context, co db.CommonOperations, chatId, messageId int64, reaction string) (dto.ReactionDto, error) {
	reactions, err := getReactionsCommon(ctx, co, chatId, []int64{messageId}, &reaction, m.cfg.Message.MaxDisplayableReactionUsers)
	if err != nil {
		return dto.ReactionDto{}, fmt.Errorf("error during interacting with db: %w", err)
	}

	if len(reactions) == 0 {
		return dto.ReactionDto{
			MessageId: messageId,
			UserIds:   []int64{},
			Reaction:  reaction,
			Count:     0,
		}, nil
	}

	if len(reactions) > 1 {
		return dto.ReactionDto{}, fmt.Errorf("wrong invarint: more than 1 reaction: %w", err)
	}

	r := reactions[0]

	return r, nil
}

func populateSets(messageId, messageOwnerId int64, additionalUserIdToFetch []int64, embed dto.Embeddable, usersSet map[int64]bool, chatsPreSet map[int64]bool, currentChatId int64, reactions map[int64][]dto.ReactionDto) error {
	usersSet[messageOwnerId] = true

	for _, au := range additionalUserIdToFetch {
		usersSet[au] = true
	}

	chatsPreSet[currentChatId] = true

	if embed != nil {
		switch typed := embed.(type) {
		case *dto.EmbedReply:
			var embeddedMessageReplyOwnerId = typed.OwnerId
			usersSet[embeddedMessageReplyOwnerId] = true
		case *dto.EmbedResend:
			var embeddedMessageResendOwnerId = typed.OwnerId
			usersSet[embeddedMessageResendOwnerId] = true
			var embeddedMessageResendChatId = typed.ChatId
			chatsPreSet[embeddedMessageResendChatId] = true
		default:
			return fmt.Errorf("Unknown type in populateSets: %T", typed)
		}
	}

	takeOnAccountReactions(messageId, usersSet, reactions)

	return nil
}

func takeOnAccountReactions(messageId int64, ownersSet map[int64]bool, messageReactions map[int64][]dto.ReactionDto) {
	rl, ok := messageReactions[messageId]
	if ok {
		for _, r := range rl {
			for _, u := range r.UserIds {
				ownersSet[u] = true
			}
		}
	}
}

func (m *CommonProjection) getChatNameForNotification(ctx context.Context, co db.CommonOperations, chatId int64) (string, error) {
	chatBasic, err := m.GetChatBasic(ctx, co, chatId)
	if err != nil {
		return "", err
	}
	chatName := chatBasic.Title
	if chatBasic.TetATet {
		chatName = ""
	}
	return chatName, nil

}

func enrichMessage(
	ctx context.Context, lgr *logger.LoggerWrapper, cfg *config.AppConfig,
	m dto.MessageDto,
	chatId int64,
	users map[int64]*dto.User,
	chatsByUserIdByChatId map[int64]map[int64]*dto.BasicChatDtoExtended,
	reactions map[int64][]dto.ReactionDto,
	behalfUserId int64,
	areAdmins map[int64]bool,
	isParticipant bool,
	bloggingIsAllowed bool,
) (*dto.MessageViewEnrichedDto, error) {
	me := dto.MessageViewEnrichedDto{
		Id:      m.Id,
		ChatId:  chatId,
		OwnerId: m.OwnerId,
		// no need to patchStorageUrlToPreventCachingVideo because there is no video html tags
		Content:        m.Content,
		BlogPost:       m.BlogPost,
		UpdateDateTime: m.UpdateDateTime,
		CreateDateTime: m.CreateDateTime,
		Owner:          users[m.OwnerId],
		BehalfUserId:   behalfUserId,
		FileItemUuid:   m.FileItemUuid,
		Pinned:         m.Pinned,
	}

	chatsBehalfUser := chatsByUserIdByChatId[behalfUserId]
	embed, err := makeEmbed(m.Embed, users, chatsBehalfUser)
	if err != nil {
		return nil, err
	}
	me.EmbedMessage = embed

	rl := reactions[m.Id]
	me.Reactions = makeReactions(users, rl)

	chat := chatsBehalfUser[chatId]
	if chat == nil {
		return nil, fmt.Errorf("Logical error during enriching messages not found chat by chatId = %v, userId = %v", chatId, behalfUserId)
	}

	setMessagePersonalizedFields(&me, chat.TetATet, chat.IsBlog, chat.RegularParticipantCanPublishMessage, chat.RegularParticipantCanPinMessage, chat.RegularParticipantCanWriteMessage, areAdmins[behalfUserId], behalfUserId, isParticipant, bloggingIsAllowed)

	return &me, nil
}

func setMessagePersonalizedFields(copied *dto.MessageViewEnrichedDto, chatTetATet, chatIsBlog, chatRegularParticipantCanPublishMessage, chatRegularParticipantCanPinMessage, chatCanWriteMessage, chatIsAdmin bool, participantId int64, isParticipant bool, bloggingIsAllowed bool) {
	canWriteMessage := CanWriteMessage(isParticipant, chatIsAdmin, chatCanWriteMessage)

	copied.CanEdit = CanEditMessage(participantId, copied.OwnerId, copied.EmbedMessage != nil, copied.GetEmbedTypeSafe(), canWriteMessage)
	copied.CanSyncEmbed = CanSyncEmbedMessage(participantId, copied.OwnerId, copied.EmbedMessage != nil, canWriteMessage)
	copied.CanDelete = CanDeleteMessage(participantId, copied.OwnerId, canWriteMessage)
	copied.CanPublish = CanPublishMessage(chatRegularParticipantCanPublishMessage, chatIsAdmin, copied.OwnerId, participantId)
	copied.CanPin = CanPinMessage(chatRegularParticipantCanPinMessage, chatIsAdmin)

	copied.CanMakeBlogPost = CanMakeMessageBlogPost(chatIsAdmin, chatTetATet, copied.BlogPost, chatIsBlog, bloggingIsAllowed)
}

// We use pure functions for authorization, for sake simplicity and composability
func CanWriteMessage(isParticipant, chatIsAdmin, chatCanWriteMessage bool) bool {
	return isParticipant && (isChatAdminInternal(chatIsAdmin) || canWriteMessageInternal(chatCanWriteMessage))
}

func isChatAdminInternal(a bool) bool {
	return a
}

func canWriteMessageInternal(chatCanWriteMessage bool) bool {
	return chatCanWriteMessage
}

func CanEditMessage(behalfParticipantId int64, messageOwnerId int64, hasEmbed bool, embedTypeSafe string, canWriteMessage bool) bool {
	return ((messageOwnerId == behalfParticipantId) && (!hasEmbed || embedTypeSafe != dto.EmbedMessageTypeResend)) && canWriteMessage
}

func CanSyncEmbedMessage(behalfParticipantId int64, messageOwnerId int64, hasEmbed bool, canWriteMessage bool) bool {
	return messageOwnerId == behalfParticipantId && hasEmbed && canWriteMessage
}

func CanDeleteMessage(behalfParticipantId int64, messageOwnerId int64, canWriteMessage bool) bool {
	return messageOwnerId == behalfParticipantId && canWriteMessage
}

func CanPublishMessage(chatRegularParticipantCanPublishMessage, chatIsAdmin bool, messageOwnerId, behalfUserId int64) bool {
	return isChatAdminInternal(chatIsAdmin) || (canPublishMessageInternal(chatRegularParticipantCanPublishMessage) && messageOwnerId == behalfUserId)
}

func CanPinMessage(chatRegularParticipantCanPinMessage, chatIsAdmin bool) bool {
	return isChatAdminInternal(chatIsAdmin) || canPinMessageInternal(chatRegularParticipantCanPinMessage)
}

func canPublishMessageInternal(chatRegularParticipantCanPublishMessage bool) bool {
	return chatRegularParticipantCanPublishMessage
}

func canPinMessageInternal(chatRegularParticipantCanPinMessage bool) bool {
	return chatRegularParticipantCanPinMessage
}

func (m *CommonProjection) ChatHasMessages(ctx context.Context, co db.CommonOperations, chatId int64) (bool, error) {
	var has bool
	err := sqlscan.Get(ctx, co, &has, "select exists(select * from message m where chat_id = $1 limit 1)", chatId)
	if err != nil {
		return false, err
	}
	return has, nil
}

func (m *CommonProjection) GetMessageDataForAuthorization(ctx context.Context, co db.CommonOperations, userId, chatId, messageId int64) (dto.MessageAuthorizationData, error) {
	d := dto.MessageAuthorizationData{}
	// it's ok if message is not found - sql handles it
	err := sqlscan.Get(ctx, co, &d, `
		with
		provided as (
			select 
				 cast($2 as bigint) as chat_id
				,cast($3 as bigint) as message_id
		),
		chat_participant_row as (
			SELECT user_id, chat_id, chat_admin FROM chat_participant WHERE user_id = $1 AND chat_id = $2 LIMIT 1
		),
		chat_info as (
			select * from chat_common where id = $2
		),
		message_info as (
			select * from message m where chat_id = $2 and id = $3
		)
		SELECT
			 cc.id is not null as is_chat_found
			,mm.id is not null as is_message_found
			,exists(SELECT * FROM chat_participant_row) as is_chat_participant
			,exists(SELECT * FROM chat_participant_row WHERE chat_admin) as is_chat_admin
			,coalesce(cc.regular_participant_can_write_message, false) as chat_can_write_message
			,coalesce(cc.tet_a_tet, false) as chat_is_tet_a_tet
			,(mm.id is not null) and (mm.embed is not null) as message_has_embed
			,coalesce(mm.owner_id, $4) as message_owner_id
			,coalesce(mm.embed ->> 'embedMessageType', $5) as message_embed_type
			,coalesce(mm.blog_post, false) as is_message_blog_post
			,coalesce(cc.regular_participant_can_pin_message, false) as chat_can_pin_message
			,b.id is not null as chat_is_blog
		FROM provided pr
		LEFT JOIN chat_info cc on pr.chat_id = cc.id
		LEFT JOIN message_info mm ON pr.message_id = mm.id
		left join blog b on cc.id = b.id
	`, userId, chatId, messageId, dto.NoOwner, dto.EmbedMessageTypeNone)
	if err != nil {
		return d, err
	}
	return d, nil
}

//func (m *CommonProjection) //

func makeReactions(users map[int64]*dto.User, reactionsList []dto.ReactionDto) []dto.Reaction {
	var convertedReactionsOfMessageToReturn = make([]dto.Reaction, 0, len(reactionsList))
	for _, dbReaction := range reactionsList {

		reactionUsers := []*dto.User{}
		for _, u := range dbReaction.UserIds {
			ru := users[u]
			if ru == nil {
				ru = getDeletedUser(u)
			}
			reactionUsers = append(reactionUsers, ru)
		}

		convertedReactionsOfMessageToReturn = append(convertedReactionsOfMessageToReturn, dto.Reaction{
			Count:    dbReaction.Count,
			Users:    reactionUsers,
			Reaction: dbReaction.Reaction,
		})
	}

	return convertedReactionsOfMessageToReturn
}

func (m *CommonProjection) IsReactionExists(ctx context.Context, chatId, messageId int64, reaction string) (bool, error) {
	var exists bool
	err := sqlscan.Get(ctx, m.db, &exists, "select exists (select * from message_reaction where chat_id = $1 and message_id = $2 and reaction = $3)", chatId, messageId, reaction)
	if err != nil {
		return false, err
	}
	return exists, err
}

func getDeletedUser(id int64) *dto.User {
	return &dto.User{Login: fmt.Sprintf("deleted_user_%v", id), Id: id}
}

func makeEmbed(
	srcEmbed dto.Embeddable,
	users map[int64]*dto.User,
	chatsBehalfUserByChatId map[int64]*dto.BasicChatDtoExtended,
) (*dto.EmbedMessageResponse, error) {
	if srcEmbed != nil {
		switch typed := srcEmbed.(type) {
		case *dto.EmbedReply:
			embeddedUser := users[typed.OwnerId]
			return &dto.EmbedMessageResponse{
				Id:        typed.MessageId,
				Text:      typed.MessageContent,
				EmbedType: string(typed.GetType()),
				Owner:     embeddedUser,
			}, nil
		case *dto.EmbedResend:
			embeddedUser := users[typed.OwnerId]
			var embedChatName *string = nil
			var isParticipant bool

			basicEmbeddedChat := chatsBehalfUserByChatId[typed.ChatId]
			if basicEmbeddedChat != nil { // basicEmbeddedChat can be deleted
				if !basicEmbeddedChat.TetATet {
					embedChatName = &basicEmbeddedChat.Title
				}
				isParticipant = basicEmbeddedChat.BehalfUserIsParticipant
			}

			return &dto.EmbedMessageResponse{
				Id:            typed.MessageId,
				ChatId:        &typed.ChatId,
				ChatName:      embedChatName,
				Text:          typed.MessageContent,
				EmbedType:     string(typed.GetType()),
				Owner:         embeddedUser,
				IsParticipant: isParticipant,
			}, nil
		default:
			return nil, fmt.Errorf("Unknown type in setEmbed: %T", typed)
		}
	}

	return nil, nil
}

func (m *CommonProjection) GetMessages(ctx context.Context, co db.CommonOperations, chatId int64, size int32, startingFromItemId *int64, includeStartingFrom, reverse bool, searchString string, messageId *int64) ([]dto.MessageDto, error) {
	type messageDto struct {
		Id             int64        `db:"id"`
		OwnerId        int64        `db:"owner_id"`
		Content        string       `db:"content"`
		BlogPost       bool         `db:"blog_post"`
		Embed          pgtype.JSONB `db:"embed"`
		CreateDateTime time.Time    `db:"create_date_time"`
		UpdateDateTime *time.Time   `db:"update_date_time"`
		FileItemUuid   *string      `db:"file_item_uuid"`
		Pinned         bool         `db:"pinned"`
	}

	if startingFromItemId != nil && messageId != nil {
		return nil, fmt.Errorf("wrong invariant: both startingFromItemId and messageId provided")
	}

	mar := []dto.MessageDto{}
	ma := []messageDto{}

	queryArgs := []any{chatId, size}

	order := ""
	nonEquality := ""
	if reverse {
		order = "desc"
		if includeStartingFrom {
			nonEquality = "<="
		} else {
			nonEquality = "<"
		}
	} else {
		order = "asc"
		if includeStartingFrom {
			nonEquality = ">="
		} else {
			nonEquality = ">"
		}
	}

	conditionClause := ""

	paginationKeyset := ""
	if startingFromItemId != nil {
		queryArgs = append(queryArgs, *startingFromItemId)
		paginationKeyset = fmt.Sprintf(` and m.id %s $%d `, nonEquality, len(queryArgs))

		conditionClause = paginationKeyset
	}

	var searchClause string
	if len(searchString) > 0 {
		searchClause = " and ("

		searchStringPercents := "%" + searchString + "%"
		queryArgs = append(queryArgs, searchStringPercents)
		searchClause += fmt.Sprintf(" (strip_tags(coalesce(m.content, '')) || ' ' || strip_tags(coalesce(m.embed ->> 'embedMessageContent', ''))) ILIKE $%d ", len(queryArgs))

		searchClause += " or "

		queryArgs = append(queryArgs, searchString)
		searchClause += fmt.Sprintf(` 
		exists ( 
			select 1 from (select * from (select unnest(tsvector_to_array(m.fts_all_content))) t(av)) inq 
			where 
				   ( inq.av %% to_tsquery('russian', $%d)::text ) 
				or ( cyrillic_transliterate(inq.av) %% cyrillic_transliterate(to_tsquery('russian', $%d)::text) ) 
		) `, len(queryArgs), len(queryArgs))

		searchClause += " ) "
	}

	orderClause := fmt.Sprintf(" order by m.id %s ", order)

	if messageId != nil {
		messageIdV := *messageId
		queryArgs = append(queryArgs, messageIdV)
		messageIdClause := fmt.Sprintf(" and m.id = $%d ", len(queryArgs))

		conditionClause = messageIdClause
		orderClause = ""
	}

	err := sqlscan.Select(ctx, co, &ma, fmt.Sprintf(`
			select 
			    m.id,
			    m.owner_id,
			    m.content,
			    m.blog_post,
				m.embed,
				m.create_date_time,
			    m.update_date_time,
			    m.file_item_uuid,
				m.pinned
			from message m
			where m.chat_id = $1 %s 
			%s
			%s 
			limit $2
		`, conditionClause, searchClause, orderClause),
		queryArgs...)

	if err != nil {
		return mar, err
	}

	for i, mm := range ma {
		mc := dto.MessageDto{
			Id:             mm.Id,
			OwnerId:        mm.OwnerId,
			Content:        mm.Content,
			BlogPost:       mm.BlogPost,
			CreateDateTime: mm.CreateDateTime,
			UpdateDateTime: mm.UpdateDateTime,
			FileItemUuid:   mm.FileItemUuid,
			Pinned:         mm.Pinned,
		}

		embeddable, err := makeEmbedddable(mm.Embed)
		if err != nil {
			return mar, fmt.Errorf("error during mapping on index %d: %w", i, err)
		}
		mc.Embed = embeddable

		mar = append(mar, mc)
	}

	return mar, nil
}

func makeEmbedddable(embedJsonb pgtype.JSONB) (dto.Embeddable, error) {
	if embedJsonb.Status == pgtype.Present {
		var typer dto.EmbedTyper
		err := embedJsonb.AssignTo(&typer)
		if err != nil {
			return nil, fmt.Errorf("error during mapping %w", err)
		}

		switch typer.Type {
		case dto.EmbedMessageTypeReply:
			var erpl dto.EmbedReply
			err = embedJsonb.AssignTo(&erpl)
			if err != nil {
				return nil, fmt.Errorf("error during mapping: %w", err)
			}
			return &erpl, nil
		case dto.EmbedMessageTypeResend:
			var eres dto.EmbedResend
			err = embedJsonb.AssignTo(&eres)
			if err != nil {
				return nil, fmt.Errorf("error during mapping: %w", err)
			}
			return &eres, nil
		default:
			return nil, fmt.Errorf("Unknown type in GetMessages: %v", typer.Type)
		}
	}
	return nil, nil
}

func (m *CommonProjection) GetMessageBasic(ctx context.Context, co db.CommonOperations, chatId, messageId int64) (*dto.MessageBasic, error) {
	var msg dto.MessageBasic
	err := sqlscan.Get(ctx, co, &msg, `
	select m.id, m.owner_id, m.content, m.blog_post, m.published, m.file_item_uuid
	from message m where m.chat_id = $1 and m.id = $2
	`, chatId, messageId)
	if errors.Is(err, sql.ErrNoRows) {
		// there were no rows, but otherwise no error occurred
		return nil, nil
	} else if err != nil {
		return nil, err
	}
	return &msg, nil
}

func (m *CommonProjection) GetMessageEmbed(ctx context.Context, co db.CommonOperations, chatId, messageId int64) (dto.Embeddable, error) {
	var embed pgtype.JSONB
	err := sqlscan.Get(ctx, co, &embed, `
	select m.embed
	from message m where m.chat_id = $1 and m.id = $2
	`, chatId, messageId)
	if errors.Is(err, sql.ErrNoRows) {
		// there were no rows, but otherwise no error occurred
		return nil, nil
	} else if err != nil {
		return nil, err
	}

	embeddable, err := makeEmbedddable(embed)
	if err != nil {
		return nil, fmt.Errorf("error during mapping: %w", err)
	}

	return embeddable, nil
}

func (m *CommonProjection) GetReactionsOnMessage(ctx context.Context, co db.CommonOperations, chatId, messageId int64) ([]string, error) {
	res := []string{}

	err := sqlscan.Select(ctx, co, &res, `
		select distinct on (reaction) reaction from message_reaction where chat_id = $1 and message_id = $2
	`, chatId, messageId)
	if err != nil {
		return nil, err
	}
	return res, nil
}

func (m *CommonProjection) GetMessageWithEmbed(ctx context.Context, co db.CommonOperations, chatId, messageId int64) (*dto.MessageWithEmbed, error) {
	type messageDto struct {
		Id      int64        `db:"id"`
		OwnerId int64        `db:"owner_id"`
		Content string       `db:"content"`
		Embed   pgtype.JSONB `db:"embed"`
	}

	var msg messageDto
	err := sqlscan.Get(ctx, co, &msg, `
	select m.id, m.owner_id, m.content, m.embed
	from message m where m.chat_id = $1 and m.id = $2
	`, chatId, messageId)
	if errors.Is(err, sql.ErrNoRows) {
		// there were no rows, but otherwise no error occurred
		return nil, nil
	} else if err != nil {
		return nil, err
	}

	embeddable, err := makeEmbedddable(msg.Embed)
	if err != nil {
		return nil, fmt.Errorf("error during mapping: %w", err)
	}

	return &dto.MessageWithEmbed{
		Id:      msg.Id,
		OwnerId: msg.OwnerId,
		Content: msg.Content,
		Embed:   embeddable,
	}, nil
}

func (m *CommonProjection) IsMessageExists(ctx context.Context, co db.CommonOperations, chatId, messageId int64) (bool, error) {
	var exists bool
	err := sqlscan.Get(ctx, co, &exists, `
	select exists (select * from message m where m.chat_id = $1 and m.id = $2)
	`, chatId, messageId)
	if err != nil {
		return false, err
	}
	return exists, nil
}

func (m *CommonProjection) FindMessageByFileItemUuid(ctx context.Context, chatId, userId int64, fileItemUuid string) (*dto.MessageId, error) {
	participant, err := m.IsParticipant(ctx, m.db, userId, chatId)
	if err != nil {
		return nil, err
	}
	if !participant {
		return nil, NewUnauthorizedError(fmt.Sprintf("user %v is not a participant of chat %v", userId, chatId))
	}

	var messageId int64
	err = sqlscan.Get(ctx, m.db, &messageId, `
		select id from message where chat_id = $1 AND file_item_uuid = $2 or content ilike '%' || $2 || '%' order by id limit 1	
	`, chatId, fileItemUuid)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			// there were no rows, but otherwise no error occurred
			return &dto.MessageId{dto.FileItemUuidMessageNotFoundId}, nil
		}
		return nil, err
	}

	return &dto.MessageId{messageId}, nil
}

func (m *EnrichingProjection) MessageFilter(ctx context.Context, co db.CommonOperations, behalfUserId, chatId int64, searchString string, messageId int64) (bool, error) {
	participant, err := m.cp.IsParticipant(ctx, co, behalfUserId, chatId)
	if err != nil {
		return false, err
	}
	if !participant {
		return false, NewUnauthorizedError(fmt.Sprintf("user %v is not a participant of chat %v", behalfUserId, chatId))
	}

	searchString = sanitizer.TrimAmdSanitize(m.policy, searchString)

	searchStringWithPercents := "%" + searchString + "%"

	var found bool
	err = sqlscan.Get(ctx, co, &found, "SELECT EXISTS (SELECT * FROM message m WHERE m.chat_id = $1 AND m.id = $2 AND strip_tags(m.content) ILIKE $3)", chatId, messageId, searchStringWithPercents)
	if err != nil {
		return false, err
	}

	return found, nil
}

func (m *EnrichingProjection) GetReadMessageUsers(ctx context.Context, userId int64, chatId int64, messageId int64, size int32, offset int64) (*dto.MessageReadResponse, error) {
	type result struct {
		userIds []int64
		count   int64
		msg     *dto.MessageBasic
	}

	txRes, errOuter := db.TransactWithResult(ctx, m.cp.db, func(tx *db.Tx) (*result, error) {
		if participant, err := m.cp.IsParticipant(ctx, tx, userId, chatId); err != nil {
			m.lgr.ErrorContext(ctx, "Error during checking participant")
			return nil, err
		} else if !participant {
			return nil, NewUnauthorizedError(fmt.Sprintf("User %v is not participant of chat %v, skipping", userId, chatId))
		}

		userIds, err := m.getParticipantsRead(ctx, tx, chatId, messageId, size, offset)
		if err != nil {
			return nil, err
		}

		count, err := m.getParticipantsReadCount(ctx, tx, chatId, messageId)
		if err != nil {
			return nil, err
		}

		msg, err := m.cp.GetMessageBasic(ctx, tx, chatId, messageId)
		if err != nil {
			return nil, err
		}

		return &result{
			userIds: userIds,
			count:   count,
			msg:     msg,
		}, nil
	})
	if errOuter != nil {
		return nil, errOuter
	}

	usersToGet := map[int64]bool{}
	for _, u := range txRes.userIds {
		usersToGet[u] = true
	}
	if txRes.msg != nil {
		usersToGet[txRes.msg.OwnerId] = true
	}

	users, err := m.aaaRestClient.GetUsers(ctx, utils.SetMapIdBoolToSlice(usersToGet))
	if err != nil {
		return nil, err
	}
	userMap := utils.ToMap(users)

	usersToReturn := []*dto.User{}
	var anOwnerLogin string

	for _, usId := range txRes.userIds {
		us, ok := userMap[usId]
		if ok {
			usersToReturn = append(usersToReturn, us)
			if txRes.msg != nil && us.Id == txRes.msg.OwnerId {
				anOwnerLogin = us.Login
			}
		}
	}

	var text string
	if txRes.msg != nil {
		text = txRes.msg.Content
	}
	previewTxt := preview.CreateMessagePreview(m.stripAllTags, m.cfg.Message.PreviewMaxTextSize, text, anOwnerLogin)

	return &dto.MessageReadResponse{
		ParticipantsWrapper: dto.ParticipantsWrapper{
			Data:  usersToReturn,
			Count: txRes.count,
		},
		Text: previewTxt,
	}, nil
}

func (m *EnrichingProjection) getParticipantsReadCount(ctx context.Context, co db.CommonOperations, chatId, messageId int64) (int64, error) {
	var count int64

	err := sqlscan.Get(ctx, co, &count, `
		select 
		    count(user_id) 
		from chat_participant 
		where chat_id = $1 and cp_last_read_message_id >= $2`,
		chatId, messageId)

	return count, err
}

func (m *EnrichingProjection) getParticipantsRead(ctx context.Context, co db.CommonOperations, chatId, messageId int64, limit int32, offset int64) ([]int64, error) {
	list := make([]int64, 0)

	err := sqlscan.Select(ctx, co, &list, `
		select 
			user_id 
		from chat_participant 
		where chat_id = $1 and cp_last_read_message_id >= $2
		ORDER BY cp_last_read_message_date_time desc
		LIMIT $3 OFFSET $4`,
		chatId, messageId, limit, offset)

	if err != nil {
		return nil, err
	}
	return list, nil
}

func (m *CommonProjection) AreHasUnreadMessagesExists(ctx context.Context, co db.CommonOperations, userId int64) (bool, error) {
	var t bool
	err := sqlscan.Get(ctx, co, &t, "select exists(select u.* from has_unread_messages u where u.user_id = $1)", userId)
	if err != nil {
		return false, err
	}
	return t, nil
}

// see also cqrs/event_handler.go
func (m *EnrichingProjection) parseMentionUserIdsFromMessageHtml(ctx context.Context, msg string) ([]int64, bool, bool) {
	ret := []int64{}

	var hasHere, hasAll bool

	doc, err := goquery.NewDocumentFromReader(strings.NewReader(msg))
	if err != nil {
		m.lgr.WarnContext(ctx, "Unable to read html", "err", err)
		return ret, false, false
	}

	doc.Find("a, span").Each(func(i int, s *goquery.Selection) { // span is for @all, @here
		maybeA := s.First()

		if maybeA != nil && maybeA.HasClass("mention") {
			idS, ok := maybeA.Attr("data-id")
			if !ok {
				m.lgr.WarnContext(ctx, "a with class mention has no data-id")
			} else {
				id, errP := utils.ParseInt64(idS)
				if errP != nil {
					m.lgr.WarnContext(ctx, fmt.Sprintf("unable to parse user id from data-id: '%s'", idS))
				} else {
					switch id {
					case dto.AllUsers:
						hasAll = true
					case dto.HereUsers:
						hasHere = true
					default:
						ret = append(ret, id)
					}
				}
			}
		}
	})

	return ret, hasHere, hasAll
}
