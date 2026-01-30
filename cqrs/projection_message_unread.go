package cqrs

import (
	"context"
	"fmt"
	"go-cqrs-chat-example/db"
	"go-cqrs-chat-example/dto"
	"time"

	"github.com/georgysavva/scany/v2/sqlscan"
)

type SetUnreadedMessagesAction int16

const (
	SetUnreadedMessagesActionUnspecified = iota
	SetUnreadedMessagesActionInitialize
	SetUnreadedMessagesActionCalculateUnreadsFromTheUsersLastSavedReadedMessage
	SetUnreadedMessagesActionCalculateUnreadsFromTheProvidedMessage
)

func (m *CommonProjection) setUnreadMessages(ctx context.Context, tx *db.Tx, participantId int64, chatId, threadId, messageId int64, setUnreadedMessagesAction SetUnreadedMessagesAction) error {
	queryArgs := []any{participantId, chatId}

	var inputOptionClause string

	switch setUnreadedMessagesAction {
	case SetUnreadedMessagesActionInitialize:
		inputOptionClause = `
		normalized_considerable_message as (
			select 
				n.user_id,
				0 as normalized_read_message_id
			from normalized_user n
		)
		`
	case SetUnreadedMessagesActionCalculateUnreadsFromTheProvidedMessage:
		queryArgs = append(queryArgs, messageId)
		// to calculate against just from the message
		inputOptionClause = `
		input_option_considerable_existing_message as (
			select coalesce(
				(select m.id from chat_messages m where m.id = $3),
				(select max from max_message),
				0
			) as normalized_read_message_id
		),
		normalized_considerable_message as (
			select 
				n.user_id,
				(select normalized_read_message_id from input_option_considerable_existing_message) as normalized_read_message_id
			from normalized_user n
		)
		`
	case SetUnreadedMessagesActionCalculateUnreadsFromTheUsersLastSavedReadedMessage:
		// to calculate from the last saved readed
		inputOptionClause = `
		input_option_considerable_last_saved_readed_message as (
			select 
				coalesce(ww.last_message_id, 0) as last_message_id,
				nu.user_id
			from (
				select
					w.cuv_last_read_message_id as last_message_id,
					w.user_id
				from chat_user_view w
				where w.id = $2 and w.user_id = $1
			) ww
			right join normalized_user nu on ww.user_id = nu.user_id
		),
		normalized_considerable_message as (
			select 
				n.user_id,
				(select l.last_message_id from input_option_considerable_last_saved_readed_message l where l.user_id = n.user_id) as normalized_read_message_id
			from normalized_user n
		)
		`
	default:
		return fmt.Errorf("Unknown action: %v", setUnreadedMessagesAction)
	}

	q := fmt.Sprintf(`
		with 
		chat_messages as (
			select m.id from message m where m.chat_id = $2
		),
		max_message as (
			select max(m.id) as max from chat_messages m
		),
		normalized_user as (
			select cast ($1 as bigint) as user_id
		),
		%s,
		input_data as (
			select
				ngm.user_id as user_id,
				cast ($2 as bigint) as chat_id,
				(
					SELECT count(m.id) FILTER(WHERE m.id > (select normalized_read_message_id from normalized_considerable_message n where n.user_id = ngm.user_id))
					FROM chat_messages m
				) as unread_messages,
				ngm.normalized_read_message_id as last_read_message_id
			from normalized_considerable_message ngm
		)
		merge into chat_user_view cuv
		using input_data idt
		on (idt.chat_id, idt.user_id) = (cuv.id, cuv.user_id)
		when matched then update set 
		   unread_messages = idt.unread_messages
		  ,cuv_last_read_message_id = idt.last_read_message_id
	`, inputOptionClause)

	_, err := tx.ExecContext(ctx, q, queryArgs...)
	if err != nil {
		return err
	}

	err = m.updateHasUnreads(ctx, tx, participantId)
	if err != nil {
		return err
	}

	return nil
}

// see also fastForwardChatParticipantMessageReadIdInAllChats()
func (m *CommonProjection) setNoUnreadsInAllChats(ctx context.Context, co db.CommonOperations, userId int64, size int) ([]dto.ChatUserViewBasic, error) {
	updatedChatsPortion := []dto.ChatUserViewBasic{}

	const noOffset = 0

	q := `
		with
		input_data as (
			select
				uv.id as chat_id
				,uv.user_id
				,coalesce(cc.last_message_id, 0) as last_message_id
			from chat_user_view uv
			join chat_common cc on uv.id = cc.id
			where uv.user_id = $1 
				-- optimization to not process all the chats and
				-- inn.unread_messages > 0 is required to always return pass pages to uv.id and, consequently, to return the full pages in returning
				and uv.unread_messages > 0 
			order by uv.id 
			limit $2 offset $3
		)
		update unread_messages_user_view cuv 
		set (unread_messages, cuv_last_read_message_id) = (
			select 0, idt.last_message_id 
			from input_data idt
			where (idt.chat_id, idt.user_id) = (cuv.id, cuv.user_id)
		)
		where (cuv.id, cuv.user_id) in (select idtt.chat_id, idtt.user_id from input_data idtt) -- to avoid null. merge with return isn't supported
		returning cuv.id, cuv.unread_messages, cuv.update_date_time
	`

	err := sqlscan.Select(ctx, co, &updatedChatsPortion, q, userId, size, noOffset)
	if err != nil {
		return nil, err
	}

	return updatedChatsPortion, nil
}

// see also setNoUnreadsInAllChats()
func (m *CommonProjection) fastForwardChatParticipantMessageReadIdInAllChats(ctx context.Context, co db.CommonOperations, userId int64, size int, offset int64, lastReadMessageDateTime time.Time) ([]int64, error) {
	// here with limit and offset
	resChatIds := []int64{}
	q := `
		with
		input_data as (
			select
				uv.chat_id
				,uv.user_id
				,coalesce(cc.last_message_id, 0) as last_message_id
			from chat_participant uv
			join chat_common cc on uv.chat_id = cc.id
			left join (
				select max(id) as max_message_id, chat_id from message group by chat_id
			) mm on mm.chat_id = uv.chat_id
			where uv.user_id = $1 
				-- optimization to not process all the chats, "max(id) as max_message_id" is a part of the optimization
				and (
					mm.max_message_id is null -- corner - all the messages were removed
					or coalesce(uv.cp_last_read_message_id, 0) < mm.max_message_id
				)
			order by uv.chat_id 
			limit $2 offset $3
		)
		update chat_participant cpa 
		set 
			cp_last_read_message_id = (
				select idt.last_message_id 
				from input_data idt
				where (idt.chat_id, idt.user_id) = (cpa.chat_id, cpa.user_id)
			)
			,cp_last_read_message_date_time = $4
		where (cpa.chat_id, cpa.user_id) in (select idtt.chat_id, idtt.user_id from input_data idtt)
		returning cpa.chat_id
	`

	err := sqlscan.Select(ctx, co, &resChatIds, q, userId, size, offset, lastReadMessageDateTime)
	if err != nil {
		return nil, err
	}

	return resChatIds, nil
}

func (m *CommonProjection) fastForwardLastRead(ctx context.Context, co db.CommonOperations, userId, chatId, threadId int64) error {
	_, err := co.ExecContext(ctx, `
		UPDATE unread_messages_user_view 
		SET unread_messages = 0, cuv_last_read_message_id = (select max(id) from message where chat_id = $2)
		WHERE (user_id, chat_id, thread_id) = ($1, $2, $3);
	`, userId, chatId, threadId)
	return err
}

// should be called after upserting into unread_messages_user_view otherwise it's going to reset has to false
func (m *CommonProjection) updateHasUnreads(ctx context.Context, tx *db.Tx, participantId int64) error {
	// TODO тут смотреть на тредИД и предсохранённые в unread_messages_user_view
	//  root threadId should incorporate child thread hases and counts
	_, err := tx.ExecContext(ctx, `
	with
	normalized_user as (
		select cast ($1 as bigint) as user_id
	),	
	users_hases as (
		select 
			uv.user_id, 
			(any_value(uv.unread_messages) filter (where uv.unread_messages > 0 and ch.consider_messages_as_unread)) != 0 as has 
		from unread_messages_user_view uv
		join chat_user_view ch on (uv.user_id, uv.chat_id) = (ch.user_id, ch.id)
		where uv.user_id = $1
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
	`, participantId)
	return err
}

func (m *CommonProjection) increaseUnreadsAndSetHasUnreads(ctx context.Context, co db.CommonOperations, participantId int64, chatId, threadId int64, increaseOn int) error {
	var cmar bool
	err := sqlscan.Get(ctx, co, &cmar, `
		UPDATE unread_messages_user_view 
		SET unread_messages = unread_messages + $4
		WHERE user_id = $1 and chat_id = $2 and thread_id = $3
		returning consider_messages_as_unread
	`, participantId, chatId, threadId, increaseOn)
	if err != nil {
		return err
	}

	if cmar {
		_, err = co.ExecContext(ctx, `
			update has_unread_messages set has = true where user_id = $1
		`, participantId)
		if err != nil {
			return err
		}
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

func (m *CommonProjection) GetLastMessageReaded(ctx context.Context, chatId, userId int64) (int64, bool, int64, error) {
	type lastMessageReaded struct {
		LastReadedMessageId int64 `db:"last_readed_message_id"`
		Has                 bool  `db:"has"`
		MaxMessageId        int64 `db:"max_message_id"`
	}

	res := lastMessageReaded{}
	// TODO thread id
	err := sqlscan.Get(ctx, m.db, &res, `
	with
	chat_messages as (
		select m.id from message m where m.chat_id = $2
	),
	user_last_read_message as (
		select cuv_last_read_message_id as last_read_message_id from unread_messages_user_view um 
		where (um.user_id, um.chat_id) = ($1, $2)
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
