package cqrs

import (
	"context"
	"fmt"
	"go-cqrs-chat-example/db"
	"go-cqrs-chat-example/dto"
)

func (m *CommonProjection) OnMessageCreated(ctx context.Context, event *MessageCreated) error {
	errOuter := db.Transact(ctx, m.db, func(tx *db.Tx) error {
		chatExists, err := m.checkChatExists(ctx, tx, event.ChatId)
		if err != nil {
			return err
		}
		if !chatExists {
			m.lgr.WithTrace(ctx).Info("Skipping MessageCreated because there is no chat", "chat_id", event.ChatId)
			return nil
		}

		_, err = tx.ExecContext(ctx, `
		insert into message(id, chat_id, owner_id, content, create_date_time, update_date_time) 
			values ($1, $2, $3, $4, $5, $6)
		on conflict(chat_id, id) do update set owner_id = excluded.owner_id, content = excluded.content, create_date_time = excluded.create_date_time, update_date_time = excluded.update_date_time
	`, event.Id, event.ChatId, event.OwnerId, event.Content, event.AdditionalData.CreatedAt, nil)
		if err != nil {
			return err
		}
		m.lgr.WithTrace(ctx).Info(
			"Handling message added",
			"id", event.Id,
			"user_id", event.OwnerId,
			"chat_id", event.ChatId,
		)
		return nil
	})

	return errOuter
}

func (m *CommonProjection) OnMessageEdited(ctx context.Context, event *MessageEdited) error {
	errOuter := db.Transact(ctx, m.db, func(tx *db.Tx) error {
		messageExists, errInner := m.checkMessageExists(ctx, tx, event.ChatId, event.Id)
		if errInner != nil {
			return errInner
		}
		if !messageExists {
			m.lgr.WithTrace(ctx).Info("Skipping MessageEdited because there is no message", "chat_id", event.ChatId, "message_id", event.Id)
			return nil
		}

		messageBlogPost, err := m.isMessageBlogPost(ctx, tx, event.ChatId, event.Id)
		if err != nil {
			return err
		}

		_, err = tx.ExecContext(ctx, `
			update message
			set	content = $3, update_date_time = $4
			where chat_id = $2 and id = $1 
		`, event.Id, event.ChatId, event.Content, event.AdditionalData.CreatedAt)
		if err != nil {
			return err
		}

		if messageBlogPost {
			err = m.refreshBlog(ctx, tx, event.ChatId, event.AdditionalData.CreatedAt)
			if err != nil {
				return err
			}
		}

		m.lgr.WithTrace(ctx).Info(
			"Handling message edited",
			"id", event.Id,
			"chat_id", event.ChatId,
			"message_id", event.Id,
		)
		return nil
	})

	return errOuter
}

func (m *CommonProjection) initializeMessageUnreadMultipleParticipants(ctx context.Context, tx *db.Tx, participantIds []int64, chatId int64) error {
	return m.setUnreadMessages(ctx, tx, participantIds, chatId, 0, true, false)
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
			err = m.refreshBlog(ctx, tx, event.ChatId, event.AdditionalData.CreatedAt)
			if err != nil {
				return err
			}
		}

		return nil
	})
	if errOuter != nil {
		return errOuter
	}

	m.lgr.WithTrace(ctx).Info(
		"Message removed from common chat",
		"message_id", event.MessageId,
		"chat_id", event.ChatId,
	)

	return nil
}

func (m *CommonProjection) setLastMessage(ctx context.Context, tx *db.Tx, participantIds []int64, chatId int64) error {

	_, err := tx.ExecContext(ctx, `
				with last_message as (
					select 
						m.id,
						m.owner_id, 
						m.content 
					from message m 
					where m.chat_id = $2 and m.id = (select max(mm.id) from message mm where mm.chat_id = $2)
				)
				UPDATE chat_user_view 
				SET 
					last_message_id = (select id from last_message),
					last_message_content = (select content from last_message),
					last_message_owner_id = (select owner_id from last_message)
				WHERE user_id = any($1) and id = $2;
			`, participantIds, chatId)
	if err != nil {
		return fmt.Errorf("error during setting last message: %w", err)
	}
	return nil
}

func (m *CommonProjection) setUnreadMessages(ctx context.Context, co db.CommonOperations, participantIds []int64, chatId, messageId int64, needSet, needRefresh bool) error {
	_, err := co.ExecContext(ctx, `
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
						when exists(select * from unread_messages_user_view uw where uw.chat_id = $2 and uw.user_id = w.user_id and uw.last_message_id > 0)
						then coalesce(
							(select m.id as last_message_id from chat_messages m where m.id = w.last_message_id),
							(select max from max_message where $5 = true)
						)
					end) as last_message_id,
					w.user_id
				from unread_messages_user_view w 
				where w.chat_id = $2 and w.user_id = any($1)
			) ww
			right join normalized_user nu on ww.user_id = nu.user_id
		),
		existing_message as (
			select coalesce(
				(select m.id from chat_messages m where m.id = $3),
				(select max from max_message),
				0
			) as normalized_message_id
		),
		normalized_given_message as (
			select 
				n.user_id,
				(case 
					when $4 = true then (select l.last_message_id from last_message l where l.user_id = n.user_id)
					else (select normalized_message_id from existing_message) 
				end) as normalized_message_id
			from normalized_user n
		),
		input_data as (
			select
				ngm.user_id as user_id,
				cast ($2 as bigint) as chat_id,
				(
					SELECT count(m.id) FILTER(WHERE m.id > (select normalized_message_id from normalized_given_message n where n.user_id = ngm.user_id))
					FROM chat_messages m
				) as unread_messages,
				ngm.normalized_message_id as last_message_id
			from normalized_given_message ngm
		)
		insert into unread_messages_user_view(user_id, chat_id, unread_messages, last_message_id)
		select 
			idt.user_id,
			idt.chat_id,
			idt.unread_messages,
			idt.last_message_id
		from input_data idt
		on conflict (user_id, chat_id) do update set unread_messages = excluded.unread_messages, last_message_id = excluded.last_message_id
	`, participantIds, chatId, messageId, needSet, needRefresh)
	return err
}

func (m *CommonProjection) OnUnreadMessageReaded(ctx context.Context, event *MessageReaded) error {
	// actually it should be an update
	// but we give a chance to create a row unread_messages_user_view in case lack of it
	// so message read event has a self-healing effect
	err := m.setUnreadMessages(ctx, m.db, []int64{event.ParticipantId}, event.ChatId, event.MessageId, false, false)
	if err != nil {
		return fmt.Errorf("error during read messages: %w", err)
	}
	return nil
}

func (m *CommonProjection) checkMessageExists(ctx context.Context, co db.CommonOperations, chatId, messageId int64) (bool, error) {
	rm := co.QueryRowContext(ctx, "select exists (select * from message where chat_id = $1 and id = $2)", chatId, messageId)
	if rm.Err() != nil {
		return false, rm.Err()
	}
	var messageExists bool
	err := rm.Scan(&messageExists)
	if err != nil {
		return false, err
	}
	return messageExists, nil
}

func (m *CommonProjection) GetMessageOwner(ctx context.Context, chatId, messageId int64) (int64, error) {
	r := m.db.QueryRowContext(ctx, "select owner_id from message where (chat_id, id) = ($1, $2)", chatId, messageId)
	if r.Err() != nil {
		return 0, r.Err()
	}
	var ownerId int64
	err := r.Scan(&ownerId)
	if err != nil {
		return 0, err
	}
	return ownerId, nil
}

func (m *CommonProjection) GetLastMessageReaded(ctx context.Context, chatId, userId int64) (int64, bool, int64, error) {
	r := m.db.QueryRowContext(ctx, `
	with
	chat_messages as (
		select m.id from message m where m.chat_id = $2
	)
	select 
	    um.last_message_id, 
	    exists(select * from chat_messages m where m.id = um.last_message_id),
	    (select max(m.id) from chat_messages m)
	from unread_messages_user_view um 
    where (um.user_id, um.chat_id) = ($1, $2)
	`, userId, chatId)
	if r.Err() != nil {
		return 0, false, 0, r.Err()
	}
	var lastReadedMessageId int64
	var has bool
	var maxMessageId int64
	err := r.Scan(&lastReadedMessageId, &has, &maxMessageId)
	if err != nil {
		return 0, false, 0, err
	}
	return lastReadedMessageId, has, maxMessageId, nil
}

func (m *CommonProjection) GetLastMessageId(ctx context.Context, chatId int64) (int64, error) {
	r := m.db.QueryRowContext(ctx, `
	select coalesce(inn.max_id, 0) 
	from (select max(id) as max_id from message m where m.chat_id = $1) inn
	`, chatId)
	if r.Err() != nil {
		return 0, r.Err()
	}
	var maxMessageId int64
	err := r.Scan(&maxMessageId)
	if err != nil {
		return 0, err
	}
	return maxMessageId, nil
}

func (m *CommonProjection) GetMessages(ctx context.Context, chatId int64, size int32, startingFromItemId *int64, includeStartingFrom, reverse bool) ([]dto.MessageViewDto, error) {
	ma := []dto.MessageViewDto{}

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

	paginationKeyset := ""
	if startingFromItemId != nil {
		paginationKeyset = fmt.Sprintf(` and m.id %s $3`, nonEquality)
		queryArgs = append(queryArgs, *startingFromItemId)
	}

	rows, err := m.db.QueryContext(ctx, fmt.Sprintf(`
			select m.id, m.owner_id, m.content, m.blog_post, m.create_date_time, m.update_date_time
			from message m
			where chat_id = $1 %s
			order by m.id %s 
			limit $2
		`, paginationKeyset, order),
		queryArgs...)
	if err != nil {
		return ma, err
	}
	defer rows.Close()
	for rows.Next() {
		var cd dto.MessageViewDto
		err = rows.Scan(&cd.Id, &cd.OwnerId, &cd.Content, &cd.BlogPost, &cd.CreateDateTime, &cd.UpdateDateTime)
		if err != nil {
			return ma, err
		}
		ma = append(ma, cd)
	}
	return ma, nil
}
