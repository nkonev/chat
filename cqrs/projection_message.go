package cqrs

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"go-cqrs-chat-example/db"
	"go-cqrs-chat-example/dto"
	"go-cqrs-chat-example/utils"
)

func (m *CommonProjection) OnMessageCreated(ctx context.Context, event *MessageCreated) error {
	errOuter := db.Transact(ctx, m.db, func(tx *db.Tx) error {
		chatExists, err := m.checkChatExists(ctx, tx, event.ChatId)
		if err != nil {
			return err
		}
		if !chatExists {
			m.lgr.InfoContext(ctx, "Skipping MessageCreated because there is no chat", "chat_id", event.ChatId)
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
		m.lgr.InfoContext(ctx,
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
			m.lgr.InfoContext(ctx, "Skipping MessageEdited because there is no message", "chat_id", event.ChatId, "message_id", event.Id)
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

		m.lgr.InfoContext(ctx,
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

	m.lgr.InfoContext(ctx,
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

func (m *CommonProjection) GetMessagesEnriched(ctx context.Context, behalfUserId, chatId int64, size int32, startingFromItemId *int64, includeStartingFrom, reverse bool) ([]dto.MessageViewEnrichedDto, error) {
	return db.TransactWithResult(ctx, m.db, func(tx *db.Tx) ([]dto.MessageViewEnrichedDto, error) {
		messages, err := m.GetMessages(ctx, tx, chatId, size, startingFromItemId, includeStartingFrom, reverse)
		if err != nil {
			m.lgr.ErrorContext(ctx, "Error getting messages", "err", err)
			return nil, err
		}

		var usersSet = map[int64]bool{}
		var chatsPreSet = map[int64]bool{}
		for _, message := range messages {
			populateSets(&message, usersSet, chatsPreSet, chatId)
		}
		chats, err := m.GetChatsBasicExtended(ctx, tx, utils.MapSetToSlice(chatsPreSet), behalfUserId)
		if err != nil {
			m.lgr.ErrorContext(ctx, "Error getting chat basic", "err", err)
			return nil, err
		}

		users, err := m.aaaRestClient.GetUsers(ctx, utils.MapSetToSlice(usersSet))
		if err != nil {
			m.lgr.WarnContext(ctx, "unable to get users")
		}

		messagesEnriched := enrichMessages(messages, utils.ToMap(users), chats)
		return messagesEnriched, nil
	})

}

func populateSets(message *dto.MessageViewDto, ownersSet map[int64]bool, chatsPreSet map[int64]bool, chatId int64) {
	ownersSet[message.OwnerId] = true
	chatsPreSet[chatId] = true
	if message.ResponseEmbeddedMessageReplyOwnerId != nil {
		var embeddedMessageReplyOwnerId = *message.ResponseEmbeddedMessageReplyOwnerId
		ownersSet[embeddedMessageReplyOwnerId] = true
	} else if message.ResponseEmbeddedMessageResendOwnerId != nil {
		var embeddedMessageResendOwnerId = *message.ResponseEmbeddedMessageResendOwnerId
		ownersSet[embeddedMessageResendOwnerId] = true
		var embeddedMessageResendChatId = *message.ResponseEmbeddedMessageResendChatId
		chatsPreSet[embeddedMessageResendChatId] = true
	}

	// TODO take into account reactions
}

func enrichMessages(messages []dto.MessageViewDto, users map[int64]*dto.User, chats map[int64]*dto.BasicChatDtoExtended) []dto.MessageViewEnrichedDto {
	res := make([]dto.MessageViewEnrichedDto, 0, len(messages))
	for _, m := range messages {
		me := dto.MessageViewEnrichedDto{
			Id:             m.Id,
			OwnerId:        m.OwnerId,
			Content:        m.Content,
			BlogPost:       m.BlogPost,
			UpdateDateTime: m.UpdateDateTime,
			CreateDateTime: m.CreateDateTime,
			Owner:          users[m.OwnerId],
		}
		setEmbed(m, me, users, chats)

		res = append(res, me)
	}
	return res
}

func setEmbed(srcDbMessage dto.MessageViewDto, dstRet dto.MessageViewEnrichedDto, users map[int64]*dto.User, chats map[int64]*dto.BasicChatDtoExtended) {
	if srcDbMessage.ResponseEmbeddedMessageReplyOwnerId != nil {
		embeddedUser := users[*srcDbMessage.ResponseEmbeddedMessageReplyOwnerId]
		dstRet.EmbedMessage = &dto.EmbedMessageResponse{
			Id:        *srcDbMessage.ResponseEmbeddedMessageReplyId,
			Text:      *srcDbMessage.ResponseEmbeddedMessageReplyText,
			EmbedType: *srcDbMessage.ResponseEmbeddedMessageType,
			Owner:     embeddedUser,
		}
	} else if srcDbMessage.ResponseEmbeddedMessageResendOwnerId != nil {
		embeddedUser := users[*srcDbMessage.ResponseEmbeddedMessageResendOwnerId]
		basicEmbeddedChat := chats[*srcDbMessage.ResponseEmbeddedMessageResendChatId]
		var embedChatName *string = nil
		var isParticipant bool
		if basicEmbeddedChat != nil { // basicEmbeddedChat can be deleted
			if !basicEmbeddedChat.TetATet {
				embedChatName = &basicEmbeddedChat.Title
			}
			isParticipant = basicEmbeddedChat.BehalfUserIsParticipant
		}

		dstRet.EmbedMessage = &dto.EmbedMessageResponse{
			Id:            *srcDbMessage.ResponseEmbeddedMessageResendId,
			ChatId:        srcDbMessage.ResponseEmbeddedMessageResendChatId,
			ChatName:      embedChatName,
			Text:          srcDbMessage.Content,
			EmbedType:     *srcDbMessage.ResponseEmbeddedMessageType,
			Owner:         embeddedUser,
			IsParticipant: isParticipant,
		}
		dstRet.Content = ""
	}
}

func (m *CommonProjection) GetMessages(ctx context.Context, co db.CommonOperations, chatId int64, size int32, startingFromItemId *int64, includeStartingFrom, reverse bool) ([]dto.MessageViewDto, error) {
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

	rows, err := co.QueryContext(ctx, fmt.Sprintf(`
			select 
			    m.id,
			    m.owner_id,
			    m.content,
			    m.blog_post,
				m.embed_message_type as embed_message_type,
				me.id as embed_message_reply_id,
				me.content as embed_message_reply_text,
				me.owner_id as embed_message_reply_owner_id,
				m.embed_message_id as embed_message_resend_id,
				m.embed_chat_id as embed_message_resend_chat_id,
				m.embed_owner_id as embed_message_resend_owner_id,
			    m.create_date_time,
			    m.update_date_time
			from message m
			left join message me 
			on (m.chat_id = me.chat_id and m.embed_message_id = me.id and m.embed_message_type = '%v')
			where m.chat_id = $1 %s
			order by m.id %s 
			limit $2
		`, dto.EmbedMessageTypeReply, paginationKeyset, order),
		queryArgs...)
	if err != nil {
		return ma, err
	}
	defer rows.Close()
	for rows.Next() {
		var cd dto.MessageViewDto
		err = rows.Scan(
			&cd.Id,
			&cd.OwnerId,
			&cd.Content,
			&cd.BlogPost,
			&cd.ResponseEmbeddedMessageType,
			&cd.ResponseEmbeddedMessageReplyId,
			&cd.ResponseEmbeddedMessageReplyText,
			&cd.ResponseEmbeddedMessageReplyOwnerId,
			&cd.ResponseEmbeddedMessageResendId,
			&cd.ResponseEmbeddedMessageResendChatId,
			&cd.ResponseEmbeddedMessageResendOwnerId,
			&cd.CreateDateTime,
			&cd.UpdateDateTime,
		)
		if err != nil {
			return ma, err
		}
		ma = append(ma, cd)
	}
	return ma, nil
}

func (m *CommonProjection) GetMessageBasic(ctx context.Context, co db.CommonOperations, chatId, messageId int64) (*dto.MessageBasic, error) {
	r := co.QueryRowContext(ctx, `
	select m.id, m.owner_id, m.content
	from message m where m.chat_id = $1 and m.id = $2
	`, chatId, messageId)
	if r.Err() != nil {
		return nil, r.Err()
	}
	var msg dto.MessageBasic
	err := r.Scan(&msg.Id, &msg.OwnerId, &msg.Content)
	if errors.Is(err, sql.ErrNoRows) {
		// there were no rows, but otherwise no error occurred
		return nil, nil
	} else if err != nil {
		return nil, err
	}
	return &msg, nil
}
