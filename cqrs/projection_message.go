package cqrs

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"github.com/georgysavva/scany/v2/sqlscan"
	"go-cqrs-chat-example/db"
	"go-cqrs-chat-example/dto"
	"go-cqrs-chat-example/services"
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

		participant, err := m.IsParticipant(ctx, tx, event.AdditionalData.BehalfUserId, event.ChatId)
		if err != nil {
			return err
		}
		if !participant {
			m.lgr.InfoContext(ctx, "Skipping MessageCreated because participant isn't participant", "user_id", event.AdditionalData.BehalfUserId, "chat_id", event.ChatId)
			return nil
		}

		_, err = tx.ExecContext(ctx, `
		insert into message(id, chat_id, owner_id, content, embed_message_id, embed_chat_id, embed_owner_id, embed_message_type, create_date_time, update_date_time) 
			values ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
		on conflict(chat_id, id) do update set 
		    owner_id = excluded.owner_id
		    , content = excluded.content
			, embed_message_id = excluded.embed_message_id
		    , embed_chat_id = excluded.embed_chat_id
		    , embed_owner_id = excluded.embed_owner_id
		    , embed_message_type = excluded.embed_message_type
		    , update_date_time = excluded.update_date_time
	`, event.Id, event.ChatId, event.AdditionalData.BehalfUserId, event.Content, event.EmbedMessageId, event.EmbedMessageChatId, event.EmbedMessageOwnerId, event.EmbedMessageType, event.AdditionalData.CreatedAt, nil)
		if err != nil {
			return err
		}
		m.lgr.InfoContext(ctx,
			"Handling message added",
			"id", event.Id,
			"user_id", event.AdditionalData.BehalfUserId,
			"chat_id", event.ChatId,
		)
		return nil
	})

	return errOuter
}

func (m *CommonProjection) OnMessageEdited(ctx context.Context, event *MessageEdited) error {
	errOuter := db.Transact(ctx, m.db, func(tx *db.Tx) error {
		participant, err := m.IsParticipant(ctx, tx, event.AdditionalData.BehalfUserId, event.ChatId)
		if err != nil {
			return err
		}
		if !participant {
			m.lgr.InfoContext(ctx, "Skipping MessageEdited because participant isn't participant", "user_id", event.AdditionalData.BehalfUserId, "chat_id", event.ChatId)
			return nil
		}

		messageOwnerId, err := m.GetMessageOwner(ctx, event.ChatId, event.Id)
		if err != nil {
			return err
		}
		if messageOwnerId != event.AdditionalData.BehalfUserId {
			m.lgr.InfoContext(ctx, "Skipping MessageEdited because participant isn't owner", "user_id", event.AdditionalData.BehalfUserId, "chat_id", event.ChatId)
			return nil
		}

		messageBlogPost, err := m.isMessageBlogPost(ctx, tx, event.ChatId, event.Id)
		if err != nil {
			return err
		}

		_, err = tx.ExecContext(ctx, `
			update message
			set	
			    content = $3
				, embed_message_id = $4
				, embed_chat_id = $5
				, embed_owner_id = $6
				, embed_message_type = $7
				, update_date_time = $8
			where chat_id = $2 and id = $1 
		`, event.Id, event.ChatId, event.Content, event.EmbedMessageId, event.EmbedMessageChatId, event.EmbedMessageOwnerId, event.EmbedMessageType, event.AdditionalData.CreatedAt)
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
	err := m.setUnreadMessages(ctx, tx, participantIds, chatId, 0, true, false)
	if err != nil {
		return err
	}
	return nil
}

func (m *CommonProjection) OnMessageRemoved(ctx context.Context, event *MessageDeleted) error {
	errOuter := db.Transact(ctx, m.db, func(tx *db.Tx) error {
		participant, err := m.IsParticipant(ctx, tx, event.AdditionalData.BehalfUserId, event.ChatId)
		if err != nil {
			return err
		}
		if !participant {
			m.lgr.InfoContext(ctx, "Skipping MessageDeleted because participant isn't participant", "user_id", event.AdditionalData.BehalfUserId, "chat_id", event.ChatId)
			return nil
		}

		messageOwnerId, err := m.GetMessageOwner(ctx, event.ChatId, event.MessageId)
		if err != nil {
			return err
		}
		if messageOwnerId != event.AdditionalData.BehalfUserId {
			m.lgr.InfoContext(ctx, "Skipping MessageDeleted because participant isn't owner", "user_id", event.AdditionalData.BehalfUserId, "chat_id", event.ChatId)
			return nil
		}

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
						left(strip_tags(m.content), $3) as content
					from message m 
					where m.chat_id = $2 and m.id = (select max(mm.id) from message mm where mm.chat_id = $2)
				)
				UPDATE chat_user_view 
				SET 
					last_message_id = (select id from last_message),
					last_message_content = (select content from last_message),
					last_message_owner_id = (select owner_id from last_message)
				WHERE user_id = any($1) and id = $2;
			`, participantIds, chatId, m.chatUserViewConfig.LastMessageMaxTextPreviewSize)
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
						when exists(select * from chat_user_view uw where uw.id = $2 and uw.user_id = w.user_id and uw.last_read_message_id > 0)
						then coalesce(
							(select m.id as last_message_id from chat_messages m where m.id = w.last_read_message_id),
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
		  ,last_read_message_id = idt.last_read_message_id
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

func (m *CommonProjection) OnUnreadMessageReaded(ctx context.Context, event *MessageReaded, allChatsReadedConsumer func([]dto.ChatUserViewBasic)) error {
	if event.ReadMessagesAction == ReadMessagesActionOneMessage || event.ReadMessagesAction == ReadMessagesActionAllMessagesInOneChat {
		errOuter := db.Transact(ctx, m.db, func(tx *db.Tx) error {
			if event.ReadMessagesAction == ReadMessagesActionOneMessage {
				participant, err := m.IsParticipant(ctx, tx, event.AdditionalData.BehalfUserId, event.ChatId)
				if err != nil {
					return err
				}
				if !participant {
					m.lgr.InfoContext(ctx, "Skipping MessageReaded because participant isn't participant", "user_id", event.AdditionalData.BehalfUserId, "chat_id", event.ChatId)
					return nil
				}

				return m.setUnreadMessages(ctx, tx, []int64{event.AdditionalData.BehalfUserId}, event.ChatId, event.MessageId, false, false) // includes updateHasUnreads()
			} else if event.ReadMessagesAction == ReadMessagesActionAllMessagesInOneChat {
				participant, err := m.IsParticipant(ctx, tx, event.AdditionalData.BehalfUserId, event.ChatId)
				if err != nil {
					return err
				}
				if !participant {
					m.lgr.InfoContext(ctx, "Skipping MessageReaded because participant isn't participant", "user_id", event.AdditionalData.BehalfUserId, "chat_id", event.ChatId)
					return nil
				}

				_, err = tx.ExecContext(ctx, `
				update chat_user_view uv set unread_messages = 0, last_read_message_id = last_message_id where uv.user_id = $1 and uv.id = $2
			`, event.AdditionalData.BehalfUserId, event.ChatId)
				if err != nil {
					return err
				}

				return m.updateHasUnreads(ctx, tx, []int64{event.AdditionalData.BehalfUserId})
			} else {
				return fmt.Errorf("Unknown action: %T", event.ReadMessagesAction)
			}
		})
		if errOuter != nil {
			return fmt.Errorf("error during read messages: %w", errOuter)
		}
	} else if event.ReadMessagesAction == ReadMessagesActionAllChats {
		offset := 0
		for {
			updatedChatsPortion := []dto.ChatUserViewBasic{}
			err := sqlscan.Select(ctx, m.db, &updatedChatsPortion, `
				update chat_user_view uv 
				set unread_messages = 0, last_read_message_id = uv.last_message_id 
				where uv.user_id = $1 and uv.id in (
					select inn.id 
					from chat_user_view inn 
					where inn.user_id = $1 and inn.unread_messages > 0 -- inn.unread_messages > 0 is required to always return pass pages to uv.id and, consequently, to return the full pages in returning
					order by inn.id 
					limit $2 offset $3
				)
				returning uv.id, uv.unread_messages, uv.update_date_time
			`, event.AdditionalData.BehalfUserId, utils.DefaultSize, offset)
			if err != nil {
				return err
			}

			allChatsReadedConsumer(updatedChatsPortion)

			if len(updatedChatsPortion) < utils.DefaultSize {
				break
			}

			offset += utils.DefaultSize
		}
		_, err := m.db.ExecContext(ctx, "update has_unread_messages set has = false where user_id = $1", event.AdditionalData.BehalfUserId)
		if err != nil {
			return err
		}
	} else {
		return fmt.Errorf("Unknown action: %T", event.ReadMessagesAction)
	}
	return nil
}

func (m *CommonProjection) OnMessageReactionFlipped(ctx context.Context, event *MessageReactionFlipped) error {
	errOuter := db.Transact(ctx, m.db, func(tx *db.Tx) error {
		participant, err := m.IsParticipant(ctx, tx, event.AdditionalData.BehalfUserId, event.ChatId)
		if err != nil {
			return err
		}
		if !participant {
			m.lgr.InfoContext(ctx, "Skipping MessageReactionFlipped because participant isn't participant", "user_id", event.AdditionalData.BehalfUserId, "chat_id", event.ChatId)
			return nil
		}

		messageExists, errInner := m.checkMessageExists(ctx, tx, event.ChatId, event.MessageId)
		if errInner != nil {
			return errInner
		}

		if !messageExists {
			m.lgr.InfoContext(ctx, "Skipping MessageReactionFlipped because there is no message", "chat_id", event.ChatId, "message_id", event.MessageId)
			return nil
		}

		var exists bool
		errInner = sqlscan.Get(ctx, tx, &exists, "SELECT EXISTS(SELECT 1 FROM message_reaction WHERE chat_id = $1 AND message_id = $2 AND user_id = $3 AND reaction = $4)", event.ChatId, event.MessageId, event.AdditionalData.BehalfUserId, event.Reaction)
		if errInner != nil {
			return errInner
		}

		if !exists {
			_, errInner = tx.ExecContext(ctx, `
			insert into message_reaction(chat_id, message_id, user_id, reaction, create_date_time)
			values ($1, $2, $3, $4, $5)
			on conflict (chat_id, message_id, user_id, reaction) do nothing
		`, event.ChatId, event.MessageId, event.AdditionalData.BehalfUserId, event.Reaction, event.AdditionalData.CreatedAt)
			if errInner != nil {
				return errInner
			}
		} else {
			_, errInner = tx.ExecContext(ctx, "DELETE FROM message_reaction WHERE chat_id = $1 AND message_id = $2 AND user_id = $3 AND reaction = $4", event.ChatId, event.MessageId, event.AdditionalData.BehalfUserId, event.Reaction)
			if errInner != nil {
				return errInner
			}
		}
		return nil
	})
	return errOuter
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
	)
	select 
	    um.last_read_message_id as last_readed_message_id, 
	    exists(select * from chat_messages m where m.id = um.last_message_id) as has,
	    (select max(m.id) from chat_messages m) as max_message_id
	from chat_user_view um 
    where (um.user_id, um.id) = ($1, $2)
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

func (m *EnrichingProjection) GetMessagesEnriched(ctx context.Context, behalfUserIds []int64, needCheckAuth bool, authForUserId *int64, chatId int64, size int32, startingFromItemId *int64, includeStartingFrom, reverse bool, searchString string, messageId *int64) ([]dto.MessageViewEnrichedDto, error) {
	return db.TransactWithResult(ctx, m.cp.db, func(tx *db.Tx) ([]dto.MessageViewEnrichedDto, error) {
		if needCheckAuth {
			if authForUserId != nil {
				participant, err := m.cp.IsParticipant(ctx, m.cp.db, *authForUserId, chatId)
				if err != nil {
					return nil, err
				}
				if !participant {
					return nil, NewUnauthorizedError(fmt.Sprintf("user %v is not a participant of chat %v", *authForUserId, chatId))
				}
			} else {
				return nil, errors.New("Unknown invariant")
			}
		}

		searchString = services.TrimAmdSanitize(m.policy, searchString)

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
					msg.UserId = userId
					messagesTmp = append(messagesTmp, msg)
				}
				messages = messagesTmp
			}
		} else if len(behalfUserIds) == 1 {
			for i := range messages {
				messages[i].UserId = behalfUserIds[0]
			}
		} else {
			return nil, fmt.Errorf("Unknown invariant")
		}

		reactions, err := getReactions(ctx, tx, chatId, messages)
		if err != nil {
			return nil, fmt.Errorf("Got error during enriching messages with reactions: %v", err)
		}

		var usersSet = map[int64]bool{}
		var chatsPreSet = map[int64]bool{}
		for _, message := range messages {
			populateSets(&message, usersSet, chatsPreSet, chatId, m.messageConfig.MaxDisplayableReactionUsers, reactions)
		}
		chats, err := m.cp.GetChatsBasicExtended(ctx, tx, utils.MapSetToSlice(chatsPreSet), behalfUserIds)
		if err != nil {
			m.lgr.ErrorContext(ctx, "Error getting chat basic", "err", err)
			return nil, err
		}

		// it's ok because we have 1 chat in the both cases
		areAdmins, err := m.cp.getAreAdminsOfUserIds(ctx, tx, behalfUserIds, chatId)
		if err != nil {
			return nil, err
		}

		users, err := m.aaaRestClient.GetUsers(ctx, utils.MapSetToSlice(usersSet))
		if err != nil {
			m.lgr.WarnContext(ctx, "unable to get users")
		}

		messagesEnriched := make([]dto.MessageViewEnrichedDto, 0, len(messages))
		for _, mm := range messages {
			me := enrichMessage(mm, chatId, utils.ToMap(users), chats, reactions, mm.UserId, areAdmins)
			messagesEnriched = append(messagesEnriched, me)
		}
		return messagesEnriched, nil
	})
}

func getReactions(ctx context.Context, co db.CommonOperations, chatId int64, list []dto.MessageDto) (map[int64][]dto.ReactionDto, error) {
	messageIds := make([]int64, 0)
	for _, message := range list {
		messageIds = append(messageIds, message.Id)
	}

	reactions := []dto.ReactionDto{}

	ret := map[int64][]dto.ReactionDto{} // messageId:reactionList

	err := sqlscan.Select(ctx, co, &reactions, `
		SELECT user_id, message_id, reaction 
		FROM message_reaction 
		WHERE chat_id = $2 AND message_id = ANY ($1)
		order by create_date_time desc
		`, messageIds, chatId)
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

func populateSets(message *dto.MessageDto, usersSet map[int64]bool, chatsPreSet map[int64]bool, currentChatId int64, maxDisplayableReactionUsers int, reactions map[int64][]dto.ReactionDto) {
	usersSet[message.OwnerId] = true

	chatsPreSet[currentChatId] = true

	if message.ResponseEmbeddedMessageReplyOwnerId != nil {
		var embeddedMessageReplyOwnerId = *message.ResponseEmbeddedMessageReplyOwnerId
		usersSet[embeddedMessageReplyOwnerId] = true
	} else if message.ResponseEmbeddedMessageResendOwnerId != nil {
		var embeddedMessageResendOwnerId = *message.ResponseEmbeddedMessageResendOwnerId
		usersSet[embeddedMessageResendOwnerId] = true
		var embeddedMessageResendChatId = *message.ResponseEmbeddedMessageResendChatId
		chatsPreSet[embeddedMessageResendChatId] = true
	}

	takeOnAccountReactions(message.Id, usersSet, maxDisplayableReactionUsers, reactions)
}

func takeOnAccountReactions(messageId int64, ownersSet map[int64]bool, maxDisplayableReactionUsers int, messageReactions map[int64][]dto.ReactionDto) {
	var currDisplayableUsers = 0

	rl, ok := messageReactions[messageId]
	if ok {
		for _, r := range rl {
			if !ownersSet[r.UserId] {
				ownersSet[r.UserId] = true
				currDisplayableUsers++
			}

			if currDisplayableUsers >= maxDisplayableReactionUsers {
				break
			}
		}
	}
}

func enrichMessage(m dto.MessageDto, chatId int64, users map[int64]*dto.User, chats map[int64]*dto.BasicChatDtoExtended, reactions map[int64][]dto.ReactionDto, behalfUserId int64, areAdmins map[int64]bool) dto.MessageViewEnrichedDto {
	me := dto.MessageViewEnrichedDto{
		Id:             m.Id,
		ChatId:         chatId,
		OwnerId:        m.OwnerId,
		Content:        m.Content,
		BlogPost:       m.BlogPost,
		UpdateDateTime: m.UpdateDateTime,
		CreateDateTime: m.CreateDateTime,
		Owner:          users[m.OwnerId],
		UserId:         behalfUserId,
	}
	setEmbed(m, &me, users, chats)

	rl := reactions[m.Id]
	setReactions(&me, users, rl)

	var chatv dto.BasicChatDtoExtended
	chat := chats[chatId]
	if chat != nil {
		chatv = *chat
	}
	SetMessagePersonalizedFields(&me, chatv.RegularParticipantCanPublishMessage, chatv.RegularParticipantCanPinMessage, chatv.RegularCanWriteMessage, areAdmins[behalfUserId], behalfUserId)
	return me
}

func SetMessagePersonalizedFields(copied *dto.MessageViewEnrichedDto, chatRegularParticipantCanPublishMessage, chatRegularParticipantCanPinMessage, chatCanWriteMessage, chatIsAdmin bool, participantId int64) {
	canWriteMessage := chatIsAdmin || chatCanWriteMessage

	copied.CanEdit = ((copied.OwnerId == participantId) && (copied.EmbedMessage == nil || copied.EmbedMessage.EmbedType != dto.EmbedMessageTypeResend)) && canWriteMessage
	copied.CanDelete = copied.OwnerId == participantId && canWriteMessage
	copied.CanPublish = CanPublishMessage(chatRegularParticipantCanPublishMessage, chatIsAdmin, copied.OwnerId, participantId)
	copied.CanPin = CanPinMessage(chatRegularParticipantCanPinMessage, chatIsAdmin)
}

func CanPublishMessage(chatRegularParticipantCanPublishMessage, chatIsAdmin bool, messageOwnerId, behalfUserId int64) bool {
	return chatIsAdmin || (chatRegularParticipantCanPublishMessage && messageOwnerId == behalfUserId)
}

func CanPinMessage(chatRegularParticipantCanPinMessage, chatIsAdmin bool) bool {
	return chatIsAdmin || chatRegularParticipantCanPinMessage
}

func setReactions(dst *dto.MessageViewEnrichedDto, users map[int64]*dto.User, reactionsList []dto.ReactionDto) {
	var convertedReactionsOfMessageToReturn = make([]dto.ReactionViewDto, 0)

	for _, dbReaction := range reactionsList {
		user := users[dbReaction.UserId]
		wasSummed := false
		for j, existingReaction := range convertedReactionsOfMessageToReturn {
			if dbReaction.Reaction == existingReaction.Reaction {
				convertedReactionsOfMessageToReturn[j].Count = existingReaction.Count + 1

				usersOfThisReaction := convertedReactionsOfMessageToReturn[j].Users
				if user != nil {
					usersOfThisReaction = append(usersOfThisReaction, user)
				} else {
					usersOfThisReaction = append(usersOfThisReaction, getDeletedUser(dbReaction.UserId))
				}

				convertedReactionsOfMessageToReturn[j].Users = usersOfThisReaction

				wasSummed = true
			}
		}
		if !wasSummed {
			usersOfThisReaction := []*dto.User{}
			if user != nil {
				usersOfThisReaction = append(usersOfThisReaction, user)
			} else {
				usersOfThisReaction = append(usersOfThisReaction, getDeletedUser(dbReaction.UserId))
			}

			convertedReactionsOfMessageToReturn = append(convertedReactionsOfMessageToReturn, dto.ReactionViewDto{
				Count:    1,
				Reaction: dbReaction.Reaction,
				Users:    usersOfThisReaction,
			})
		}
	}

	dst.Reactions = convertedReactionsOfMessageToReturn
}

func getDeletedUser(id int64) *dto.User {
	return &dto.User{Login: fmt.Sprintf("deleted_user_%v", id), Id: id}
}

func setEmbed(srcDbMessage dto.MessageDto, dstRet *dto.MessageViewEnrichedDto, users map[int64]*dto.User, chats map[int64]*dto.BasicChatDtoExtended) {
	if srcDbMessage.ResponseEmbeddedMessageType != nil {
		if *srcDbMessage.ResponseEmbeddedMessageType == dto.EmbedMessageTypeReply {
			embeddedUser := users[*srcDbMessage.ResponseEmbeddedMessageReplyOwnerId]
			dstRet.EmbedMessage = &dto.EmbedMessageResponse{
				Id:        *srcDbMessage.ResponseEmbeddedMessageReplyId,
				Text:      *srcDbMessage.ResponseEmbeddedMessageReplyText,
				EmbedType: *srcDbMessage.ResponseEmbeddedMessageType,
				Owner:     embeddedUser,
			}
		} else if *srcDbMessage.ResponseEmbeddedMessageType == dto.EmbedMessageTypeResend {
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
}

func (m *CommonProjection) GetMessages(ctx context.Context, co db.CommonOperations, chatId int64, size int32, startingFromItemId *int64, includeStartingFrom, reverse bool, searchString string, messageId *int64) ([]dto.MessageDto, error) {

	if startingFromItemId != nil && messageId != nil {
		return nil, fmt.Errorf("wrong invariant: both startingFromItemId and messageId provided")
	}

	ma := []dto.MessageDto{}

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
		searchStringPercents := "%" + searchString + "%"
		queryArgs = append(queryArgs, searchStringPercents)
		searchClause = fmt.Sprintf(" AND strip_tags(m.content) ILIKE $%d ", len(queryArgs))
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
			%s
			%s 
			limit $2
		`, dto.EmbedMessageTypeReply, conditionClause, searchClause, orderClause),
		queryArgs...)

	if err != nil {
		return ma, err
	}
	return ma, nil
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
