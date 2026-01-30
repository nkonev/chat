package cqrs

import (
	"context"
	"fmt"
	"go-cqrs-chat-example/db"
	"go-cqrs-chat-example/dto"
	"go-cqrs-chat-example/preview"
	"go-cqrs-chat-example/utils"
	"time"

	"github.com/georgysavva/scany/v2/sqlscan"
)

func (m *CommonProjection) insertLastReadParticipantChatPartitioned(ctx context.Context, co db.CommonOperations, participantIds []int64, chatId, threadId int64) error {
	_, err := co.ExecContext(ctx, `
	with input_data as (
		select unnest(cast ($1 as bigint[])) as t(user_id)
	)
	insert into last_read_message_chat_view(
		user_id
		,chat_id
		,thread_id
		,cp_last_read_message_id
		,cp_last_read_message_date_time
	)
	select
		idt.user_id
		,cast($2 as bigint)
		,cast($3 as bigint)
		,0
		,null
	from input_data idt
	on conflict (chat_id, thread_id, user_id) do nothing
	`, participantIds, chatId, threadId)
	return err
}

func (m *CommonProjection) deleteLastReadParticipantChatPartitioned(ctx context.Context, co db.CommonOperations, participantIds []int64, chatId, threadId int64) error {
	_, err := co.ExecContext(ctx, `
	with input_data as (
		select * from unnest(cast ($1 as bigint[])) as t(user_id)
	)
	delete from last_read_message_chat_view
	where (chat_id, thread_id, user_id) in (select cast($2 as bigint), cast($3 as bigint), idt.user_id from input_data idt)
	`, participantIds, chatId, threadId)
	return err
}

// MessageReadUsersModal.vue
func (m *EnrichingProjection) GetReadMessageUsers(ctx context.Context, userId int64, chatId, threadId int64, messageId int64, size int32, offset int64) (*dto.MessageReadResponse, error) {
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

		userIds, err := m.getParticipantsRead(ctx, tx, chatId, threadId, messageId, size, offset)
		if err != nil {
			return nil, err
		}

		count, err := m.getParticipantsReadCount(ctx, tx, chatId, threadId, messageId)
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

func (m *EnrichingProjection) getParticipantsReadCount(ctx context.Context, co db.CommonOperations, chatId, threadId, messageId int64) (int64, error) {
	var count int64

	err := sqlscan.Get(ctx, co, &count, `
		select 
		    count(user_id) 
		from last_read_message_chat_view 
		where chat_id = $1 and thread_id = $2 and cp_last_read_message_id >= $3`,
		chatId, threadId, messageId)

	return count, err
}

func (m *EnrichingProjection) getParticipantsRead(ctx context.Context, co db.CommonOperations, chatId, threadId, messageId int64, limit int32, offset int64) ([]int64, error) {
	list := make([]int64, 0)

	err := sqlscan.Select(ctx, co, &list, `
		select 
			user_id 
		from last_read_message_chat_view 
		where chat_id = $1 and thread_id = $2 and cp_last_read_message_id >= $3
		ORDER BY cp_last_read_message_date_time desc
		LIMIT $4 OFFSET $5`,
		chatId, threadId, messageId, limit, offset)

	if err != nil {
		return nil, err
	}
	return list, nil
}

func (m *CommonProjection) updateParticipantMessageReadId(ctx context.Context, co db.CommonOperations, userId, chatId, threadId, messageId int64, lastReadMessageDateTime time.Time) error {
	_, err := co.ExecContext(ctx, `
		with
		max_message as (
			select max(id) as max from message where chat_id = $2 and thread_id = $3
		),
		max_message_normalized as (
			select coalesce((select max from max_message), 0) as max
		),
		normalized_message as (
			select case 
				when cast($4 as bigint) <= (select max from max_message_normalized) then cast($4 as bigint)
				else (select max from max_message_normalized)
			end
			as id
		)
		UPDATE last_read_message_chat_view 
		SET 
			 cp_last_read_message_id = (select id from normalized_message)
			,cp_last_read_message_date_time = $5
		WHERE (user_id, chat_id, thread_id) = ($1, $2, $3);
	`, userId, chatId, threadId, messageId, lastReadMessageDateTime)
	return err
}

func (m *CommonProjection) fastForwardParticipantMessageReadId(ctx context.Context, co db.CommonOperations, userId, chatId, threadId int64, lastReadMessageDateTime time.Time) error {
	_, err := co.ExecContext(ctx, `
		with 
		curr_message as (
			select coalesce((select cp_last_read_message_id from last_read_message_chat_view where (user_id, chat_id, thread_id) = ($1, $2, $3)), 0) as curr
		),
		max_message as (
			select coalesce((select max(id) from message where chat_id = $2 and thread_id = $3), 0) as max
		)
		UPDATE chat_participant 
		SET 
		    cp_last_read_message_id = (select max from max_message)
			,cp_last_read_message_date_time = $4
		WHERE 
			(user_id, chat_id, thread_id) = ($1, $2, $3)
			and (select curr from curr_message) != (select max from max_message)
	`, userId, chatId, threadId, lastReadMessageDateTime)
	return err
}
