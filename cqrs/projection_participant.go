package cqrs

import (
	"context"
	"go-cqrs-chat-example/db"
	"go-cqrs-chat-example/utils"
)

func (m *CommonProjection) OnParticipantAdded(ctx context.Context, event *ParticipantsAdded) error {
	errOuter := db.Transact(ctx, m.db, func(tx *db.Tx) error {
		chatExists, err := m.checkChatExists(ctx, tx, event.ChatId)
		if err != nil {
			return err
		}
		if !chatExists {
			m.lgr.InfoContext(ctx, "Skipping ParticipantsAdded because there is no chat", "chat_id", event.ChatId)
			return nil
		}

		admin, err := m.IsChatAdmin(ctx, tx, event.BehalfUserId, event.ChatId)
		if err != nil {
			return err
		}
		if !admin && !event.SkipChatAdminCheck {
			m.lgr.InfoContext(ctx,
				"Participant isn't admin so he cannot add a participant",
				"user_id", event.BehalfUserId,
				"chat_id", event.ChatId,
			)
			return nil
		}

		_, err = tx.ExecContext(ctx, `
		with input_data as (
			select * from unnest(cast ($1 as bigint[]), cast ($2 as boolean[])) as t(user_id, chat_admin)
		)
		insert into chat_participant(user_id, chat_admin, chat_id, create_date_time)
		select idt.user_id, idt.chat_admin, $3, $4 from input_data idt
		on conflict(user_id, chat_id) do nothing
	`, GetParticipantIds(event.Participants), getParticipantChatAdmins(event.Participants), event.ChatId, event.AdditionalData.CreatedAt)
		if err != nil {
			return err
		}

		// no problems here because
		// a) we've already added participants in the previous step
		// b) there is no batching-with-pagination among addable participants
		//      which would cause gaps in participants_count for the participants of current and previous iterations

		// because we select chat_common, inserted from this consumer group in ChatCreated handler
		_, err = tx.ExecContext(ctx, `
		with 
		this_chat_participants as (
			select user_id, create_date_time from chat_participant where chat_id = $2
		),
		chat_participant_count as (
			select count (*) as count from this_chat_participants
		),
		chat_participants_last_n as (
			select user_id from this_chat_participants order by create_date_time desc limit $4
		),
		user_input as (
			select unnest(cast ($1 as bigint[])) as user_id
		),
		input_data as (
			select 
				c.id as chat_id, 
				c.title as title, 
				false as pinned, 
				u.user_id as user_id, 
				cast ($3 as timestamp) as update_date_time,
				(select count from chat_participant_count) as participants_count, 
				(select array_agg(user_id) from chat_participants_last_n) as participant_ids
			from user_input u
			cross join (select cc.id, cc.title from chat_common cc where cc.id = $2) c 
		)
		insert into chat_user_view(id, title, pinned, user_id, update_date_time, participants_count, participant_ids) 
			select chat_id, title, pinned, user_id, update_date_time, participants_count, participant_ids from input_data
		on conflict(user_id, id) do update set
			pinned = excluded.pinned, 
			title = excluded.title, 
			update_date_time = excluded.update_date_time, 
			participants_count = excluded.participants_count, 
			participant_ids = excluded.participant_ids
		`, GetParticipantIds(event.Participants), event.ChatId, event.AdditionalData.CreatedAt, m.chatUserViewConfig.MaxViewableParticipants)
		if err != nil {
			return err
		}

		// recalc in case an user was added after
		err = m.initializeMessageUnreadMultipleParticipants(ctx, tx, GetParticipantIds(event.Participants), event.ChatId)
		if err != nil {
			return err
		}

		err = m.setLastMessage(ctx, tx, GetParticipantIds(event.Participants), event.ChatId)
		if err != nil {
			return err
		}
		return nil
	})
	if errOuter != nil {
		return errOuter
	}

	m.lgr.InfoContext(ctx,
		"Participant added into common chat",
		"user_ids", GetParticipantIds(event.Participants),
		"chat_id", event.ChatId,
	)

	return nil
}

func (m *CommonProjection) OnParticipantRemoved(ctx context.Context, event *ParticipantDeleted) error {
	errOuter := db.Transact(ctx, m.db, func(tx *db.Tx) error {
		admin, err := m.IsChatAdmin(ctx, tx, event.BehalfUserId, event.ChatId)
		if err != nil {
			return err
		}
		if !admin {
			m.lgr.InfoContext(ctx,
				"Participant isn't admin so he cannot remove a participant",
				"user_id", event.BehalfUserId,
				"chat_id", event.ChatId,
			)
			return nil
		}

		_, err = tx.ExecContext(ctx, `
		delete from chat_participant where chat_id = $2 and user_id = any($1)
	`, event.ParticipantIds, event.ChatId)
		if err != nil {
			return err
		}

		_, err = tx.ExecContext(ctx, `
		delete from chat_user_view where user_id = any($1) and id = $2
	`, event.ParticipantIds, event.ChatId)
		if err != nil {
			return err
		}

		return nil
	})
	if errOuter != nil {
		return errOuter
	}

	m.lgr.InfoContext(ctx,
		"Participant removed from common chat",
		"user_ids", event.ParticipantIds,
		"chat_id", event.ChatId,
	)

	return nil
}

func (m *CommonProjection) OnParticipantChanged(ctx context.Context, event *ParticipantChanged) error {
	return db.Transact(ctx, m.db, func(tx *db.Tx) error {
		admin, err := m.IsChatAdmin(ctx, tx, event.BehalfUserId, event.ChatId)
		if err != nil {
			return err
		}
		if !admin {
			m.lgr.InfoContext(ctx,
				"Participant isn't admin so he cannot change admin flag of the other participant",
				"user_id", event.BehalfUserId,
				"chat_id", event.ChatId,
			)
			return nil
		}

		_, err = tx.ExecContext(ctx, "update chat_participant set chat_admin = $1 where user_id = $2 and chat_id = $3", event.NewAdmin, event.ParticipantId, event.ChatId)
		return err
	})
}

func (m *CommonProjection) GetParticipantIdsForExternal(ctx context.Context, chatId int64, size int32, offset int64, reverse bool) ([]ParticipantWithAdmin, error) {
	return getParticipantIdsCommon(ctx, m.db, chatId, nil, size, offset, reverse)
}

func (m *CommonProjection) IterateOverChatParticipantIds(ctx context.Context, co db.CommonOperations, chatId int64, excluding []int64, consumer func(participantIdsPortion []int64) error) error {
	shouldContinue := true
	var lastError error
	for page := int64(0); shouldContinue; page++ {
		offset := utils.GetOffset(page, utils.DefaultSize)
		participants, err := getParticipantIdsCommon(ctx, co, chatId, excluding, utils.DefaultSize, offset, false)
		if err != nil {
			m.lgr.ErrorContext(ctx, "Got error during getting portion", "err", err)
			lastError = err
			break
		}
		if len(participants) == 0 {
			return nil
		}
		if len(participants) < utils.DefaultSize {
			shouldContinue = false
		}

		participantIds := GetParticipantIds(participants)

		err = consumer(participantIds)
		if err != nil {
			m.lgr.ErrorContext(ctx, "Got error during invoking consumer portion", "err", err)
			lastError = err
			break
		}
	}
	return lastError
}

func (m *CommonProjection) IsChatAdmin(ctx context.Context, co db.CommonOperations, userId, chatId int64) (bool, error) {
	rm := co.QueryRowContext(ctx, "SELECT exists(SELECT * FROM chat_participant WHERE user_id = $1 AND chat_id = $2 AND chat_admin = true LIMIT 1)", userId, chatId)
	if rm.Err() != nil {
		return false, rm.Err()
	}
	var admin bool
	err := rm.Scan(&admin)
	if err != nil {
		return false, err
	}
	return admin, nil
}
