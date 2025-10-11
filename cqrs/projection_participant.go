package cqrs

import (
	"context"
	"errors"
	"fmt"
	"github.com/georgysavva/scany/v2/sqlscan"
	"go-cqrs-chat-example/db"
	"go-cqrs-chat-example/dto"
	"go-cqrs-chat-example/sanitizer"
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

		admin, err := m.IsChatAdmin(ctx, tx, event.AdditionalData.BehalfUserId, event.ChatId)
		if err != nil {
			return err
		}
		if !admin && !event.SkipChatAdminCheck {
			m.lgr.InfoContext(ctx,
				"Participant isn't admin so he cannot add a participant",
				"user_id", event.AdditionalData.BehalfUserId,
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
				false as pinned, 
				u.user_id as user_id, 
				cast ($3 as timestamp) as update_date_time,
				(select count from chat_participant_count) as participants_count, 
				(select array_agg(user_id) from chat_participants_last_n) as participant_ids
			from user_input u
			cross join (select cc.id, cc.title from chat_common cc where cc.id = $2) c 
		)
		insert into chat_user_view(id, pinned, user_id, update_date_time, participants_count, participant_ids) 
			select chat_id, pinned, user_id, update_date_time, participants_count, participant_ids from input_data
		on conflict(user_id, id) do update set
			pinned = excluded.pinned
			, update_date_time = excluded.update_date_time 
			, participants_count = excluded.participants_count 
			, participant_ids = excluded.participant_ids
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

func (m *CommonProjection) OnParticipantRemoved(ctx context.Context, additionalData *AdditionalData, participantIds []int64, chatId int64, behalfUserId int64) error {
	errOuter := db.Transact(ctx, m.db, func(tx *db.Tx) error {
		admin, err := m.IsChatAdmin(ctx, tx, behalfUserId, chatId)
		if err != nil {
			return err
		}
		if !admin {
			m.lgr.InfoContext(ctx,
				"Participant isn't admin so he cannot remove a participant",
				"user_id", behalfUserId,
				"chat_id", chatId,
			)
			return nil
		}

		_, err = tx.ExecContext(ctx, `
		delete from chat_participant where chat_id = $2 and user_id = any($1)
	`, participantIds, chatId)
		if err != nil {
			return err
		}

		_, err = tx.ExecContext(ctx, `
		delete from chat_user_view where user_id = any($1) and id = $2
	`, participantIds, chatId)
		if err != nil {
			return err
		}

		err = m.updateHasUnreads(ctx, tx, participantIds)
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
		"user_ids", participantIds,
		"chat_id", chatId,
	)

	return nil
}

func (m *CommonProjection) OnParticipantChanged(ctx context.Context, event *ParticipantChanged) error {
	return db.Transact(ctx, m.db, func(tx *db.Tx) error {
		admin, err := m.IsChatAdmin(ctx, tx, event.AdditionalData.BehalfUserId, event.ChatId)
		if err != nil {
			return err
		}
		if !admin {
			m.lgr.InfoContext(ctx,
				"Participant isn't admin so he cannot change admin flag of the other participant",
				"user_id", event.AdditionalData.BehalfUserId,
				"chat_id", event.ChatId,
			)
			return nil
		}

		_, err = tx.ExecContext(ctx, "update chat_participant set chat_admin = $1 where user_id = $2 and chat_id = $3", event.NewAdmin, event.ParticipantId, event.ChatId)
		return err
	})
}

func (m *CommonProjection) ParticipantsExistence(ctx context.Context, co db.CommonOperations, chatId int64, participantIds []int64) ([]int64, error) {
	list := make([]int64, 0)

	err := sqlscan.Select(ctx, co, &list, "SELECT user_id FROM chat_participant WHERE chat_id = $1 AND user_id = ANY ($2)", chatId, participantIds)
	if err != nil {
		return nil, err
	}
	return list, nil
}

func (m *EnrichingProjection) GetParticipantsEnriched(ctx context.Context, behalfUserId int64, chatId int64, size int32, offset int64, searchString string) ([]*dto.UserWithAdmin, int64, error) {
	participant, err := m.cp.IsParticipant(ctx, m.cp.db, behalfUserId, chatId)
	if err != nil {
		return nil, 0, err
	}
	if !participant {
		return nil, 0, NewUnauthorizedError(fmt.Sprintf("user %v is not a participant of chat %v", behalfUserId, chatId))
	}

	searchString = sanitizer.TrimAmdSanitize(m.policy, searchString)
	const reverse = true

	if len(searchString) > 0 {
		usersWithAdmin, count, err := m.SearchUsersContaining(ctx, m.cp.db, searchString, chatId, size, offset, reverse)
		if err != nil {
			m.lgr.ErrorContext(ctx, "Error getting participant ids", "err", err)
			return nil, 0, err
		}
		return usersWithAdmin, count, nil
	} else {
		type participantsWithCount struct {
			participants []*ParticipantWithAdmin
			count        int64
		}

		pwc, errOuter := db.TransactWithResult(ctx, m.cp.db, func(tx *db.Tx) (*participantsWithCount, error) {
			participants, err := getParticipantsCommon(ctx, m.cp.db, chatId, nil, size, offset, reverse)
			if err != nil {
				m.lgr.ErrorContext(ctx, "Error getting participants", "err", err)

				return nil, err
			}
			count, err := getParticipantsCount(ctx, m.cp.db, chatId)
			if err != nil {
				m.lgr.ErrorContext(ctx, "Error getting participant count", "err", err)

				return nil, err
			}

			return &participantsWithCount{
				participants: participants,
				count:        count,
			}, nil
		})
		if errOuter != nil {
			return nil, 0, errors.New("Error getting participants")
		}

		participantIds := GetParticipantIdsP(pwc.participants)

		users, err := m.aaaRestClient.GetUsers(ctx, participantIds)
		if err != nil {
			m.lgr.WarnContext(ctx, "unable to get users")
		}

		orderedEnrichedParticipants := makeParticipantsWithAdmin(pwc.participants, utils.ToMap(users))

		return orderedEnrichedParticipants, pwc.count, nil
	}
}

func (m *EnrichingProjection) SearchUsersContaining(ctx context.Context, co db.CommonOperations, searchString string, chatId int64, pageSize int32, requestOffset int64, reverse bool) ([]*dto.UserWithAdmin, int64, error) {
	searchString = sanitizer.TrimAmdSanitize(m.policy, searchString)

	var resUsers = make([]*dto.UserWithAdmin, 0)
	shouldContinue := true
	processedItems := int64(0)
	totalCountInChat := int64(0) // total count is for pagination in ParticipantsModal - should react on search

	// iterate over all chat participants
	for page := int64(0); shouldContinue; page++ {
		offset := utils.GetOffset(page, pageSize)
		participantsPortion, err := getParticipantsCommon(ctx, co, chatId, nil, utils.DefaultSize, offset, reverse)
		if int32(len(participantsPortion)) < pageSize {
			shouldContinue = false
		}
		if err != nil {
			m.lgr.ErrorContext(ctx, "Got error during getting portion", "err", err)
			break
		}

		participantIds := GetParticipantIdsP(participantsPortion)

		// we don't send offset to SearchGetUsers(), because it's enriching, the base are participantsPortion from getParticipantsCommon()
		// page 0 because it's portion by ids
		usersPortion, _, err := m.aaaRestClient.SearchGetUsers(ctx, searchString, true, participantIds, 0, pageSize)
		if err != nil {
			m.lgr.ErrorContext(ctx, "Error get resUsers from aaa", "err", err)
			break
		}

		participantsWithAdminPortionMap := utils.ToMap(participantsPortion)
		usersPortionMap := utils.ToMap(usersPortion)

		// order aaa's users in accordance with participantsPortion
		foundUsersPortionOrderedSlice := make([]*dto.User, 0)
		for _, p := range participantsPortion {
			u, ok := usersPortionMap[p.ParticipantId]
			if ok {
				foundUsersPortionOrderedSlice = append(foundUsersPortionOrderedSlice, u)
			}
		}

		// here we make the intersection of participantsPortion and usersPortion and preserving initial order of participantsPortion
		for _, u := range foundUsersPortionOrderedSlice {
			if int32(len(resUsers)) < pageSize {
				if processedItems >= requestOffset { // skip those whose offset is lower than requested
					participantWithAdmin, ok := participantsWithAdminPortionMap[u.Id]
					if ok {
						resUsers = append(resUsers, &dto.UserWithAdmin{
							User:      *u,
							ChatAdmin: participantWithAdmin.ChatAdmin,
						})
					}
				}
				processedItems++
			}
			totalCountInChat++ // users portion is a subset of participantsPortion, so here we have the actual counter
		}
	}

	return resUsers, totalCountInChat, nil
}

func (m *EnrichingProjection) SearchUsersNotContaining(ctx context.Context, co db.CommonOperations, searchString string, chatId int64, pageSize int32) ([]*dto.User, error) {
	searchString = sanitizer.TrimAmdSanitize(m.policy, searchString)

	var notFoundUsers []*dto.User = make([]*dto.User, 0)
	shouldContinueSearch := true
	for page := int64(0); shouldContinueSearch; page++ {
		ignoredInAaa := false
		usersPortion, _, err := m.aaaRestClient.SearchGetUsers(ctx, searchString, ignoredInAaa, []int64{}, page, pageSize)
		if err != nil {
			m.lgr.ErrorContext(ctx, "Error get resUsers from aaa", "err", err)
			break
		}
		if int32(len(usersPortion)) < pageSize {
			shouldContinueSearch = false
		}

		var portionUserIds = []int64{}
		for _, u := range usersPortion {
			portionUserIds = append(portionUserIds, u.Id)
		}

		foundParticipantIds, err := m.cp.ParticipantsExistence(ctx, co, chatId, portionUserIds)
		if err != nil {
			m.lgr.WarnContext(ctx, "Got error during getting ParticipantsNonExistence", "err", err)
			break
		}
		for _, u := range usersPortion {
			if int32(len(notFoundUsers)) < pageSize {
				if !utils.Contains(foundParticipantIds, u.Id) {
					notFoundUsers = append(notFoundUsers, u)
				}
			} else {
				shouldContinueSearch = false // break outer
				break                        // inner
			}
		}
	}

	return notFoundUsers, nil
}

// you cannot use it in command handler
// if you do this you will introduce a race condition
// see comments in TestUnreads()
func (m *CommonProjection) IterateOverChatParticipantIds(ctx context.Context, co db.CommonOperations, chatId int64, excluding []int64, consumer func(participantIdsPortion []int64) error) error {
	shouldContinue := true
	var lastError error
	for page := int64(0); shouldContinue; page++ {
		offset := utils.GetOffset(page, utils.DefaultSize)
		participants, err := getParticipantsCommon(ctx, co, chatId, excluding, utils.DefaultSize, offset, false)
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

		participantIds := GetParticipantIdsP(participants)

		err = consumer(participantIds)
		if err != nil {
			m.lgr.ErrorContext(ctx, "Got error during invoking consumer portion", "err", err)
			lastError = err
			break
		}
	}
	return lastError
}

func (m *CommonProjection) IterateOverParticipantsChatIds(ctx context.Context, co db.CommonOperations, participantId int64, consumer func(chatIdsPortion []int64) error) error {
	shouldContinue := true
	var lastError error
	for page := int64(0); shouldContinue; page++ {
		offset := utils.GetOffset(page, utils.DefaultSize)
		chatIds, err := getParticipantsChatsCommon(ctx, co, participantId, utils.DefaultSize, offset, false)
		if err != nil {
			m.lgr.ErrorContext(ctx, "Got error during getting portion", "err", err)
			lastError = err
			break
		}
		if len(chatIds) == 0 {
			return nil
		}
		if len(chatIds) < utils.DefaultSize {
			shouldContinue = false
		}

		err = consumer(chatIds)
		if err != nil {
			m.lgr.ErrorContext(ctx, "Got error during invoking consumer portion", "err", err)
			lastError = err
			break
		}
	}
	return lastError
}

func (m *CommonProjection) GetParticipantsCount(ctx context.Context, co db.CommonOperations, chatId int64) (int64, error) {
	var count int64
	err := sqlscan.Get(ctx, co, &count, "SELECT count(*) FROM chat_participant WHERE chat_id = $1", chatId)
	if err != nil {
		return 0, err
	}
	return count, nil
}

func (m *CommonProjection) IsChatAdmin(ctx context.Context, co db.CommonOperations, userId, chatId int64) (bool, error) {
	var admin bool
	err := sqlscan.Get(ctx, co, &admin, "SELECT exists(SELECT * FROM chat_participant WHERE user_id = $1 AND chat_id = $2 AND chat_admin = true LIMIT 1)", userId, chatId)
	if err != nil {
		return false, err
	}
	return admin, nil
}

func (m *CommonProjection) IsParticipant(ctx context.Context, co db.CommonOperations, userId, chatId int64) (bool, error) {
	var participant bool
	err := sqlscan.Get(ctx, co, &participant, "SELECT exists(SELECT * FROM chat_participant WHERE user_id = $1 AND chat_id = $2 LIMIT 1)", userId, chatId)
	if err != nil {
		return false, err
	}
	return participant, nil
}

type ParticipantWithAdmin struct {
	ParticipantId int64 `json:"participantId" db:"user_id"`
	ChatAdmin     bool  `json:"chatAdmin" db:"chat_admin"`
}

func (u *ParticipantWithAdmin) GetId() int64 {
	if u != nil {
		return u.ParticipantId
	} else {
		return dto.NoId
	}
}

func GetParticipantIds(participants []ParticipantWithAdmin) []int64 {
	res := make([]int64, 0, len(participants))
	for _, pa := range participants {
		res = append(res, pa.ParticipantId)
	}
	return res
}

func GetParticipantIdsP(participants []*ParticipantWithAdmin) []int64 {
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

func getParticipantsCount(ctx context.Context, co db.CommonOperations, chatId int64) (int64, error) {
	var res int64

	sqlQuery := `
		SELECT 
		    count(*)
		FROM chat_participant
		WHERE chat_id = $1
	`
	err := sqlscan.Get(ctx, co, &res, sqlQuery, chatId)
	if err != nil {
		return 0, fmt.Errorf("error during interacting with db: %w", err)
	}
	return res, nil
}

func getParticipantsCommon(ctx context.Context, co db.CommonOperations, chatId int64, excluding []int64, participantsSize int32, participantsOffset int64, reverseOrder bool) ([]*ParticipantWithAdmin, error) {
	list := make([]*ParticipantWithAdmin, 0)

	var err error

	order := "asc"
	if reverseOrder {
		order = "desc"
	}

	sqlArgs := []any{chatId, participantsSize, participantsOffset}
	condition := ""
	if len(excluding) > 0 {
		condition = "AND user_id NOT IN (select * from unnest(cast ($4 as bigint[])))"
		sqlArgs = append(sqlArgs, excluding)
	}
	sqlQuery := fmt.Sprintf(`
		SELECT 
		    user_id,
		    chat_admin 
		FROM chat_participant
		WHERE chat_id = $1
			%s
		ORDER BY create_date_time %s, user_id asc
		LIMIT $2 OFFSET $3
	`, condition, order)
	err = sqlscan.Select(ctx, co, &list, sqlQuery, sqlArgs...)
	if err != nil {
		return nil, fmt.Errorf("error during interacting with db: %w", err)
	}
	return list, nil
}

func getParticipantsChatsCommon(ctx context.Context, co db.CommonOperations, participantId int64, chatsSize int32, chatsOffset int64, reverseOrder bool) ([]int64, error) {
	list := make([]int64, 0)

	var err error

	order := "asc"
	if reverseOrder {
		order = "desc"
	}

	sqlArgs := []any{participantId, chatsSize, chatsOffset}
	sqlQuery := fmt.Sprintf(`
		SELECT 
		    chat_id
		FROM chat_participant
		WHERE user_id = $1
		ORDER BY create_date_time %s, user_id asc
		LIMIT $2 OFFSET $3
	`, order)
	err = sqlscan.Select(ctx, co, &list, sqlQuery, sqlArgs...)
	if err != nil {
		return nil, fmt.Errorf("error during interacting with db: %w", err)
	}
	return list, nil
}

func makeParticipants(participantIds []int64, users map[int64]*dto.User) []dto.User {
	res := make([]dto.User, 0, len(participantIds))

	for _, p := range participantIds {
		u := users[p]
		if u != nil {
			res = append(res, *u)
		}
	}

	return res
}

func makeParticipantsWithAdmin(participants []*ParticipantWithAdmin, users map[int64]*dto.User) []*dto.UserWithAdmin {
	res := make([]*dto.UserWithAdmin, 0, len(participants))

	for _, p := range participants {
		u := users[p.ParticipantId]
		if u != nil {
			res = append(res, &dto.UserWithAdmin{
				User:      *u,
				ChatAdmin: p.ChatAdmin,
			})
		}
	}

	return res
}
