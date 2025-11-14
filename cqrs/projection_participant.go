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
		basic, err := m.GetChatBasic(ctx, tx, event.ChatId)
		if err != nil {
			return err
		}
		if basic == nil {
			m.lgr.InfoContext(ctx, "Skipping ParticipantsAdded because there is no chat", "chat_id", event.ChatId)
			return nil
		}

		admin, err := m.IsChatAdmin(ctx, tx, event.AdditionalData.BehalfUserId, event.ChatId)
		if err != nil {
			return err
		}
		if !admin && !event.AreFirstUsers {
			if event.IsJoining {
				if !basic.AvailableToSearch && !basic.IsBlog {
					m.lgr.InfoContext(ctx,
						"Participant isn't allowed to join to chat",
						"user_id", event.AdditionalData.BehalfUserId,
						"chat_id", event.ChatId,
					)
					return nil
				}
			} else {
				m.lgr.InfoContext(ctx,
					"Participant isn't admin so he cannot add a participant",
					"user_id", event.AdditionalData.BehalfUserId,
					"chat_id", event.ChatId,
				)
				return nil
			}
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
		user_input as (
			select unnest(cast ($1 as bigint[])) as user_id
		),
		input_data as (
			select 
				c.id as chat_id, 
				false as pinned, 
				u.user_id as user_id, 
				cast ($3 as timestamp) as update_date_time
			from user_input u
			cross join (select cc.id, cc.title from chat_common cc where cc.id = $2) c 
		)
		insert into chat_user_view(id, pinned, user_id, update_date_time) 
			select chat_id, pinned, user_id, update_date_time from input_data
		on conflict(user_id, id) do update set
			pinned = excluded.pinned
			, update_date_time = excluded.update_date_time 
		`, GetParticipantIds(event.Participants), event.ChatId, event.AdditionalData.CreatedAt)
		if err != nil {
			return err
		}

		err = m.updateViewableParticipants(ctx, tx, event.ChatId)
		if err != nil {
			return err
		}

		// recalc in case an user was added after
		err = m.initializeMessageUnreadMultipleParticipants(ctx, tx, GetParticipantIds(event.Participants), event.ChatId)
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

func (m *CommonProjection) OnParticipantRemoved(ctx context.Context, additionalData *AdditionalData, participantIds []int64, chatId int64, behalfUserId int64, isLeaving bool, isRemoveAllParticipants bool) error {
	errOuter := db.Transact(ctx, m.db, func(tx *db.Tx) error {
		admin, err := m.IsChatAdmin(ctx, tx, behalfUserId, chatId)
		if err != nil {
			return err
		}
		if !isLeaving && !admin {
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

		if !isRemoveAllParticipants { // an optimization for chat deletion
			err = m.updateViewableParticipants(ctx, tx, chatId)
			if err != nil {
				return err
			}
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

func (m *CommonProjection) updateViewableParticipants(ctx context.Context, co db.CommonOperations, chatId int64) error {
	_, err := co.ExecContext(ctx, `
		with 
		this_chat_participants as (
			select user_id, create_date_time from chat_participant where chat_id = $1
		),
		chat_participant_count as (
			select count (*) as count from this_chat_participants
		),
		chat_participants_last_n as (
			select user_id from this_chat_participants order by create_date_time desc limit $2
		),
		input_data as (
			select 
				(select count from chat_participant_count) as participants_count, 
				(select array_agg(user_id) from chat_participants_last_n) as participant_ids
		)
		update chat_common cc
		SET 
			participants_count = (select participants_count from input_data),
			participant_ids = (select participant_ids from input_data)
		where cc.id = $1
		`, chatId, m.cfg.Cqrs.Projections.ChatUserView.MaxViewableParticipants)
	if err != nil {
		return err
	}

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

// output: behalfUserId:[]*dto.UserViewEnrichedDto
func (m *EnrichingProjection) GetParticipantsEnriched(ctx context.Context, behalfUserIds []int64, chatId int64, size int32, offset int64, searchString string, needCount bool, userIds []int64) (map[int64][]*dto.UserViewEnrichedDto, int64, error) {
	isSingleBehalf := len(behalfUserIds) == 1

	if isSingleBehalf {
		behalfUserId := behalfUserIds[0]
		participant, err := m.cp.IsParticipant(ctx, m.cp.db, behalfUserId, chatId)
		if err != nil {
			return nil, 0, err
		}
		if !participant {
			return nil, 0, NewUnauthorizedError(fmt.Sprintf("user %v is not a participant of chat %v", behalfUserId, chatId))
		}
	}

	searchString = sanitizer.TrimAmdSanitize(m.policy, searchString)

	if !isSingleBehalf && len(searchString) > 0 {
		return nil, 0, fmt.Errorf("Wrong invariant - we cannot use both searchString and multiple behalfs")
	}

	if offset > 0 && len(userIds) > 0 {
		return nil, 0, fmt.Errorf("Wrong invariant - we cannot use both offset and multiple userIds")
	}

	const reverse = true

	type participantsWithCount struct {
		participants       []*ParticipantWithAdmin
		count              int64
		areAdminsOfUserIds map[int64]bool
		chat               *dto.ChatBasic
	}

	type usersWithCount struct {
		usersWithAdmin     []*dto.UserWithAdmin
		count              int64
		areAdminsOfUserIds map[int64]bool
		chat               *dto.ChatBasic
	}

	if len(searchString) > 0 {
		if len(behalfUserIds) != 1 {
			return nil, 0, fmt.Errorf("Wrong invariant - for searchString we should have exactly 1 behalfUserId")
		}

		behalfUserId := behalfUserIds[0]

		pwc, errOuter := db.TransactWithResult(ctx, m.cp.db, func(tx *db.Tx) (*usersWithCount, error) {
			usersWithAdmin, count, err := m.SearchUsersContaining(ctx, tx, searchString, chatId, size, offset, reverse, needCount)
			if err != nil {
				m.lgr.ErrorContext(ctx, "Error getting participant ids", "err", err)
				return nil, err
			}

			areAdminsOfUserIds, err := m.cp.getAreAdminsOfUserIds(ctx, tx, behalfUserIds, chatId)
			if err != nil {
				return nil, err
			}

			chat, err := m.cp.GetChatBasic(ctx, tx, chatId)
			if err != nil {
				return nil, err
			}

			if chat == nil {
				return nil, fmt.Errorf("No chat found, chatId = %v", chatId)
			}

			return &usersWithCount{
				usersWithAdmin:     usersWithAdmin,
				count:              count,
				areAdminsOfUserIds: areAdminsOfUserIds,
				chat:               chat,
			}, nil
		})
		if errOuter != nil {
			return nil, 0, errors.New("Error getting participants")
		}

		enrichedUsers := makeEnrichedUsers(pwc.usersWithAdmin, behalfUserId, pwc.areAdminsOfUserIds[behalfUserId], pwc.chat.TetATet)

		return map[int64][]*dto.UserViewEnrichedDto{
			behalfUserId: enrichedUsers,
		}, pwc.count, nil
	} else {
		pwc, errOuter := db.TransactWithResult(ctx, m.cp.db, func(tx *db.Tx) (*participantsWithCount, error) {
			var participants []*ParticipantWithAdmin
			var err error

			if len(userIds) == 0 {
				participants, err = getParticipantsCommonExcepting(ctx, tx, chatId, nil, size, offset, reverse)
				if err != nil {
					m.lgr.ErrorContext(ctx, "Error getting participants", "err", err)

					return nil, err
				}
			} else {
				participants, err = getParticipantsCommonIncluding(ctx, tx, chatId, userIds, int32(len(userIds)), 0, reverse)
				if err != nil {
					m.lgr.ErrorContext(ctx, "Error getting participants", "err", err)

					return nil, err
				}
			}

			areAdminsOfUserIds, err := m.cp.getAreAdminsOfUserIds(ctx, tx, behalfUserIds, chatId)
			if err != nil {
				return nil, err
			}

			chat, err := m.cp.GetChatBasic(ctx, tx, chatId)
			if err != nil {
				return nil, err
			}

			if chat == nil {
				return nil, fmt.Errorf("No chat found, chatId = %v", chatId)
			}

			var theCount int64
			if needCount {
				theCount, err = getParticipantsCount(ctx, tx, chatId)
				if err != nil {
					m.lgr.ErrorContext(ctx, "Error getting participant count", "err", err)

					return nil, err
				}
			}

			return &participantsWithCount{
				participants:       participants,
				areAdminsOfUserIds: areAdminsOfUserIds,
				count:              theCount,
				chat:               chat,
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

		res := map[int64][]*dto.UserViewEnrichedDto{}

		for _, behalfUserId := range behalfUserIds {
			enrichedUsersBehalfUser := makeEnrichedUsers(orderedEnrichedParticipants, behalfUserId, pwc.areAdminsOfUserIds[behalfUserId], pwc.chat.TetATet)
			res[behalfUserId] = enrichedUsersBehalfUser
		}

		return res, pwc.count, nil
	}
}

func makeEnrichedUsers(users []*dto.UserWithAdmin, behalfUserId int64, behalfIsChatAdmin bool, isTetATetChat bool) []*dto.UserViewEnrichedDto {
	var res = make([]*dto.UserViewEnrichedDto, 0, len(users))
	for _, u := range users {
		enriched := dto.UserViewEnrichedDto{
			BehalfUserId:  behalfUserId,
			UserWithAdmin: *u,
			CanChange:     CanChangeParticipant(behalfUserId, behalfIsChatAdmin, isTetATetChat, u),
			CanDelete:     CanDeleteParticipant(behalfUserId, behalfIsChatAdmin, isTetATetChat, u),
		}
		res = append(res, &enriched)
	}

	return res
}

func CanChangeParticipant(behalfUserId int64, behalfIsChatAdmin bool, isTetATetChat bool, user *dto.UserWithAdmin) bool {
	return CanEditChat(behalfIsChatAdmin, isTetATetChat) && CanChangeChatParticipants(behalfIsChatAdmin, isTetATetChat) && user.Id != behalfUserId
}

func CanDeleteParticipant(behalfUserId int64, behalfIsChatAdmin bool, isTetATetChat bool, user *dto.UserWithAdmin) bool {
	return CanEditChat(behalfIsChatAdmin, isTetATetChat) && user.Id != behalfUserId
}

func (m *EnrichingProjection) ParticipantsFilter(ctx context.Context, co db.CommonOperations, searchString string, chatId int64, requestedParticipantIds []int64) ([]dto.FilteredParticipantItemResponse, error) {
	userSearchString := sanitizer.TrimAmdSanitize(m.policy, searchString)

	var response = []dto.FilteredParticipantItemResponse{}

	if userSearchString != "" {
		var batches = [][]int64{}
		var batch = []int64{}
		for _, pid := range requestedParticipantIds {
			batch = append(batch, pid)
			if len(batch) == utils.DefaultSize {
				batches = append(batches, batch)
				batch = []int64{}
			}
		}
		for _, aBatch := range batches { // we already know that requestedParticipantIds belong to this chat, so our sole task is to pass them through aaa filter
			usersPortion, _, err := m.aaaRestClient.SearchGetUsers(ctx, userSearchString, true, aBatch, 0, utils.DefaultSize)
			if err != nil {
				m.lgr.ErrorContext(ctx, "Error get users from aaa", "err", err)
			} else {
				for _, user := range usersPortion {
					response = append(response, dto.FilteredParticipantItemResponse{user.Id})
				}
			}
		}
	} else {
		foundParticipantIds, err := m.cp.ParticipantsExistence(ctx, co, chatId, requestedParticipantIds)
		if err != nil {
			return nil, err
		}

		for _, userId := range foundParticipantIds {
			response = append(response, dto.FilteredParticipantItemResponse{Id: userId})
		}
	}

	return response, nil
}

func (m *EnrichingProjection) SearchUsersContaining(ctx context.Context, co db.CommonOperations, searchString string, chatId int64, pageSize int32, requestOffset int64, reverse bool, needCount bool) ([]*dto.UserWithAdmin, int64, error) {
	searchString = sanitizer.TrimAmdSanitize(m.policy, searchString)

	var resUsers = make([]*dto.UserWithAdmin, 0)
	shouldContinue := true
	processedItems := int64(0)
	totalCountInChat := int64(0) // total count is for pagination in ParticipantsModal - should react on search

	// iterate over all chat participants
	for page := int64(0); shouldContinue; page++ {
		offset := utils.GetOffset(page, pageSize)
		participantsPortion, err := getParticipantsCommonExcepting(ctx, co, chatId, nil, utils.DefaultSize, offset, reverse)
		if int32(len(participantsPortion)) < pageSize {
			shouldContinue = false
		}
		if err != nil {
			m.lgr.ErrorContext(ctx, "Got error during getting portion", "err", err)
			break
		}

		participantIds := GetParticipantIdsP(participantsPortion)

		// we don't send offset to SearchGetUsers(), because it's enriching, the base are participantsPortion from getParticipantsCommonExcepting()
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
			} else if !needCount {
				shouldContinue = false
				break
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
func (m *CommonProjection) IterateOverChatParticipantIdsExcepting(ctx context.Context, co db.CommonOperations, chatId int64, excluding []int64, consumer func(participantIdsPortion []int64) error) error {
	shouldContinue := true
	var lastError error
	for page := int64(0); shouldContinue; page++ {
		offset := utils.GetOffset(page, utils.DefaultSize)
		participants, err := getParticipantsCommonExcepting(ctx, co, chatId, excluding, utils.DefaultSize, offset, false)
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

func (m *CommonProjection) IterateOverChatParticipantIdsIncluding(ctx context.Context, co db.CommonOperations, chatId int64, including []int64, consumer func(participantIdsPortion []int64) error) error {
	shouldContinue := true
	var lastError error
	for page := int64(0); shouldContinue; page++ {
		offset := utils.GetOffset(page, utils.DefaultSize)
		participants, err := getParticipantsCommonIncluding(ctx, co, chatId, including, utils.DefaultSize, offset, false)
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

func (m *CommonProjection) IterateOverCoChattedParticipantIds(ctx context.Context, co db.CommonOperations, participantId int64, consumer func(participantIdsPortion []int64) error) error {
	shouldContinue := true
	var lastError error
	for page := int64(0); shouldContinue; page++ {
		offset := utils.GetOffset(page, utils.DefaultSize)

		participantIds := []int64{}

		err := sqlscan.Select(ctx, co, &participantIds, "SELECT DISTINCT user_id FROM chat_participant WHERE chat_id IN (SELECT chat_id FROM chat_participant WHERE user_id = $1) ORDER BY user_id LIMIT $2 OFFSET $3", participantId, utils.DefaultSize, offset)
		if err != nil {
			m.lgr.ErrorContext(ctx, "Got error during getting portion", "err", err)
			lastError = err
			break
		}
		if len(participantIds) == 0 {
			return nil
		}
		if len(participantIds) < utils.DefaultSize {
			shouldContinue = false
		}

		err = consumer(participantIds)
		if err != nil {
			m.lgr.ErrorContext(ctx, "Got error during invoking consumer portion", "err", err)
			lastError = err
			break
		}
	}
	return lastError
}

func (m *CommonProjection) GetParticipantIds(ctx context.Context, co db.CommonOperations, chatId int64, participantsSize int32, participantsOffset int64) ([]int64, error) {
	pwa, err := getParticipantsCommonExcepting(ctx, co, chatId, nil, participantsSize, participantsOffset, true)
	if err != nil {
		return nil, err
	}

	return GetParticipantIdsP(pwa), nil
}

func (m *CommonProjection) GetParticipantsCount(ctx context.Context, co db.CommonOperations, chatId int64) (int64, error) {
	return getParticipantsCount(ctx, co, chatId)
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

func (m *CommonProjection) areAdminsCommon(ctx context.Context, co db.CommonOperations, participantIds []int64, chatIds []int64) ([]ParticipantAdmin, error) {
	list := []ParticipantAdmin{}
	err := sqlscan.Select(ctx, co, &list, `
		select 
			user_id,
			chat_id,
			chat_admin
		from chat_participant
		where user_id = any($1) and chat_id = any($2)
		order by create_date_time
	`, participantIds, chatIds)
	if err != nil {
		return nil, err
	}
	return list, nil
}

func (m *CommonProjection) GetAreAdminsOfUserIds(ctx context.Context, co db.CommonOperations, participantIds []int64, chatId int64) (map[int64]bool, error) {
	return m.getAreAdminsOfUserIds(ctx, co, participantIds, chatId)
}

// returns [userId]isAdmin
func (m *CommonProjection) getAreAdminsOfUserIds(ctx context.Context, co db.CommonOperations, participantIds []int64, chatId int64) (map[int64]bool, error) {
	res := map[int64]bool{}

	list, err := m.areAdminsCommon(ctx, co, participantIds, []int64{chatId})
	if err != nil {
		return res, err
	}

	for _, pa := range list {
		res[pa.UserId] = pa.Admin
	}

	return res, nil
}

// returns [chatId]isAdmin
func (m *CommonProjection) getAreAdminsOfChatIds(ctx context.Context, co db.CommonOperations, participantId int64, chatIds []int64) (map[int64]bool, error) {
	res := map[int64]bool{}

	list, err := m.areAdminsCommon(ctx, co, []int64{participantId}, chatIds)
	if err != nil {
		return res, err
	}

	for _, pa := range list {
		res[pa.ChatId] = pa.Admin
	}

	return res, nil
}

type ParticipantAdmin struct {
	UserId int64 `db:"user_id"`
	ChatId int64 `db:"chat_id"`
	Admin  bool  `db:"chat_admin"`
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

func getParticipantsCommonExcepting(ctx context.Context, co db.CommonOperations, chatId int64, excluding []int64, participantsSize int32, participantsOffset int64, reverseOrder bool) ([]*ParticipantWithAdmin, error) {
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

func getParticipantsCommonIncluding(ctx context.Context, co db.CommonOperations, chatId int64, including []int64, participantsSize int32, participantsOffset int64, reverseOrder bool) ([]*ParticipantWithAdmin, error) {
	list := make([]*ParticipantWithAdmin, 0)

	var err error

	order := "asc"
	if reverseOrder {
		order = "desc"
	}

	sqlArgs := []any{chatId, participantsSize, participantsOffset}
	condition := "AND user_id IN (select * from unnest(cast ($4 as bigint[])))"
	sqlArgs = append(sqlArgs, including)

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

func CanChangeChatParticipants(isAdmin, isTetATet bool) bool {
	return isAdmin && !isTetATet
}

func CanAddParticipant(admin, tetATet, isJoining, chatIsAvailableToSearch, chatIsBlog bool) bool {
	if CanEditChat(admin, tetATet) {
		// ok
	} else {
		if isJoining {
			if !chatIsAvailableToSearch && !chatIsBlog {
				return false
			}
		} else {
			return false
		}
	}

	return true
}

func CanRemoveParticipant(admin, tetATet, isLeaving, isParticipant bool) bool {
	if !admin {
		if isLeaving && CanLeaveChat(admin, tetATet, isParticipant) {
			// ok
		} else {
			return false
		}
	}
	return true
}
