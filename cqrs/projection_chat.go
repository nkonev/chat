package cqrs

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"go-cqrs-chat-example/db"
	"go-cqrs-chat-example/dto"
	"go-cqrs-chat-example/services"
	"go-cqrs-chat-example/utils"
	"slices"
	"time"

	"github.com/georgysavva/scany/v2/sqlscan"
	"github.com/jackc/pgtype"
)

func (m *CommonProjection) GetChatIds(ctx context.Context, tx *db.Tx, size int32, offset int64) ([]int64, error) {
	ma := []int64{}

	err := sqlscan.Select(ctx, tx, &ma, `
		select c.id
		from chat_common c
		order by c.id asc 
		limit $1 offset $2
	`, size, offset)

	if err != nil {
		return ma, err
	}
	return ma, nil
}

func (m *CommonProjection) OnChatCreated(ctx context.Context, event *ChatCreated) error {
	_, err := m.db.ExecContext(ctx, `
		insert into chat_common(
			id, 
			title,
			create_date_time,
			can_resend,
			tet_a_tet,
			avatar,
			avatar_big
		) values (
			$1, 
			$2,
			$3,
			$4,
			$5,
		    $6,
		    $7
		)
		on conflict(id) do update set 
		    title = excluded.title
		    ,can_resend = excluded.can_resend
		    ,tet_a_tet = excluded.tet_a_tet
		    ,avatar = excluded.avatar
		    ,avatar_big = excluded.avatar_big
	`, event.ChatId, event.Title, event.AdditionalData.CreatedAt, event.CanResend, event.TetATet, event.Avatar, event.AvatarBig)
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

		admin, err := m.IsChatAdmin(ctx, tx, event.AdditionalData.BehalfUserId, event.ChatId)
		if err != nil {
			return err
		}
		if !admin {
			m.lgr.InfoContext(ctx,
				"Participant isn't admin so he cannot change chat",
				"user_id", event.AdditionalData.BehalfUserId,
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
			    can_resend = $3,
			    avatar = $4,
			    avatar_big = $5
			where id = $1
		`, event.ChatId, event.Title, event.CanResend, event.Avatar, event.AvatarBig)
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
			err = m.removeBlog(ctx, tx, event.ChatId)
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

		admin, err := m.IsChatAdmin(ctx, tx, event.AdditionalData.BehalfUserId, event.ChatId)
		if err != nil {
			return err
		}
		if !admin {
			m.lgr.InfoContext(ctx,
				"Participant isn't admin so he cannot delete chat",
				"user_id", event.AdditionalData.BehalfUserId,
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
			err = m.removeBlog(ctx, tx, event.ChatId)
			if err != nil {
				return err
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
	errOuter := db.Transact(ctx, m.db, func(tx *db.Tx) error {
		participant, err := m.IsParticipant(ctx, tx, event.AdditionalData.BehalfUserId, event.ChatId)
		if err != nil {
			return err
		}
		if !participant {
			m.lgr.InfoContext(ctx, "Skipping ChatPinned because participant isn't participant", "user_id", event.AdditionalData.BehalfUserId, "chat_id", event.ChatId)
			return nil
		}

		_, err = tx.ExecContext(ctx, `
		update chat_user_view
		set pinned = $3
		where (id, user_id) = ($1, $2)
	`, event.ChatId, event.AdditionalData.BehalfUserId, event.Pinned)
		if err != nil {
			return err
		}
		return nil
	})
	if errOuter != nil {
		return errOuter
	}

	m.lgr.InfoContext(ctx,
		"Chat pinned",
		"user_id", event.AdditionalData.BehalfUserId,
		"chat_id", event.ChatId,
		"pinned", event.Pinned,
	)

	return nil
}

func (m *CommonProjection) OnChatNotificationSettingsSetted(ctx context.Context, event *ChatNotificationSettingsSetted) error {

	errOuter := db.Transact(ctx, m.db, func(tx *db.Tx) error {
		participant, err := m.IsParticipant(ctx, tx, event.AdditionalData.BehalfUserId, event.ChatId)
		if err != nil {
			return err
		}
		if !participant {
			m.lgr.InfoContext(ctx, "Skipping ChatNotificationSettingsSetted because participant isn't participant", "user_id", event.AdditionalData.BehalfUserId, "chat_id", event.ChatId)
			return nil
		}

		_, err = tx.ExecContext(ctx, `
		update chat_user_view 
		set consider_messages_as_unread = $3
		where id = $1 and user_id = $2 
	`, event.ChatId, event.AdditionalData.BehalfUserId, event.Setted)
		if err != nil {
			return err
		}

		m.lgr.InfoContext(ctx,
			"Chat notification settings setted",
			"user_id", event.AdditionalData.BehalfUserId,
			"chat_id", event.ChatId,
			"setted", event.Setted,
		)

		err = m.updateHasUnreads(ctx, tx, []int64{event.AdditionalData.BehalfUserId})
		if err != nil {
			return err
		}

		return nil
	})

	// TODO send consider_messages_as_unread via event

	return errOuter
}

// called in cases when chat should lift because of changing update_date_time
// in other cases (for example, read all the messafes in the chat), when no need to update th timestamp - we should use another method
func (m *CommonProjection) OnChatViewRefreshed(ctx context.Context, additionalData *AdditionalData, participantIds []int64, chatId int64, unreadMessagesAction UnreadMessagesAction, lastMessageAction LastMessageAction, participantsAction ParticipantsAction, increaseOn int, messageOwnerId int64) error {
	errOuter := db.Transact(ctx, m.db, func(tx *db.Tx) error {
		// in oder not to have a potential race condition
		// for example "by upserting refresh view we can resurrect view of the newly removed participant in case message add"
		// we shouldn't upsert into chat_user_view
		// we can only update it here

		wasUpdated := false
		if unreadMessagesAction == UnreadMessagesActionIncrease {
			participantIdsWithoutMessageOwner := utils.GetSliceWithout(messageOwnerId, participantIds)
			var ownerIdP *int64
			if slices.Contains(participantIds, messageOwnerId) { // for batches with[out] owner
				ownerIdP = &messageOwnerId
			}

			// not owners
			if len(participantIdsWithoutMessageOwner) > 0 && increaseOn > 0 {
				_, err := tx.ExecContext(ctx, `
					UPDATE chat_user_view 
					SET unread_messages = unread_messages + $3
					WHERE user_id = any($1) and id = $2;
				`, participantIdsWithoutMessageOwner, chatId, increaseOn)
				if err != nil {
					return fmt.Errorf("error during increasing unread messages: %w", err)
				}

				// upsert only for sake using CTE
				_, err = tx.ExecContext(ctx, `
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
				`, participantIdsWithoutMessageOwner, chatId)
				if err != nil {
					return fmt.Errorf("error during setting has unread messages: %w", err)
				}
			}

			// owner
			if ownerIdP != nil {
				_, err := tx.ExecContext(ctx, `
					UPDATE chat_user_view 
					SET last_read_message_id = (select max(id) from message where chat_id = $2)
					WHERE (user_id, id) = ($1, $2);
				`, *ownerIdP, chatId)
				if err != nil {
					return fmt.Errorf("error during increasing unread messages: %w", err)
				}
			}

			wasUpdated = true
		} else if unreadMessagesAction == UnreadMessagesActionRefresh {
			err := m.setUnreadMessages(ctx, tx, participantIds, chatId, 0, true, true)
			if err != nil {
				return err
			}

			wasUpdated = true
		}

		// it's not forgotten else, it's the different action
		if lastMessageAction == LastMessageActionRefresh {
			err := m.setLastMessage(ctx, tx, participantIds, chatId)
			if err != nil {
				return err
			}

			wasUpdated = true
		}

		if participantsAction == ParticipantsActionRefresh {
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
				`, participantIds, chatId, m.chatUserViewConfig.MaxViewableParticipants)
			if err != nil {
				return fmt.Errorf("error during increasing unread messages: %w", err)
			}

			wasUpdated = true
		}

		if wasUpdated {
			_, err := tx.ExecContext(ctx, `
				update chat_user_view set update_date_time = $3 where user_id = any($1) and id = $2
			`, participantIds, chatId, additionalData.CreatedAt)
			if err != nil {
				return err
			}
		}

		return nil
	})

	if errOuter != nil {
		return errOuter
	}
	return nil
}

func (m *CommonProjection) checkChatExists(ctx context.Context, co db.CommonOperations, chatId int64) (bool, error) {
	var chatExists bool

	err := sqlscan.Get(ctx, co, &chatExists, "select exists (select * from chat_common where id = $1)", chatId)

	if err != nil {
		return false, err
	}
	return chatExists, nil
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

// contract: either multiple chats
// or one chatId != nil
func (m *EnrichingProjection) GetChatsEnriched(ctx context.Context, behalfParticipantIds []int64, size int32, startingFromItemId *dto.ChatId, includeStartingFrom, reverse bool, searchString string, chatId *int64) ([]dto.ChatViewEnrichedDto, map[int64]*dto.User, error) {
	if len(behalfParticipantIds) == 0 {
		return nil, nil, errors.New("Wrong invariant")
	}
	multipleBehalfUserId := len(behalfParticipantIds) > 1
	if multipleBehalfUserId && chatId == nil {
		return nil, nil, errors.New("Wrong invariant")
	}

	searchString = services.TrimAmdSanitize(m.policy, searchString)

	additionalFoundUserIds := m.searchForUsers(ctx, searchString)

	type tupleDto struct {
		resultChats       []dto.ChatViewEnrichedDto
		intermediateUsers map[int64]*dto.User
	}

	d, errOuter := db.TransactWithResult(ctx, m.cp.db, func(tx *db.Tx) (*tupleDto, error) {
		chats, err := m.cp.GetChats(ctx, tx, behalfParticipantIds, size, startingFromItemId, includeStartingFrom, reverse, searchString, additionalFoundUserIds, chatId)
		if err != nil {
			m.lgr.ErrorContext(ctx, "Error getting chats", "err", err)
			return nil, err
		}

		userIds := getUserIdsFromChats(chats) // max num of users should fit aaa's limitation
		users, err := m.aaaRestClient.GetUsers(ctx, userIds)
		if err != nil {
			m.lgr.WarnContext(ctx, "unable to get users")
		}

		usersMap := utils.ToMap(users)

		var areAdminsOfUserIds = map[int64]bool{}
		var areAdminsOfChatIds = map[int64]bool{}
		if multipleBehalfUserId {
			areAdminsOfUserIds, err = m.cp.getAreAdminsOfUserIds(ctx, tx, behalfParticipantIds, *chatId)
			if err != nil {
				return nil, err
			}
		} else {
			chatIds := getChatIdsFromChats(chats)

			areAdminsOfChatIds, err = m.cp.getAreAdminsOfChatIds(ctx, tx, behalfParticipantIds[0], chatIds)
			if err != nil {
				return nil, err
			}
		}

		chatsEnriched := make([]dto.ChatViewEnrichedDto, 0, len(chats))
		for _, ch := range chats {
			var admin bool
			if multipleBehalfUserId {
				admin = areAdminsOfUserIds[ch.UserId]
			} else {
				admin = areAdminsOfChatIds[ch.Id]
			}

			che := enrichChat(ch.UserId, ch, usersMap, admin)
			chatsEnriched = append(chatsEnriched, che)
		}

		return &tupleDto{
			resultChats:       chatsEnriched,
			intermediateUsers: usersMap,
		}, nil
	})
	if errOuter != nil {
		return nil, nil, errOuter
	}
	return d.resultChats, d.intermediateUsers, nil
}

func (m *EnrichingProjection) searchForUsers(ctx context.Context, searchString string) []int64 {
	var additionalFoundUserIds = []int64{}

	if searchString != "" && searchString != dto.ReservedPublicallyAvailableForSearchChats {
		users, _, err := m.aaaRestClient.SearchGetUsers(ctx, searchString, true, []int64{}, 0, 0)
		if err != nil {
			m.lgr.ErrorContext(ctx, "Error get users from aaa", "err", err)
		}
		for _, u := range users {
			additionalFoundUserIds = append(additionalFoundUserIds, u.Id)
		}
	}
	return additionalFoundUserIds
}

func getUserIdsFromChats(chats []dto.ChatViewDto) []int64 {
	m := map[int64]struct{}{}

	for _, ch := range chats {
		for _, p := range ch.ParticipantIds {
			m[p] = struct{}{}
		}

		if ch.LastMessageOwnerId != nil {
			m[*ch.LastMessageOwnerId] = struct{}{}
		}
	}

	r := []int64{}

	for k, _ := range m {
		r = append(r, k)
	}
	return r
}

func getChatIdsFromChats(chats []dto.ChatViewDto) []int64 {
	m := map[int64]struct{}{}

	for _, ch := range chats {
		m[ch.Id] = struct{}{}
	}

	r := []int64{}

	for k, _ := range m {
		r = append(r, k)
	}
	return r
}

func enrichChat(behalfUserId int64, ch dto.ChatViewDto, users map[int64]*dto.User, admin bool) dto.ChatViewEnrichedDto {
	che := dto.ChatViewEnrichedDto{
		ChatViewDto:  ch,
		Participants: makeParticipants(ch.ParticipantIds, users),
	}
	if che.TetATet {
		oppa := utils.GetSliceWithout(behalfUserId, che.ParticipantIds)
		if len(oppa) == 1 {
			oppositeUserId := oppa[0]
			oppositeUser := users[oppositeUserId]
			if oppositeUser != nil {
				che.Title = oppositeUser.Login
				che.Avatar = oppositeUser.Avatar
				// TODO set last seen
			}
		}
	}
	SetChatPersonalizedFields(&che, admin, true)

	if ch.LastMessageOwnerId != nil && ch.LastMessageContent != nil {
		u := users[*ch.LastMessageOwnerId]
		if u != nil {
			preview := createMessagePreview(*ch.LastMessageContent, u.Login)
			che.LastMessagePreview = &preview
		}
	}

	return che
}

func createMessagePreview(text, login string) string {
	input := loginPrefix(login) + text
	return input
}

func loginPrefix(login string) string {
	return login + ": "
}

func SetChatPersonalizedFields(copied *dto.ChatViewEnrichedDto, admin bool, participant bool) {
	canEdit := admin && !copied.TetATet
	copied.CanEdit = &canEdit
	canDelete := admin
	copied.CanDelete = &canDelete
	canLeave := !admin && !copied.TetATet && participant
	copied.CanLeave = &canLeave
	copied.CanVideoKick = admin
	copied.CanAudioMute = admin
	copied.CanChangeChatAdmins = admin && !copied.TetATet
	copied.CanBroadcast = admin

	if !participant {
		isResultFromSearch := true
		copied.IsResultFromSearch = &isResultFromSearch
	}

	copied.CanWriteMessage = true
	// see also handlers PostMessage, EditMessage, DeleteMessage
	if !copied.RegularParticipantCanWriteMessage && !admin {
		copied.CanWriteMessage = false
	}
	// TODO pinned
}

func (m *CommonProjection) GetChats(ctx context.Context, co db.CommonOperations, participantIds []int64, size int32, startingFromItemId *dto.ChatId, includeStartingFrom, reverse bool, searchString string, additionalFoundUserIds []int64, chatId *int64) ([]dto.ChatViewDto, error) {
	type chatDto struct {
		Id                                int64            `db:"id"`
		UserId                            int64            `db:"user_id"`
		Title                             string           `db:"title"`
		Pinned                            bool             `db:"pinned"`
		UnreadMessages                    int64            `db:"unread_messages"`
		LastMessageId                     *int64           `db:"last_message_id"`
		LastMessageOwnerId                *int64           `db:"last_message_owner_id"`
		LastMessageContent                *string          `db:"last_message_content"`
		ParticipantsCount                 int64            `db:"participants_count"`
		ParticipantIds                    pgtype.Int8Array `db:"participant_ids"` // ids of last N participants
		Blog                              bool             `db:"blog"`
		UpdateDateTime                    *time.Time       `db:"update_date_time"`
		TetATet                           bool             `db:"tet_a_tet"`
		Avatar                            *string          `db:"avatar"`
		AvatarBig                         *string          `db:"avatar_big"`
		ConsiderMessagesAsUnread          bool             `db:"consider_messages_as_unread"`
		RegularParticipantCanWriteMessage bool             `db:"regular_participant_can_write_message"`
	}

	list := []chatDto{}
	res := []dto.ChatViewDto{}

	queryArgs := []any{participantIds, size}

	order := "desc"
	offset := " offset 1" // to make behaviour the same as in users, messages (there is > or <)
	if reverse {
		order = "asc"
	}

	orderClause := fmt.Sprintf("order by (ch.pinned, ch.update_date_time, ch.id) %s", order)
	// see also getSafeDefaultUserId() in aaa
	if includeStartingFrom || startingFromItemId == nil {
		offset = ""
	}

	nonEquality := "<="
	if reverse {
		nonEquality = ">="
	}

	conditionClause := ""

	if startingFromItemId != nil && chatId != nil {
		return nil, fmt.Errorf("wrong invariant: both startingFromItemId and chatId provided")
	}

	if startingFromItemId != nil {
		paginationKeyset := fmt.Sprintf(` and (ch.pinned, ch.update_date_time, ch.id) %s ($%d, $%d, $%d)`, nonEquality, len(queryArgs)+1, len(queryArgs)+2, len(queryArgs)+3)
		queryArgs = append(queryArgs, startingFromItemId.Pinned, startingFromItemId.LastUpdateDateTime, startingFromItemId.Id)

		conditionClause = paginationKeyset
	}

	var searchClause = ""
	var searchCte = ""
	if len(searchString) > 0 {
		var additionalUserIdsClause = ""
		if len(additionalFoundUserIds) > 0 {
			queryArgs = append(queryArgs, additionalFoundUserIds)
			searchCte = fmt.Sprintf(`
			with tet_a_tet_chats_ids as materialized (
				SELECT distinct (cp.chat_id) as chat_id
				FROM chat_common cc 
				join chat_participant cp
				on cc.id = cp.chat_id
				WHERE cc.tet_a_tet IS true AND cp.user_id = any($%d)
			)
			`, len(queryArgs))
			additionalUserIdsClause = fmt.Sprintf(" ( cc.id = any(array(SELECT chat_id FROM tet_a_tet_chats_ids)) ) or ")
		}
		// TODO available_to_search
		searchClause = fmt.Sprintf("and ( ( %s cc.title ILIKE $%d ) OR ( (cc.available_to_search = TRUE OR b.id is not null) AND $%d = '%s' ) )", additionalUserIdsClause, len(queryArgs)+1, len(queryArgs)+2, dto.ReservedPublicallyAvailableForSearchChats)
		searchStringPercents := "%" + searchString + "%"
		queryArgs = append(queryArgs, searchStringPercents)
		queryArgs = append(queryArgs, searchString)
	}

	if chatId != nil {
		chatIdV := *chatId
		queryArgs = append(queryArgs, chatIdV)
		chatIdClause := fmt.Sprintf("and ch.id = $%d", len(queryArgs))

		conditionClause = chatIdClause
		orderClause = "order by ch.update_date_time desc, ch.user_id" // to prevent flaky tests. the same as in projection_participantv :: getParticipantsCommon()
	}

	// it is optimized (all order by in the same table)
	// so querying a page (using keyset) from a large amount of chats is fast
	// it's the root cause why we use cqrs
	q := fmt.Sprintf(`
		%s
		select 
		    ch.id,
			ch.user_id,
		    cc.title,
		    ch.pinned,
		    ch.unread_messages,
		    ch.last_message_id,
		    ch.last_message_owner_id,
		    ch.last_message_content,
		    ch.participants_count,
		    ch.participant_ids,
		    b.id is not null as blog,
		    ch.update_date_time,
		    cc.tet_a_tet,
			cc.avatar,
			cc.avatar_big,
			coalesce(ch.consider_messages_as_unread, true) as consider_messages_as_unread,
			cc.regular_participant_can_write_message
		from chat_user_view ch
		left join blog b on ch.id = b.id
		join chat_common cc on cc.id = ch.id
		where ch.user_id = any($1) %s
		%s
		%s
		limit $2 
		%s
		`, searchCte, conditionClause, searchClause, orderClause, offset)
	err := sqlscan.Select(ctx, co, &list, q, queryArgs...)
	if err != nil {
		return res, err
	}

	for i, de := range list {
		mapped := dto.ChatViewDto{
			Id:                                de.Id,
			UserId:                            de.UserId,
			Title:                             de.Title,
			Pinned:                            de.Pinned,
			UnreadMessages:                    de.UnreadMessages,
			LastMessageId:                     de.LastMessageId,
			LastMessageOwnerId:                de.LastMessageOwnerId,
			LastMessageContent:                de.LastMessageContent,
			ParticipantsCount:                 de.ParticipantsCount,
			Blog:                              de.Blog,
			UpdateDateTime:                    de.UpdateDateTime,
			TetATet:                           de.TetATet,
			Avatar:                            de.Avatar,
			AvatarBig:                         de.AvatarBig,
			ConsiderMessagesAsUnread:          de.ConsiderMessagesAsUnread,
			RegularParticipantCanWriteMessage: de.RegularParticipantCanWriteMessage,
		}
		err = de.ParticipantIds.AssignTo(&mapped.ParticipantIds)
		if err != nil {
			return res, fmt.Errorf("error during mapping on index %d: %w", i, err)
		}

		res = append(res, mapped)
	}

	return res, nil
}

func (m *CommonProjection) GetHasUnreadMessages(ctx context.Context, userIds []int64) (map[int64]bool, error) {
	var has = map[int64]bool{}

	type hasDto struct {
		UserId int64 `db:"user_id"`
		Has    bool  `db:"has"`
	}
	list := []hasDto{}
	err := sqlscan.Select(ctx, m.db, &list, `
	with
	normalized_user as (
		select unnest(cast ($1 as bigint[])) as user_id
	)
	select 
		nu.user_id,
		coalesce(h.has, false) as has
	from has_unread_messages h
	right join normalized_user nu on h.user_id = nu.user_id
	where h.user_id = any($1)
	`, userIds)
	if err != nil {
		return nil, err
	}
	for _, hd := range list {
		has[hd.UserId] = hd.Has
	}
	return has, nil
}

func (m *CommonProjection) GetChatByUserIdAndChatId(ctx context.Context, userId, chatId int64) (string, error) {
	var t string
	err := sqlscan.Get(ctx, m.db, &t, "select c.title from chat_user_view ch join chat_common c on ch.id = c.id where ch.user_id = $1 and ch.id = $2", userId, chatId)
	if err != nil {
		return "", err
	}
	return t, nil
}

func (m *CommonProjection) GetChatUserViewBasic(ctx context.Context, co db.CommonOperations, chatId, participantId int64) (dto.ChatUserViewBasic, error) {
	var t dto.ChatUserViewBasic
	err := sqlscan.Get(ctx, co, &t, "select ch.id, ch.update_date_time, ch.unread_messages from chat_user_view ch where ch.user_id = $1 and ch.id = $2", participantId, chatId)
	if err != nil {
		return t, err
	}
	return t, nil

}

func (m *CommonProjection) GetChatBasic(ctx context.Context, co db.CommonOperations, chatId int64) (*dto.ChatBasic, error) {
	var cht dto.ChatBasic

	err := sqlscan.Get(ctx, co, &cht, `
		select 
		    ch.id,
		    ch.title,
		    ch.can_resend,
		    ch.tet_a_tet,
			b.id is not null as blog
		from chat_common ch 
		left join blog b on ch.id = b.id
		where ch.id = $1
	`, chatId)

	if errors.Is(err, sql.ErrNoRows) {
		// there were no rows, but otherwise no error occurred
		return nil, nil
	} else if err != nil {
		return nil, err
	}
	return &cht, nil
}

func (m *CommonProjection) GetChatsBasicExtended(ctx context.Context, co db.CommonOperations, chatIds []int64, behalfParticipantIds []int64) (map[int64]*dto.BasicChatDtoExtended, error) {
	result := map[int64]*dto.BasicChatDtoExtended{}
	if len(chatIds) == 0 {
		return result, nil
	}

	list := []dto.BasicChatDtoExtended{}
	err := sqlscan.Select(ctx, co, &list, `
		SELECT 
			c.id, 
			c.title,
			(cp.user_id is not null) as behalf_user_is_participant,
			c.tet_a_tet,
			c.can_resend,
			c.regular_participant_can_publish_message,
			c.regular_participant_can_pin_message,
			c.regular_participant_can_write_message,
			b.id is not null as blog
		FROM chat_common c 
		LEFT JOIN chat_participant cp ON (c.id = cp.chat_id AND cp.user_id = any($1))
		left join blog b on c.id = b.id
		WHERE c.id = any($2)`,
		behalfParticipantIds, chatIds)
	if err != nil {
		return nil, err
	}
	for _, bc := range list {
		result[bc.Id] = &bc
	}
	return result, nil
}

func (m *CommonProjection) GetChatNotificationSettings(ctx context.Context, behalfParticipantId int64, chatId int64) (*dto.UserChatNotificationSettings, error) {
	value := dto.UserChatNotificationSettings{}
	err := sqlscan.Get(ctx, m.db, &value, "select ch.consider_messages_as_unread from chat_user_view ch where ch.user_id = $1 and ch.id = $2", behalfParticipantId, chatId)

	if errors.Is(err, sql.ErrNoRows) {
		// there were no rows, but otherwise no error occurred
		return nil, nil
	} else if err != nil {
		return nil, err
	}

	return &value, err
}
