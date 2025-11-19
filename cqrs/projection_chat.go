package cqrs

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"go-cqrs-chat-example/db"
	"go-cqrs-chat-example/dto"
	"go-cqrs-chat-example/preview"
	"go-cqrs-chat-example/sanitizer"
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
	// we don't check chat existence for the chat creation

	_, err := m.db.ExecContext(ctx, `
		insert into chat_common(
			 id
			,title
			,create_date_time
			,tet_a_tet
			,avatar
			,avatar_big
			,can_resend
			,can_react
			,available_to_search
			,regular_participant_can_publish_message
			,regular_participant_can_pin_message
			,regular_participant_can_write_message
		) values (
			$1
			,$2
			,$3
			,$4
			,$5
		    ,$6
		    ,$7
		    ,$8
		    ,$9
		    ,$10
		    ,$11
		    ,$12
		)
		on conflict(id) do update set 
		    title = excluded.title
		    ,tet_a_tet = excluded.tet_a_tet
		    ,avatar = excluded.avatar
		    ,avatar_big = excluded.avatar_big
			,can_resend = excluded.can_resend
			,can_react = excluded.can_react
			,available_to_search = excluded.available_to_search
			,regular_participant_can_publish_message = excluded.regular_participant_can_publish_message
			,regular_participant_can_pin_message = excluded.regular_participant_can_pin_message
			,regular_participant_can_write_message = excluded.regular_participant_can_write_message
	`, event.ChatId, event.Title, event.AdditionalData.CreatedAt, event.TetATet, event.Avatar, event.AvatarBig, event.CanResend, event.CanReact, event.AvailableToSearch, event.RegularParticipantCanPublishMessage, event.RegularParticipantCanPinMessage, event.RegularParticipantCanWriteMessage)
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

		blog, errInner := m.isChatBlog(ctx, tx, event.ChatId)
		if errInner != nil {
			return errInner
		}

		_, errInner = tx.ExecContext(ctx, `
			update chat_common
			set title = $2
			    ,avatar = $3
			    ,avatar_big = $4
				,can_resend = $5
				,can_react = $6
				,available_to_search = $7
				,regular_participant_can_publish_message = $8
				,regular_participant_can_pin_message = $9
				,regular_participant_can_write_message = $10
			where id = $1
		`, event.ChatId, event.Title, event.Avatar, event.AvatarBig, event.CanResend, event.CanReact, event.AvailableToSearch, event.RegularParticipantCanPublishMessage, event.RegularParticipantCanPinMessage, event.RegularParticipantCanWriteMessage)
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
		// we don't check IsChatAdmin because a participant was already removed

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

		_, errInner = m.db.ExecContext(ctx, `
			delete from message
			where chat_id = $1
		`, event.ChatId)
		if errInner != nil {
			return errInner
		}

		if blog {
			err := m.removeBlog(ctx, tx, event.ChatId)
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
func (m *CommonProjection) OnChatViewRefreshed(ctx context.Context, additionalData *AdditionalData, participantIds []int64, chatId int64, unreadMessagesAction UnreadMessagesAction, lastMessageAction LastMessageAction, increaseOn int, messageOwnerId int64, chatAction ChatAction) error {
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
				err := m.increaseUnreadsAndSetHasUnreads(ctx, tx, participantIdsWithoutMessageOwner, chatId, increaseOn)
				if err != nil {
					return fmt.Errorf("error during increasing unread messages: %w", err)
				}
			}

			// owner
			if ownerIdP != nil {
				err := m.fastForwardLastRead(ctx, tx, *ownerIdP, chatId)
				if err != nil {
					return fmt.Errorf("error during increasing unread messages: %w", err)
				}

				err = m.fastForwardParticipantMessageReadId(ctx, tx, *ownerIdP, chatId, additionalData.CreatedAt)
				if err != nil {
					return fmt.Errorf("error during increasing unread messages: %w", err)
				}

				// update red dot
				err = m.updateHasUnreads(ctx, tx, []int64{*ownerIdP})
				if err != nil {
					return err
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
			err := m.setLastMessage(ctx, tx, chatId)
			if err != nil {
				return err
			}

			wasUpdated = true
		}

		if chatAction == ChatActionRefresh {
			// for the cases like renaming chat, ...
			// the db was updated earlier, here we need to update chat_user_view.update_date_time
			wasUpdated = true
		}

		// to eliminate unnecessary chat_user_view writes in participant changed
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

func (m *EnrichingProjection) ChatFilter(ctx context.Context, co db.CommonOperations, behalfUserId, chatId int64, searchString string) (bool, error) {
	participant, err := m.cp.IsParticipant(ctx, co, behalfUserId, chatId)
	if err != nil {
		return false, err
	}
	if !participant {
		return false, NewUnauthorizedError(fmt.Sprintf("user %v is not a participant of chat %v", behalfUserId, chatId))
	}

	searchString = sanitizer.TrimAmdSanitize(m.policy, searchString)

	additionalFoundUserIds := m.searchForUsers(ctx, searchString)

	queryArgs := []any{chatId, behalfUserId}

	var searchClause = ""
	var searchCte = ""
	if len(searchString) > 0 {
		searchClause += " and ("

		searchClauseT, searchCteT, _, queryArgsT := processAdditionalUserIds(queryArgs, additionalFoundUserIds, searchString)
		searchClause += searchClauseT
		searchCte = searchCteT
		queryArgs = queryArgsT

		searchClause += " ) "
	}

	var found bool
	err = sqlscan.Get(ctx, co, &found, fmt.Sprintf(`
		%s
		SELECT EXISTS (
			select 1
			from chat_common cc
			join chat_user_view ch on (cc.id = ch.id and ch.user_id = $2)
			left join blog b on ch.id = b.id
			where ch.id = $1
			%s
		)
	`, searchCte, searchClause), queryArgs...)
	if err != nil {
		return false, err
	}

	return found, nil
}

func processAdditionalUserIds(queryArgsInput []any, additionalFoundUserIds []int64, searchString string) (searchClause string, searchCte string, searchForPublic bool, queryArgs []any) {
	queryArgs = queryArgsInput
	var additionalUserIdsClause = ""
	searchForPublic = searchString == dto.ReservedPublicallyAvailableForSearchChats
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
	searchClause = fmt.Sprintf(" ( ( %s cc.title ILIKE $%d ) OR ( (cc.available_to_search = TRUE OR b.id is not null) AND $%d = true ) )", additionalUserIdsClause, len(queryArgs)+1, len(queryArgs)+2)
	searchStringPercents := "%" + searchString + "%"
	queryArgs = append(queryArgs, searchStringPercents)
	queryArgs = append(queryArgs, searchForPublic)

	return
}

// contract: either multiple chats
// or one chatId != nil
func (m *EnrichingProjection) GetChatsEnriched(ctx context.Context, behalfParticipantIds []int64, size int32, startingFromItemId *dto.ChatId, includeStartingFrom, reverse bool, searchString string, chatId *int64) ([]dto.ChatViewEnrichedDto, map[int64]*dto.User, error) {
	if len(behalfParticipantIds) == 0 {
		return nil, nil, errors.New("Wrong invariant: len(behalfParticipantIds) == 0")
	}
	multipleBehalfUserId := len(behalfParticipantIds) > 1
	if multipleBehalfUserId && chatId == nil {
		return nil, nil, errors.New("Wrong invariant: multipleBehalfUserId is true and null chatId")
	}

	searchString = sanitizer.TrimAmdSanitize(m.policy, searchString)

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

		participantIds, participantOfTetAtetId := getUserIdsFromChats(chats) // max num of users should fit aaa's limitation
		users, err := m.aaaRestClient.GetUsers(ctx, participantIds)
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

		tetATetOnlines, err := m.getParticipantsOnlineForTetATetMap(ctx, participantOfTetAtetId)
		if err != nil {
			m.lgr.WarnContext(ctx, "Something bad during getting tetATetOnlines", "err", err)
		}

		chatsEnriched := make([]dto.ChatViewEnrichedDto, 0, len(chats))
		for _, ch := range chats {
			var admin bool
			if multipleBehalfUserId {
				admin = areAdminsOfUserIds[ch.BehalfUserId]
			} else {
				admin = areAdminsOfChatIds[ch.Id]
			}

			che := m.enrichChat(ch.BehalfUserId, ch, usersMap, admin, tetATetOnlines)
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

func (m *EnrichingProjection) getParticipantsOnlineForTetATetMap(ctx context.Context, userIds []int64) (map[int64]bool, error) {
	ret := map[int64]bool{}

	if len(userIds) == 0 {
		return ret, nil
	}

	onlines, err := m.aaaRestClient.GetOnlines(ctx, userIds) // get online for opposite user
	if err != nil {
		m.lgr.WarnContext(ctx, "Unable to get online for", "user_ids", userIds, "err", err)
		// nothing
		return ret, nil
	}

	for _, onl := range onlines {
		ret[onl.Id] = onl.Online
	}
	return ret, err
}

func (m *EnrichingProjection) GetChat(ctx context.Context, userId, chatId int64) (res *dto.ChatViewEnrichedDto, shouldJoin bool, err error) {
	size := int32(1)
	reverse := false

	var startingFromItemId *dto.ChatId = nil
	includeStartingFrom := true
	searchString := ""

	chats, _, errG := m.GetChatsEnriched(ctx, []int64{userId}, size, startingFromItemId, includeStartingFrom, reverse, searchString, &chatId)
	if errG != nil {
		m.lgr.ErrorContext(ctx, "Error getting chats", "err", errG)
		err = errG
		return
	}

	if len(chats) == 0 {
		basic, errB := m.cp.GetChatBasic(ctx, m.cp.db, chatId)
		if errB != nil {
			m.lgr.ErrorContext(ctx, "Error getting basic chat", "err", errB)
			err = errB
			return
		}
		if basic != nil && (basic.AvailableToSearch || basic.IsBlog) {
			shouldJoin = true
			return
		} else {
			res = nil
			return
		}
	} else if len(chats) > 1 {
		err = errors.New("Wrong invariant: More than 1 chats got")
		return
	}

	chat := chats[0]
	res = &chat
	return
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

func getUserIdsFromChats(chats []dto.ChatViewDto) ([]int64, []int64) {
	m := map[int64]struct{}{}
	mt := map[int64]struct{}{}

	for _, ch := range chats {
		for _, p := range ch.ParticipantIds {
			m[p] = struct{}{}

			if ch.TetATet {
				mt[p] = struct{}{}
			}
		}

		if ch.LastMessageOwnerId != nil {
			m[*ch.LastMessageOwnerId] = struct{}{}
		}
	}

	r := []int64{}
	rt := []int64{}

	for k, _ := range m {
		r = append(r, k)
	}

	for k, _ := range mt {
		rt = append(rt, k)
	}

	return r, rt
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

func (m *EnrichingProjection) enrichChat(behalfUserId int64, ch dto.ChatViewDto, users map[int64]*dto.User, admin bool, tetATetOnlines map[int64]bool) dto.ChatViewEnrichedDto {
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

				che.ShortInfo = oppositeUser.ShortInfo
				che.LoginColor = oppositeUser.LoginColor
				che.AdditionalData = oppositeUser.AdditionalData

				if oppositeUserId != behalfUserId {
					che.LastSeenDateTime = oppositeUser.LastSeenDateTime

					onl, ok := tetATetOnlines[oppositeUser.Id]
					if ok {
						if onl { // if the opposite user is online we don't need to show last login
							che.LastSeenDateTime = nil
						}
					}
				}
			}
		}
	}
	SetChatPersonalizedFields(&che, behalfUserId, admin, ch.IsParticipant)

	if ch.LastMessageOwnerId != nil && ch.LastMessageContent != nil {
		u := users[*ch.LastMessageOwnerId]
		if u != nil {
			previewStr := preview.CreateMessagePreview(m.stripAllTags, m.cfg.Message.PreviewMaxTextSize, *ch.LastMessageContent, u.Login)
			che.LastMessagePreview = &previewStr
		}
	}

	return che
}

func SetChatPersonalizedFields(copied *dto.ChatViewEnrichedDto, behalfUserId int64, admin bool, participant bool) {
	canEdit := CanEditChat(admin, copied.TetATet)
	copied.CanEdit = &canEdit
	canDelete := CanDeleteChat(admin)
	copied.CanDelete = &canDelete
	canLeave := CanLeaveChat(admin, copied.TetATet, participant)
	copied.CanLeave = &canLeave
	copied.CanVideoKick = admin
	copied.CanAudioMute = admin
	copied.CanChangeChatAdmins = CanChangeParticipant(behalfUserId, admin, copied.TetATet, dto.NonExistentUser)
	copied.CanBroadcast = CanBroadcast(admin)

	// yes, mutate the fields
	copied.CanReact = CanReactOnMessage(copied.CanReact, participant)
	copied.CanResend = CanResendMessage(copied.CanResend, participant)

	// participant can be false in case result from search for publicly available chats
	copied.IsResultFromSearch = !participant

	copied.CanWriteMessage = CanWriteMessage(participant, admin, copied.RegularParticipantCanWriteMessage)
}

func CanEditChat(isAdmin, tetATet bool) bool {
	return isChatAdminInternal(isAdmin) && !tetATet
}

func CanDeleteChat(isAdmin bool) bool {
	return isAdmin
}

func CanLeaveChat(isAdmin, tetATet, isParticipant bool) bool {
	return !isAdmin && !tetATet && isParticipant
}

func CanBroadcast(isAdmin bool) bool {
	return isAdmin
}

func CanReactOnMessage(chatCanReact bool, isParticipant bool) bool {
	return chatCanReact && isParticipant
}

func CanResendMessage(chatCanResend bool, isParticipant bool) bool {
	return chatCanResend && isParticipant
}

func (m *CommonProjection) GetChatDataForAuthorization(ctx context.Context, co db.CommonOperations, userId, chatId int64) (dto.ChatAuthorizationData, error) {
	d := dto.ChatAuthorizationData{}
	err := sqlscan.Get(ctx, co, &d, `
		with
		chat_participant_row as (
			SELECT user_id, chat_admin FROM chat_participant WHERE user_id = $1 AND chat_id = $2 LIMIT 1
		),
		chat_info as (
			select * from chat_common where id = $2
		),
		chat_blog as (
			select b.id is not null as is_blog
			from chat_common cc
			left join blog b on cc.id = b.id
			where cc.id = $2
		)
		SELECT 
			(SELECT exists(SELECT * FROM chat_participant_row) as is_chat_participant)
			,(SELECT exists(SELECT * FROM chat_participant_row WHERE chat_admin) as is_chat_admin)
			,(select cc.regular_participant_can_write_message as chat_can_write_message from chat_info cc)
			,(select cc.can_resend as chat_can_resend_message from chat_info cc)
			,(select cc.can_react as chat_can_react_on_message from chat_info cc)
			,(select cc.tet_a_tet as chat_is_tet_a_tet from chat_info cc)
			,(select cc.available_to_search as chat_is_available_to_search from chat_info cc)
			,(select cb.is_blog as chat_is_blog from chat_blog cb)
	`, userId, chatId)
	if err != nil {
		return d, err
	}
	return d, nil
}

func (m *CommonProjection) GetChats(ctx context.Context, co db.CommonOperations, participantIds []int64, size int32, startingFromItemId *dto.ChatId, includeStartingFrom, reverse bool, searchString string, additionalFoundUserIds []int64, chatId *int64) ([]dto.ChatViewDto, error) {
	type chatDto struct {
		Id                                  int64            `db:"id"`
		UserId                              int64            `db:"user_id"`
		Title                               string           `db:"title"`
		Pinned                              bool             `db:"pinned"`
		UnreadMessages                      int64            `db:"unread_messages"`
		LastMessageId                       *int64           `db:"last_message_id"`
		LastMessageOwnerId                  *int64           `db:"last_message_owner_id"`
		LastMessageContent                  *string          `db:"last_message_content"`
		ParticipantsCount                   int64            `db:"participants_count"`
		ParticipantIds                      pgtype.Int8Array `db:"participant_ids"` // ids of last N participants
		Blog                                bool             `db:"blog"`
		UpdateDateTime                      *time.Time       `db:"update_date_time"`
		TetATet                             bool             `db:"tet_a_tet"`
		Avatar                              *string          `db:"avatar"`
		AvatarBig                           *string          `db:"avatar_big"`
		ConsiderMessagesAsUnread            bool             `db:"consider_messages_as_unread"`
		CanResend                           bool             `db:"can_resend"`
		CanReact                            bool             `db:"can_react"`
		RegularParticipantCanPublishMessage bool             `db:"regular_participant_can_publish_message"`
		RegularParticipantCanPinMessage     bool             `db:"regular_participant_can_pin_message"`
		RegularParticipantCanWriteMessage   bool             `db:"regular_participant_can_write_message"`
		AvailableToSearch                   bool             `db:"available_to_search"`
		IsParticipant                       bool             `db:"is_participant"`
	}

	list := []chatDto{}
	res := []dto.ChatViewDto{}

	queryArgs := []any{size, participantIds, dto.NonExistentUser}

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

	conditionClause := " true "

	var joinClause string

	if startingFromItemId != nil && chatId != nil {
		return nil, fmt.Errorf("wrong invariant: both startingFromItemId and chatId provided")
	}

	if startingFromItemId != nil {
		paginationKeyset := fmt.Sprintf(` and (ch.pinned, ch.update_date_time, ch.id) %s ($%d, $%d, $%d)`, nonEquality, len(queryArgs)+1, len(queryArgs)+2, len(queryArgs)+3)
		queryArgs = append(queryArgs, startingFromItemId.Pinned, startingFromItemId.LastUpdateDateTime, startingFromItemId.Id)

		conditionClause += paginationKeyset
	}

	var searchClause = ""
	var searchCte = ""
	var searchForPublic bool
	if len(searchString) > 0 {
		searchClause = " and ("

		searchClauseT, searchCteT, searchForPublicT, queryArgsT := processAdditionalUserIds(queryArgs, additionalFoundUserIds, searchString)
		searchClause += searchClauseT
		searchCte = searchCteT
		queryArgs = queryArgsT
		searchForPublic = searchForPublicT

		searchClause += " or "
		queryArgs = append(queryArgs, searchString)
		searchClause += fmt.Sprintf(` exists( 
			select 1 from (select * from (select unnest(tsvector_to_array(cc.fts_title))) t(av)) inq 
			where
				   ( inq.av %% to_tsquery('russian', $%d)::text )
			    or ( cyrillic_transliterate(inq.av) %% cyrillic_transliterate(to_tsquery('russian', $%d)::text) ) 
		) `, len(queryArgs), len(queryArgs))

		searchClause += " ) "
	}

	if chatId != nil {
		chatIdV := *chatId
		queryArgs = append(queryArgs, chatIdV)
		chatIdClause := fmt.Sprintf("and ch.id = $%d", len(queryArgs))

		conditionClause += chatIdClause
		orderClause = "order by ch.update_date_time desc, ch.user_id" // to prevent flaky tests. the same as in projection_participantv :: getParticipantsCommonExcepting()
	}

	if !searchForPublic {
		conditionClause += " and ch.user_id = any($2) "
		joinClause = " join "
	} else {
		joinClause = " left join "
	}

	// it is optimized (all order by in the same table)
	// so querying a page (using keyset) from a large amount of chats is fast
	// it's the root cause why we use cqrs
	q := fmt.Sprintf(`
		%s
		select 
		    cc.id,
			coalesce(ch.user_id, $3) as user_id,
		    cc.title,
		    coalesce(ch.pinned, false) as pinned,
		    coalesce(ch.unread_messages, 0) as unread_messages,
		    cc.last_message_id,
		    cc.last_message_owner_id,
		    cc.last_message_content,
		    cc.participants_count,
		    cc.participant_ids,
		    b.id is not null as blog,
		    ch.update_date_time,
		    cc.tet_a_tet,
			cc.avatar,
			cc.avatar_big,
			coalesce(ch.consider_messages_as_unread, true) as consider_messages_as_unread,
			cc.can_resend,
			cc.can_react,
			cc.regular_participant_can_publish_message,
			cc.regular_participant_can_pin_message,
			cc.regular_participant_can_write_message,
			cc.available_to_search,
			ch.id is not null as is_participant
		from chat_common cc
		%s chat_user_view ch on (cc.id = ch.id and ch.user_id = any($2))
		left join blog b on ch.id = b.id
		where %s
		%s
		%s
		limit $1
		%s
		`, searchCte, joinClause, conditionClause, searchClause, orderClause, offset)
	err := sqlscan.Select(ctx, co, &list, q, queryArgs...)
	if err != nil {
		return res, err
	}

	for i, de := range list {
		mapped := dto.ChatViewDto{
			Id:                                  de.Id,
			BehalfUserId:                        de.UserId,
			Title:                               de.Title,
			Pinned:                              de.Pinned,
			UnreadMessages:                      de.UnreadMessages,
			LastMessageId:                       de.LastMessageId,
			LastMessageOwnerId:                  de.LastMessageOwnerId,
			LastMessageContent:                  de.LastMessageContent,
			ParticipantsCount:                   de.ParticipantsCount,
			Blog:                                de.Blog,
			UpdateDateTime:                      de.UpdateDateTime,
			TetATet:                             de.TetATet,
			Avatar:                              de.Avatar,
			AvatarBig:                           de.AvatarBig,
			ConsiderMessagesAsUnread:            de.ConsiderMessagesAsUnread,
			CanResend:                           de.CanResend,
			CanReact:                            de.CanReact,
			RegularParticipantCanPublishMessage: de.RegularParticipantCanPublishMessage,
			RegularParticipantCanPinMessage:     de.RegularParticipantCanPinMessage,
			RegularParticipantCanWriteMessage:   de.RegularParticipantCanWriteMessage,
			AvailableToSearch:                   de.AvailableToSearch,
			IsParticipant:                       de.IsParticipant,
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
		    c.id,
		    c.title,
		    c.can_resend,
		    c.tet_a_tet,
			b.id is not null as blog,
			c.available_to_search,
			c.regular_participant_can_publish_message,
			c.regular_participant_can_pin_message,
			c.regular_participant_can_write_message
		from chat_common c
		left join blog b on c.id = b.id
		where c.id = $1
	`, chatId)

	if errors.Is(err, sql.ErrNoRows) {
		// there were no rows, but otherwise no error occurred
		return nil, nil
	} else if err != nil {
		return nil, err
	}
	return &cht, nil
}

// result: map[userId][chatId]*dto.BasicChatDtoExtended
func (m *CommonProjection) GetChatsBasicExtended(ctx context.Context, co db.CommonOperations, chatIds []int64, behalfParticipantIds []int64) (map[int64]map[int64]*dto.BasicChatDtoExtended, error) {
	result := map[int64]map[int64]*dto.BasicChatDtoExtended{}
	if len(chatIds) == 0 {
		return result, nil
	}

	list := []dto.BasicChatDtoExtended{}
	err := sqlscan.Select(ctx, co, &list, `
		with requested_participants as (
			select * from unnest(cast ($1 as bigint[])) as t(user_id)
		),
		chats_participants as (
			select 
				user_id,
				chat_id 
			from chat_participant cp 
			where cp.chat_id = any($2) AND cp.user_id = any($1)
		)
		SELECT 
			re.user_id,
			c.id,
			c.title,
			(cp.user_id is not null) as behalf_user_is_participant,
			c.tet_a_tet,
			c.can_resend,
			b.id is not null as blog,
			c.available_to_search,
			c.regular_participant_can_publish_message,
			c.regular_participant_can_pin_message,
			c.regular_participant_can_write_message
		FROM chat_common c
		CROSS JOIN requested_participants re
		LEFT JOIN chats_participants cp ON (c.id = cp.chat_id and re.user_id = cp.user_id)
		left join blog b on c.id = b.id
		WHERE c.id = any($2)
	`,
		behalfParticipantIds, chatIds)
	if err != nil {
		return nil, err
	}
	for _, bc := range list {
		innerMap, ok := result[bc.BehalfUserId]
		if !ok {
			innerMap = map[int64]*dto.BasicChatDtoExtended{}
			result[bc.BehalfUserId] = innerMap
		}
		innerMap[bc.Id] = &bc
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

func (m *CommonProjection) GetExistingChatIds(ctx context.Context, co db.CommonOperations, chatIds []int64) ([]int64, error) {
	list := []int64{}
	err := sqlscan.Select(ctx, co, &list, `
	select id from chat_common
	where id = any($1)
	`, chatIds)
	if err != nil {
		return nil, err
	}
	return list, nil
}
