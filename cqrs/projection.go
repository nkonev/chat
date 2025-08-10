package cqrs

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"github.com/georgysavva/scany/v2/sqlscan"
	"go-cqrs-chat-example/client"
	"go-cqrs-chat-example/config"
	"go-cqrs-chat-example/db"
	"go-cqrs-chat-example/logger"
)

type CommonProjection struct {
	db                 *db.DB
	lgr                *logger.LoggerWrapper
	chatUserViewConfig *config.ChatUserViewConfig
	blogViewConfig     *config.BlogViewConfig
}

type EnrichingProjection struct {
	cp            *CommonProjection
	lgr           *logger.LoggerWrapper
	aaaRestClient client.AaaRestClient
}

func NewCommonProjection(db *db.DB, lgr *logger.LoggerWrapper, cfg *config.AppConfig) *CommonProjection {
	return &CommonProjection{
		db:                 db,
		lgr:                lgr,
		chatUserViewConfig: &cfg.Projections.ChatUserView,
		blogViewConfig:     &cfg.Projections.BlogView,
	}
}

func NewEnrichingProjection(cp *CommonProjection, lgr *logger.LoggerWrapper, aaaRestClient client.AaaRestClient) *EnrichingProjection {
	return &EnrichingProjection{
		cp:            cp,
		lgr:           lgr,
		aaaRestClient: aaaRestClient,
	}
}

func (m *CommonProjection) GetNextChatId(ctx context.Context, tx *db.Tx) (int64, error) {
	var nid int64
	err := sqlscan.Get(ctx, tx, &nid, "select nextval('chat_id_sequence')")
	if err != nil {
		return 0, err
	}
	return nid, nil
}

func (m *CommonProjection) InitializeChatIdSequenceIfNeed(ctx context.Context, tx *db.Tx) error {
	var called bool
	err := sqlscan.Get(ctx, tx, &called, "SELECT is_called FROM chat_id_sequence")
	if err != nil {
		return err
	}

	if !called {
		var maxChatId int64
		err = sqlscan.Get(ctx, tx, &maxChatId, "SELECT coalesce(max(id), 0) from chat_common")
		if err != nil {
			return err
		}

		if maxChatId > 0 {
			m.lgr.Info("Fast-forwarding chatId sequence")
			_, err = tx.ExecContext(ctx, "SELECT setval('chat_id_sequence', $1, true)", maxChatId)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

const ChatStillNotExists = -1

func (m *CommonProjection) GetNextMessageId(ctx context.Context, tx *db.Tx, chatId int64) (int64, error) {
	var messageId int64
	err := sqlscan.Get(ctx, tx, &messageId, "UPDATE chat_common SET last_generated_message_id = last_generated_message_id + 1 WHERE id = $1 RETURNING last_generated_message_id;", chatId)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			// there were no rows, but otherwise no error occurred
			return ChatStillNotExists, nil
		}
		return 0, fmt.Errorf("error during generating message id: %w", err)
	}
	return messageId, nil
}

func (m *CommonProjection) InitializeMessageIdSequenceIfNeed(ctx context.Context, tx *db.Tx, chatId int64) error {
	var currentGeneratedMessageId int64

	err := sqlscan.Get(ctx, tx, &currentGeneratedMessageId, "SELECT coalesce(last_generated_message_id, 0) from chat_common where id = $1", chatId)
	if err != nil {
		return err
	}

	if currentGeneratedMessageId == 0 {
		var maxMessageId int64
		err = sqlscan.Get(ctx, tx, &maxMessageId, "SELECT coalesce(max(id), 0) from message where chat_id = $1", chatId)
		if err != nil {
			return err
		}

		if maxMessageId > 0 {
			m.lgr.Info("Fast-forwarding messageId sequence", "chat_id", chatId)

			_, err = tx.ExecContext(ctx, "update chat_common set last_generated_message_id = $2 where id = $1", chatId, maxMessageId)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (m *CommonProjection) SetIsNeedToFastForwardSequences(ctx context.Context) error {
	_, err := m.db.ExecContext(ctx, "insert into technical(id, need_to_fast_forward_sequences) values (1, true) on conflict (id) do update set need_to_fast_forward_sequences = excluded.need_to_fast_forward_sequences")
	return err
}

func (m *CommonProjection) UnsetIsNeedToFastForwardSequences(ctx context.Context, tx *db.Tx) error {
	_, err := tx.ExecContext(ctx, "delete from technical where need_to_fast_forward_sequences = true")
	return err
}

func (m *CommonProjection) GetIsNeedToFastForwardSequences(ctx context.Context, tx *db.Tx) (bool, error) {
	var e bool
	err := sqlscan.Get(ctx, tx, &e, "select exists(select * from technical where need_to_fast_forward_sequences = true)")
	if err != nil {
		return false, err
	}
	return e, err
}

const lockIdKey1 = 1
const lockIdKey2 = 2

func (m *CommonProjection) SetXactFastForwardSequenceLock(ctx context.Context, tx *db.Tx) error {
	_, err := tx.ExecContext(ctx, "select pg_advisory_xact_lock($1, $2)", lockIdKey1, lockIdKey2)
	return err
}
