package cqrs

import (
	"context"
	"go-cqrs-chat-example/db"

	"github.com/georgysavva/scany/v2/sqlscan"
)

func (m *CommonProjection) OnThreadCreated(ctx context.Context, event *ThreadCreated) error {
	_, err := m.db.ExecContext(ctx, `update message set thread_id = $3 where chat_id = $1 and id = $2`, event.ChatId, event.MessageId, event.ThreadId)
	if err != nil {
		return err
	}
	return nil
}

func (m *CommonProjection) OnThreadDeleted(ctx context.Context, event *ThreadDeleted) (int64, error) {
	threadIdOuter, errOuter := db.TransactWithResult(ctx, m.db, func(tx *db.Tx) (int64, error) {
		var threadId int64
		err := sqlscan.Get(ctx, tx, &threadId, "select coalesce((select thread_id from message where chat_id = $1 and id = $2), 0)", event.ChatId, event.MessageId)
		if err != nil {
			return 0, err
		}

		_, err = tx.ExecContext(ctx, `update message set thread_id = null where chat_id = $1 and id = $2`, event.ChatId, event.MessageId)
		if err != nil {
			return 0, err
		}

		return threadId, nil
	})
	return threadIdOuter, errOuter
}
