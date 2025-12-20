package tasks

import (
	"context"
	"github.com/nkonev/dcron"
	"go-cqrs-chat-example/client"
	"go-cqrs-chat-example/config"
	"go-cqrs-chat-example/db"
	"go-cqrs-chat-example/logger"
	"go-cqrs-chat-example/utils"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"
)

type CleanChatsOfDeletedUserTask struct {
	dcron.Job
}

func CleanChatsOfDeletedUserScheduler(
	lgr *logger.LoggerWrapper,
	service *CleanChatsOfDeletedUserService,
	cfg *config.AppConfig,
) *CleanChatsOfDeletedUserTask {
	const key = "cleanChatsOfDeletedUserTask"
	var str = cfg.Schedulers.CleanChatsOfDeletedUserTask.Cron
	lgr.Info("Created CleanChatsOfDeletedUserScheduler with cron %v", str)

	job := dcron.NewJob(key, str, func(ctx context.Context) error {
		service.doJob(ctx)
		return nil
	}, dcron.WithTracing(service.spanStarter, service.spanFinisher))

	return &CleanChatsOfDeletedUserTask{job}
}

type CleanChatsOfDeletedUserService struct {
	restClient client.AaaRestClient
	tracer     trace.Tracer
	dbR        *db.DB
	lgr        *logger.LoggerWrapper
}

func (srv *CleanChatsOfDeletedUserService) doJob(ctx context.Context) {
	srv.processChats(ctx)
}

func (srv *CleanChatsOfDeletedUserService) processChats(c context.Context) {
	srv.lgr.InfoContext(c, "Starting cleaning chats of deleted user job")

	errOuter := db.Transact(c, srv.dbR, func(tx *db.Tx) error {
		return tx.IterateOverAllParticipantIds(c, func(participantIds []int64) error {
			existResponse, err := srv.restClient.CheckAreUsersExists(c, participantIds)
			if err != nil {
				srv.lgr.ErrorContext(c, "Got error getting existResponse", "err", err)
				return nil
			}
			if existResponse == nil {
				srv.lgr.ErrorContext(c, "Got null getting existResponse", "err", err)
				return nil
			}

			for _, userExists := range *existResponse {
				if !userExists.Exists {
					// remove message_read
					srv.lgr.InfoContext(c, "Deleting message read for user", "user_id", userExists.UserId)
					err = tx.DeleteAllMessageRead(c, userExists.UserId)
					if err != nil {
						srv.lgr.ErrorContext(c, "Got error delete message read", "err", err)
					}
					// remove from chat_participants
					srv.lgr.InfoContext(c, "Deleting participance for user", "user_id", userExists.UserId)
					err = tx.DeleteUserAsAParticipantFromAllChats(c, userExists.UserId)
					if err != nil {
						srv.lgr.ErrorContext(c, "Got error delete participance", "err", err)
					}
					srv.lgr.InfoContext(c, "Deleting pinned chats for user", "user_id", userExists.UserId)
					err = tx.DeleteChatsPinned(c, userExists.UserId)
					if err != nil {
						srv.lgr.ErrorContext(c, "Got error delete chat pinned", "err", err)
					}
					srv.lgr.WithTracing(c).Infof("Deleting notification settings for user", "user_id", userExists.UserId)
					err = tx.DeleteAllChatParticipantNotification(c, userExists.UserId)
					if err != nil {
						srv.lgr.ErrorContext(c, "Got error delete notification settings", "err", err)
					}
				}
			}
			return nil
		})
	})
	if errOuter != nil {
		srv.lgr.ErrorContext(c, "Got error during remove an user leftovers", "err", errOuter)
	}

	// batch by chats // ... order by id
	var hasMoreChats = true
	for chatPage := int64(0); hasMoreChats; chatPage++ {
		errOuter = db.Transact(c, srv.dbR, func(tx *db.Tx) error {
			chatIds, err := tx.GetChatIds(c, utils.DefaultSize, utils.GetOffset(chatPage, utils.DefaultSize))
			if err != nil {
				return err
			}
			hasMoreChats = len(chatIds) == utils.DefaultSize

			hasParticipantsMap, err := tx.HasParticipants(c, chatIds)
			if err != nil {
				srv.lgr.ErrorContext(c, "Got error HasParticipants", "err", err)
				return nil
			}

			for _, chatId := range chatIds {
				// if chat has 0 participants - then remove chat
				hasParticipants := hasParticipantsMap[chatId]
				if !hasParticipants {
					srv.lgr.InfoContext(c, "Deleting chat because it does not have participants", "chat_id", chatId)
					err = tx.DeleteChat(c, chatId)
					if err != nil {
						srv.lgr.ErrorContext(c, "Got error DeleteChat", "err", err)
						continue
					}
				}
			}
			return nil
		})
		if errOuter != nil {
			srv.lgr.ErrorContext(c, "Got error in the portion", "page", chatPage, "err", errOuter)
		}

	}

	srv.lgr.InfoContext(c, "End of cleaning chats of deleted user job")
}

func (srv *CleanChatsOfDeletedUserService) spanStarter(ctx context.Context) (context.Context, any) {
	return srv.tracer.Start(ctx, "scheduler.cleanChatsOfDeletedUser")
}

func (srv *CleanChatsOfDeletedUserService) spanFinisher(ctx context.Context, span any) {
	span.(trace.Span).End()
}

func NewCleanChatsOfDeletedUserService(lgr *logger.LoggerWrapper, chatClient client.AaaRestClient, dbR *db.DB) *CleanChatsOfDeletedUserService {
	trcr := otel.Tracer("scheduler/clean-chats-of-deleted-user")
	return &CleanChatsOfDeletedUserService{
		restClient: chatClient,
		tracer:     trcr,
		dbR:        dbR,
		lgr:        lgr,
	}
}
