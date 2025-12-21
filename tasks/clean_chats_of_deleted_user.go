package tasks

import (
	"context"
	"github.com/nkonev/dcron"
	"go-cqrs-chat-example/client"
	"go-cqrs-chat-example/config"
	"go-cqrs-chat-example/cqrs"
	"go-cqrs-chat-example/db"
	"go-cqrs-chat-example/dto"
	"go-cqrs-chat-example/logger"
	"go-cqrs-chat-example/utils"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"
)

const CleanChatsOfDeletedUserSchedulerKey = "cleanChatsOfDeletedUserTask"

type CleanChatsOfDeletedUserTask struct {
	dcron.Job
}

func CleanChatsOfDeletedUserScheduler(
	lgr *logger.LoggerWrapper,
	service *CleanChatsOfDeletedUserService,
	cfg *config.AppConfig,
) *CleanChatsOfDeletedUserTask {
	var str = cfg.Schedulers.CleanChatsOfDeletedUserTask.Cron
	lgr.Info("Created CleanChatsOfDeletedUserScheduler with cron", "cron", str, dcron.SlogKeyTaskName, CleanChatsOfDeletedUserSchedulerKey)

	job := dcron.NewJob(CleanChatsOfDeletedUserSchedulerKey, str, func(ctx context.Context) error {
		service.DoJob(ctx)
		return nil
	}, dcron.WithTracing(service.spanStarter, service.spanFinisher))

	return &CleanChatsOfDeletedUserTask{job}
}

type CleanChatsOfDeletedUserService struct {
	restClient client.AaaRestClient
	tracer     trace.Tracer
	dbR        *db.DB
	lgr        *logger.LoggerWrapper
	eventBus   *cqrs.PartitionAwareEventBus
	co         *cqrs.CommonProjection
}

func (srv *CleanChatsOfDeletedUserService) DoJob(ctx context.Context) {
	srv.processChats(ctx)
}

func (srv *CleanChatsOfDeletedUserService) processChats(c context.Context) {
	srv.lgr.InfoContext(c, "Starting cleaning chats of deleted user job")

	errOuter := srv.co.IterateOverAllParticipants(c, srv.dbR, func(chatParticipants []dto.ChatParticipant) error {
		userIdMap := map[int64]struct{}{}
		for _, cp := range chatParticipants {
			userIdMap[cp.UserId] = struct{}{}
		}

		existResponse, err := srv.restClient.CheckAreUsersExists(c, utils.SetMapIdStructToSlice(userIdMap))
		if err != nil {
			srv.lgr.ErrorContext(c, "Got error getting existResponse", "err", err)
			return nil
		}
		if existResponse == nil {
			srv.lgr.ErrorContext(c, "Got null getting existResponse", "err", err)
			return nil
		}

		existsMap := utils.ToMap(existResponse)

		for _, cp := range chatParticipants {
			ue, ok := existsMap[cp.UserId]
			if !ok {
				srv.lgr.WarnContext(c, "aaa responded no exists, probably the error in aaa", "user_id", cp.UserId)
				continue
			}

			if !ue.Exists {
				srv.lgr.InfoContext(c, "Deleting participant because it does not exists in aaa", "user_id", ue.UserId)
				cmd := cqrs.TechnicalRemoveContentOfDeletedUser{ // ~ DeleteParticipant
					UserId: cp.UserId,
					ChatId: cp.ChatId,
				}

				err = cmd.Handle(c, srv.eventBus)
				if err != nil {
					srv.lgr.ErrorContext(c, "error during removing content of deleted user: %w", err)
				}
			}
		}

		return nil
	})
	if errOuter != nil {
		srv.lgr.ErrorContext(c, "error during removing content of deleted user: %w", errOuter)
	}

	errOuter = srv.co.IterateOverAllChats(c, srv.dbR, func(chatIdsPortion []int64) error {
		hasParticipantsMap, err := srv.co.HasParticipants(c, srv.dbR, chatIdsPortion) // will re-check on the projection side after kafka
		if err != nil {
			srv.lgr.ErrorContext(c, "Got error HasParticipants", "err", err)
			return nil
		}

		for _, ch := range chatIdsPortion {
			hasParticipants := hasParticipantsMap[ch]

			if !hasParticipants {
				srv.lgr.InfoContext(c, "Deleting chat because it does not have participants", "chat_id", ch)
				cmd := cqrs.TechnicalRemoveAbandonedChat{
					ChatId: ch,
				}
				err = cmd.Handle(c, srv.eventBus)
				if err != nil {
					srv.lgr.ErrorContext(c, "error during removing abandoned chats: %w", err)
				}
			}
		}
		return nil
	})
	if errOuter != nil {
		srv.lgr.ErrorContext(c, "error during removing abandoned chats: %w", errOuter)
	}

	srv.lgr.InfoContext(c, "End of cleaning chats of deleted user job")
}

func (srv *CleanChatsOfDeletedUserService) spanStarter(ctx context.Context) (context.Context, any) {
	return srv.tracer.Start(ctx, "scheduler.cleanChatsOfDeletedUser")
}

func (srv *CleanChatsOfDeletedUserService) spanFinisher(ctx context.Context, span any) {
	span.(trace.Span).End()
}

func NewCleanChatsOfDeletedUserService(
	lgr *logger.LoggerWrapper,
	chatClient client.AaaRestClient,
	dbR *db.DB,
	eventBus *cqrs.PartitionAwareEventBus,
	co *cqrs.CommonProjection,
) *CleanChatsOfDeletedUserService {
	trcr := otel.Tracer("scheduler/clean-chats-of-deleted-user")
	return &CleanChatsOfDeletedUserService{
		restClient: chatClient,
		tracer:     trcr,
		dbR:        dbR,
		lgr:        lgr,
		eventBus:   eventBus,
		co:         co,
	}
}
