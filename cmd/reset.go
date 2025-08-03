package cmd

import (
	"go-cqrs-chat-example/app"
	"go-cqrs-chat-example/config"
	"go-cqrs-chat-example/cqrs"
	"go-cqrs-chat-example/db"
	"go-cqrs-chat-example/kafka"
	"go-cqrs-chat-example/logger"
	"go-cqrs-chat-example/otel"
	"go.uber.org/fx"
	"go.uber.org/fx/fxevent"
	"os"
)

const CommandResetName = "reset"

func RunReset(args []string) {
	cfg, err := config.CreateTypedConfig(args)
	if err != nil {
		panic(err)
	}
	baseLogger := logger.NewBaseLogger(os.Stdout, cfg)
	lgr := logger.NewLogger(baseLogger)

	lgr.Info("Start reset command")

	appFx := fx.New(
		fx.Supply(cfg),
		fx.Supply(lgr),
		fx.WithLogger(func(lgr *logger.LoggerWrapper) fxevent.Logger {
			return &fxevent.SlogLogger{Logger: lgr.Logger}
		}),
		fx.Provide(
			otel.ConfigureTracePropagator,
			otel.ConfigureTraceProvider,
			otel.ConfigureTraceExporter,
			db.ConfigureDatabase,
			kafka.ConfigureKafkaAdmin,
			cqrs.ConfigureCommonProjection,
		),
		fx.Invoke(
			db.RunResetDatabase,
			kafka.RunResetPartitions,
			db.RunMigrations,
			kafka.RunCreateTopic,
			cqrs.SetIsNeedToFastForwardSequences,
			app.Shutdown,
		),
	)
	appFx.Run()
	lgr.Info("Exit reset command")
}
