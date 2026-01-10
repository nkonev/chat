package cmd

import (
	"go-cqrs-chat-example/app"
	"go-cqrs-chat-example/config"
	"go-cqrs-chat-example/cqrs"
	"go-cqrs-chat-example/db"
	"go-cqrs-chat-example/kafka"
	"go-cqrs-chat-example/logger"
	"go-cqrs-chat-example/otel"
	"go-cqrs-chat-example/sanitizer"
	"log/slog"
	"os"

	"go.uber.org/fx"
	"go.uber.org/fx/fxevent"
)

const CommandImportName = "import"

func RunImport(args []string) {
	cfg, err := config.CreateTypedConfig(args)
	if err != nil {
		panic(err)
	}
	lgr := logger.NewLogger(os.Stdout, cfg)
	defer lgr.CloseLogger()

	lgr.Info("Start import command")

	appFx := fx.New(
		fx.Supply(cfg),
		fx.Supply(lgr),
		fx.WithLogger(func(lgr *logger.LoggerWrapper) fxevent.Logger {
			fsl := &fxevent.SlogLogger{Logger: lgr.Logger}
			fsl.UseLogLevel(slog.LevelDebug)
			return fsl
		}),
		fx.Provide(
			otel.ConfigureTracePropagator,
			otel.ConfigureTraceProvider,
			otel.ConfigureTraceExporter,
			db.ConfigureDatabase,
			kafka.ConfigureKafkaAdmin,
			cqrs.ConfigureCommonProjection,
			sanitizer.CreateStripTags,
		),
		fx.Invoke(
			db.RunMigrations,
			kafka.RunCreateTopicChat,
			kafka.RunCreateTopicUser,
			kafka.Import,
			cqrs.SetIsNeedToFastForwardSequences,
			app.Shutdown,
		),
	)
	appFx.Run()
	lgr.Info("Exit import command")
}
