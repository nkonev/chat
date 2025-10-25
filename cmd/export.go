package cmd

import (
	"go-cqrs-chat-example/app"
	"go-cqrs-chat-example/config"
	"go-cqrs-chat-example/kafka"
	"go-cqrs-chat-example/logger"
	"go.uber.org/fx"
	"go.uber.org/fx/fxevent"
	"log/slog"
	"os"
)

const CommandExportName = "export"

func RunExport(args []string) {
	cfg, err := config.CreateTypedConfig(args)
	if err != nil {
		panic(err)
	}
	lgr := logger.NewLogger(os.Stderr, cfg)
	defer lgr.CloseLogger()

	lgr.Info("Start export command")

	appFx := fx.New(
		fx.Supply(cfg),
		fx.Supply(lgr),
		fx.WithLogger(func(lgr *logger.LoggerWrapper) fxevent.Logger {
			fsl := &fxevent.SlogLogger{Logger: lgr.Logger}
			fsl.UseLogLevel(slog.LevelDebug)
			return fsl
		}),
		fx.Provide(
			kafka.ConfigureSaramaClient,
		),
		fx.Invoke(
			kafka.Export,
			app.Shutdown,
		),
	)
	appFx.Run()
	lgr.Info("Exit export command")
}
