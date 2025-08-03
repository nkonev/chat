package cmd

import (
	"go-cqrs-chat-example/app"
	"go-cqrs-chat-example/config"
	"go-cqrs-chat-example/kafka"
	"go-cqrs-chat-example/logger"
	"go.uber.org/fx"
	"go.uber.org/fx/fxevent"
	"os"
)

const CommandExportName = "export"

func RunExport() {
	cfg, err := config.CreateTypedConfig()
	if err != nil {
		panic(err)
	}
	baseLogger := logger.NewBaseLogger(os.Stderr, cfg)
	lgr := logger.NewLogger(baseLogger)

	lgr.Info("Start export command")

	appFx := fx.New(
		fx.Supply(cfg),
		fx.Supply(lgr),
		fx.WithLogger(func(lgr *logger.LoggerWrapper) fxevent.Logger {
			return &fxevent.SlogLogger{Logger: lgr.Logger}
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
