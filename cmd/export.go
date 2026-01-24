package cmd

import (
	"fmt"
	"go-cqrs-chat-example/app"
	"go-cqrs-chat-example/config"
	"go-cqrs-chat-example/kafka"
	"go-cqrs-chat-example/logger"
	"log/slog"
	"os"

	"go.uber.org/fx"
	"go.uber.org/fx/fxevent"
)

const CommandExportName = "export"

func RunExport(args []string) {
	if app.IsHelp(args) {
		fmt.Println(`
Performs export of CQRS Kafka events topic to the json line file.
See cqrs.export.file setting.
		`)

		return
	}

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
