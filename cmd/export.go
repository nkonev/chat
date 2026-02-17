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
	processedArgs, hasHelp := app.IsHelp(args)
	if hasHelp {
		fmt.Printf(`
Performs export of CQRS Kafka events topic to the json line file.
See cqrs.export.file setting. This settings along with /path/to/file.json also accepts a special '%s' pseudofile.
So all the logs are written to stderr.

To export to the file:
./%s %s --cqrs.export.file=/tmp/export.json

or via pipe:
./%s %s --cqrs.export.file=%s > /tmp/export.json

To export to stdout:
./%s %s --cqrs.export.file=%s

`, app.PseudoFileStdout,
			ExecutableName, CommandExportName,
			ExecutableName, CommandExportName, app.PseudoFileStdout,
			ExecutableName, CommandExportName, app.PseudoFileStdout,
		)

		return
	}

	cfg, err := config.CreateTypedConfig(processedArgs)
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
