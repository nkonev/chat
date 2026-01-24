package cmd

import (
	"fmt"
	"go-cqrs-chat-example/app"
)

const CommandHelpName = "help"

const executableName = app.TRACE_RESOURCE

func RunHelp(args []string) {
	fmt.Printf(`SYNOPSIS:
%s <command> [[--config[=| ]|[-c[=| ]]/path/to/config.yml] --some.option=overridedValue

Where command is one of %v

Examples:

./%s serve --config=./config/config/config-dev.yml --logger.json=true
./%s serve --config ./config/config/config-dev.yml --server.address=:8888

./%s serve -c ./config/config/config-dev.yml --logger.level=debug --postgresql.prettyLog=false --logger.json=true
./%s -c=./config/config/config-dev.yml --server.dump=false --http.dump=false --postgresql.dump=false --cqrs.dump=false

To get the particular command's help, use
%s <command> [--help|-h]

Examples:
./%s import --help

`, executableName, AllCommands.String(), executableName, executableName, executableName, executableName, executableName, executableName)
}
