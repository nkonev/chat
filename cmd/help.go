package cmd

import (
	"fmt"
	"go-cqrs-chat-example/app"
)

const CommandHelpName = "help"

const ExecutableName = app.TRACE_RESOURCE

func RunHelp(args []string) {
	fmt.Printf(`SYNOPSIS:
%s <command> [[%s[=| ]|[%s[=| ]]/path/to/config.yml] [--help|-h] --some.option=overridedValue

Where command is one of %v.

%s or %s or %s or %s should be the first option. 

Examples:

./%s %s %s=./config/config/config-dev.yml --logger.json=true
./%s %s %s ./config/config/config-dev.yml --server.address=:8888

./%s %s %s ./config/config/config-dev.yml --logger.level=debug --postgresql.prettyLog=false --logger.json=true
./%s %s %s=./config/config/config-dev.yml --server.dump=false --http.dump=false --postgresql.dump=false --cqrs.dump=false


To get the particular command's help, use
%s <command> [%s|%s]

Examples:
./%s %s %s

`, ExecutableName, app.ConfigLongPrefix, app.ConfigShortPrefix,
		AllCommands.String(),
		app.ConfigLongPrefix, app.ConfigShortPrefix, app.HelpLongPrefix, app.HelpShortPrefix,
		ExecutableName, CommandServeName, app.ConfigLongPrefix,
		ExecutableName, CommandServeName, app.ConfigLongPrefix,
		ExecutableName, CommandServeName, app.ConfigShortPrefix,
		ExecutableName, CommandServeName, app.ConfigShortPrefix,
		ExecutableName, app.HelpLongPrefix, app.HelpShortPrefix,
		ExecutableName, CommandServeName, app.HelpLongPrefix,
	)
}
