package cmd

import "fmt"

const CommandHelpName = "help"

func RunHelp(args []string) {
	fmt.Printf(`SYNOPSIS:
go-cqrs-chat-example <command> [[--config[=| ]|[-c[=| ]]/path/to/config.yml] --some.option=overridedValue

Where command is one of %v

Examples:

./go-cqrs-chat-example serve --config=./config/config/config-dev.yml --logger.json=true
./go-cqrs-chat-example serve --config ./config/config/config-dev.yml --server.address=:8888

./go-cqrs-chat-example serve -c ./config/config/config-dev.yml --logger.level=debug --postgresql.prettyLog=false --logger.json=true
./go-cqrs-chat-example serve -c=./config/config/config-dev.yml --postgresql.dump=false
`, AllCommands)
}
