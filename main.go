package main

import (
	"fmt"
	"go-cqrs-chat-example/cmd"
	"os"
)

func main() {
	var allCommands = []string{cmd.CommandExportName, cmd.CommandImportName, cmd.CommandResetName, cmd.CommandServeName}

	if len(os.Args) < 2 {
		fmt.Printf("No command provided. Expected command one of %v\n", allCommands)
		os.Exit(1)
	}

	theCmd := os.Args[1]
	switch theCmd {
	case cmd.CommandImportName:
		cmd.RunImport()
	case cmd.CommandExportName:
		cmd.RunExport()
	case cmd.CommandResetName:
		cmd.RunReset()
	case cmd.CommandServeName:
		cmd.RunServe()
	default:
		fmt.Printf("Unknown command '%v'. Expected command one of %v\n", theCmd, allCommands)
		os.Exit(1)
	}
}
