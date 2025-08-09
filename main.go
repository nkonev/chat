package main

import (
	"fmt"
	"go-cqrs-chat-example/cmd"
	"os"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Printf("No command provided. Expected command one of %v\n", cmd.AllCommands)
		os.Exit(1)
	}

	theCmd := os.Args[1]
	remainingArgs := os.Args[2:]
	switch theCmd {
	case cmd.CommandImportName:
		cmd.RunImport(remainingArgs)
	case cmd.CommandExportName:
		cmd.RunExport(remainingArgs)
	case cmd.CommandResetName:
		cmd.RunReset(remainingArgs)
	case cmd.CommandHelpName:
		cmd.RunHelp(remainingArgs)
	case cmd.CommandServeName:
		cmd.RunServe(remainingArgs)
	default:
		fmt.Printf("Unknown command '%v'. Expected command one of %v\n", theCmd, cmd.AllCommands)
		os.Exit(1)
	}
}
