package app

import (
	"fmt"
	"strings"
)

const ConfigLongPrefix = "--config"
const ConfigShortPrefix = "-c"

const HelpLongPrefix = "--help"
const HelpShortPrefix = "-h"

const PseudoFileStdout = "stdout"
const PseudoFileStdin = "stdin"

func IsHelp(args []string) bool {
	var help bool

	if len(args) > 0 && (args[0] == HelpLongPrefix || args[0] == HelpShortPrefix) {
		help = true
	}

	return help
}

func IsConfig(args []string) (bool, string, []string, error) {
	if len(args) > 0 && (strings.HasPrefix(args[0], ConfigLongPrefix) || strings.HasPrefix(args[0], ConfigShortPrefix)) {
		var argsToReadConfig []string

		// load provided config
		stringWithConfig := args[0]
		var thePath = stringWithConfig
		thePath, _ = strings.CutPrefix(thePath, ConfigLongPrefix)
		thePath, _ = strings.CutPrefix(thePath, ConfigShortPrefix)

		if strings.HasPrefix(thePath, "=") {
			thePath, _ = strings.CutPrefix(thePath, "=")
			argsToReadConfig = args[1:]
		} else {
			if len(args) < 2 {
				return false, "", nil, fmt.Errorf("expected file argument")
			}
			thePath = args[1]
			argsToReadConfig = args[2:]
		}

		thePath = strings.TrimSpace(thePath)

		return true, thePath, argsToReadConfig, nil

	} else {
		return false, "", nil, nil
	}
}
