//go:build tools

package poo

// The backend's tests build the pinned poo command from its module:
//
//	go build github.com/sdougbrown/poo/cmd/poo
//
// That command is not a package this module imports, so the module graph does
// not otherwise carry its dependencies. This file pins the ones the build
// needs beyond what avenor already requires, and go mod tidy keeps it because
// it loads every build tag.
import (
	_ "github.com/JohannesKaufmann/html-to-markdown/v2"
	_ "github.com/charmbracelet/glamour"
	_ "github.com/charmbracelet/x/ansi"
	_ "github.com/reeflective/readline"
	_ "github.com/sdougbrown/cli-registry"
	_ "golang.org/x/term"
)
