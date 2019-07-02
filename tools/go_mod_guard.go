package _tools

// This file ensures `go mod tidy` will not delete entries to all tools.

import (
	// golangci-lint is a package-based linter
	_ "github.com/golangci/golangci-lint/pkg/commands"

	// revive is a file-based linter
	_ "github.com/mgechev/revive/lint"

	// failpoint-ctl for enabling and disabling failpoints
	_ "github.com/pingcap/failpoint"

	// vfsgen for embedding HTML resources
	_ "github.com/shurcooL/vfsgen/cmd/vfsgendev"
)
