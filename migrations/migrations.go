// Package migrations embeds the .sql files inside the Go binary,
// so the Docker image needs no extra files.
package migrations

import "embed"

//go:embed *.sql
var FS embed.FS
