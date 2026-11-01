// Package migrations embeds the ordered SQL migration files so the binary and
// the migration tool carry them without any runtime file dependency.
package migrations

import "embed"

// FS holds every *.sql file in this directory, embedded at compile time.
//
//go:embed *.sql
var FS embed.FS
