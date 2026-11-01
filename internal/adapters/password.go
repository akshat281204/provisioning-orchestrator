package adapters

import (
	"crypto/rand"
	"fmt"
	"math/big"
)

// passwordLen bounds generated passwords: long enough to satisfy any
// reasonable policy, short enough for logs-never-write-you-anyway policies.
const passwordLen = 32

// passwordCharsetName is the alphabet of every generated password: only
// identifier-safe characters, so the Postgres DDL literal built around it
// needs no exotic escaping and the login line stays unquoted either way.
const passwordCharsetName = "abcdefghijkmnopqrstuvwxyzABCDEFGHJKLMNPQRSTUVWXYZ23456789"

// NewPassword produces a random alphanumeric password from crypto/rand.
// Only identifier-safe characters are used, so the Postgres DDL literal
// built around it needs no exotic escaping.
func NewPassword() (string, error) {
	out := make([]byte, passwordLen)
	for i := range out {
		n, err := rand.Int(rand.Reader, big.NewInt(int64(len(passwordCharsetName))))
		if err != nil {
			return "", fmt.Errorf("adapters: generate password: %w", err)
		}
		out[i] = passwordCharsetName[n.Int64()]
	}
	return string(out), nil
}
