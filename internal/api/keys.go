package api

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
)

// keyPrefix makes our keys easy to recognize, e.g. in leak scanners.
const keyPrefix = "gk_"

// generateKey creates a new random API key.
// It returns the raw key (shown to the customer once) and its hash (saved in the database).
func generateKey() (raw string, hash string, err error) {
	b := make([]byte, 32) // 32 random bytes = 256 bits, impossible to guess
	if _, err := rand.Read(b); err != nil {
		return "", "", err
	}
	raw = keyPrefix + hex.EncodeToString(b)
	return raw, hashKey(raw), nil
}

// hashKey returns the SHA-256 hash of a key as text.
// The same key always gives the same hash, so we can look it up in the database.
func hashKey(raw string) string {
	sum := sha256.Sum256([]byte(raw))
	return hex.EncodeToString(sum[:])
}
