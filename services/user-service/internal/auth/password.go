// Package auth implements password hashing (Argon2id per A1), password
// policy, opaque token generation, and access-JWT issuance for
// user-service. Per P4, nothing in this package ever logs or returns a
// password or token in an error.
package auth

import (
	"crypto/rand"
	"crypto/subtle"
	_ "embed"
	"encoding/base64"
	"errors"
	"fmt"
	"strings"
	"unicode/utf8"

	"golang.org/x/crypto/argon2"
)

// Argon2id parameters per A1: 64 MB, t=3, p=2.
const (
	argonMemoryKiB = 64 * 1024
	argonTime      = 3
	argonThreads   = 2
	argonSaltLen   = 16
	argonKeyLen    = 32
)

// Password policy per A1: minimum 12 characters, no composition rules,
// rejection against a small embedded common-password list. The maximum
// only bounds hashing cost and is far above any real passphrase.
const (
	PasswordMinLength = 12
	PasswordMaxLength = 512
)

//go:embed common_passwords.txt
var commonPasswordsRaw string

var commonPasswords = func() map[string]struct{} {
	m := make(map[string]struct{})
	for _, line := range strings.Split(commonPasswordsRaw, "\n") {
		line = strings.TrimSpace(line)
		if line != "" {
			m[strings.ToLower(line)] = struct{}{}
		}
	}
	return m
}()

// Password policy violations. The API layer maps these to
// validation_failed; the text never includes the password itself.
var (
	ErrPasswordTooShort = fmt.Errorf("password must be at least %d characters", PasswordMinLength)
	ErrPasswordTooLong  = fmt.Errorf("password must be at most %d characters", PasswordMaxLength)
	ErrPasswordCommon   = errors.New("password is too common")
)

// ValidatePassword enforces the A1 policy.
func ValidatePassword(password string) error {
	switch n := utf8.RuneCountInString(password); {
	case n < PasswordMinLength:
		return ErrPasswordTooShort
	case n > PasswordMaxLength:
		return ErrPasswordTooLong
	}
	if _, ok := commonPasswords[strings.ToLower(password)]; ok {
		return ErrPasswordCommon
	}
	return nil
}

// HashPassword returns a PHC-encoded Argon2id hash of password.
func HashPassword(password string) (string, error) {
	salt := make([]byte, argonSaltLen)
	if _, err := rand.Read(salt); err != nil {
		return "", fmt.Errorf("generate salt: %w", err)
	}
	key := argon2.IDKey([]byte(password), salt, argonTime, argonMemoryKiB, argonThreads, argonKeyLen)
	return fmt.Sprintf("$argon2id$v=%d$m=%d,t=%d,p=%d$%s$%s",
		argon2.Version, argonMemoryKiB, argonTime, argonThreads,
		base64.RawStdEncoding.EncodeToString(salt),
		base64.RawStdEncoding.EncodeToString(key)), nil
}

// VerifyPassword reports whether password matches the PHC-encoded hash,
// comparing in constant time. Errors describe only the stored hash's
// encoding, never the password.
func VerifyPassword(encoded, password string) (bool, error) {
	parts := strings.Split(encoded, "$")
	if len(parts) != 6 || parts[1] != "argon2id" {
		return false, errors.New("malformed password hash")
	}
	var version int
	if _, err := fmt.Sscanf(parts[2], "v=%d", &version); err != nil {
		return false, errors.New("malformed password hash version")
	}
	var memory, time uint32
	var threads uint8
	if _, err := fmt.Sscanf(parts[3], "m=%d,t=%d,p=%d", &memory, &time, &threads); err != nil {
		return false, errors.New("malformed password hash parameters")
	}
	salt, err := base64.RawStdEncoding.DecodeString(parts[4])
	if err != nil {
		return false, errors.New("malformed password hash salt")
	}
	want, err := base64.RawStdEncoding.DecodeString(parts[5])
	if err != nil {
		return false, errors.New("malformed password hash digest")
	}
	got := argon2.IDKey([]byte(password), salt, time, memory, threads, uint32(len(want)))
	return subtle.ConstantTimeCompare(got, want) == 1, nil
}
