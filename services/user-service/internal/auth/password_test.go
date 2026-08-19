package auth

import (
	"errors"
	"strings"
	"testing"
)

func TestValidatePassword(t *testing.T) {
	tests := []struct {
		name     string
		password string
		wantErr  error
	}{
		{"ok long passphrase", "battery horse staple purple", nil},
		{"exactly 12 chars", "zxqvbnmkjhgf", nil},
		{"too short", "elevenchars", ErrPasswordTooShort},
		{"empty", "", ErrPasswordTooShort},
		{"too long", strings.Repeat("a", PasswordMaxLength+1), ErrPasswordTooLong},
		{"common", "password12345", ErrPasswordCommon},
		{"common case-insensitive", "PASSWORD12345", ErrPasswordCommon},
		{"no composition rules", "aaaaaaaaaaaa", nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidatePassword(tt.password)
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("ValidatePassword(...) = %v, want %v", err, tt.wantErr)
			}
		})
	}
}

func TestHashVerifyRoundtrip(t *testing.T) {
	const password = "a perfectly fine passphrase"
	hash, err := HashPassword(password)
	if err != nil {
		t.Fatalf("HashPassword: %v", err)
	}
	if !strings.HasPrefix(hash, "$argon2id$") {
		t.Fatalf("hash is not PHC argon2id encoded")
	}
	if strings.Contains(hash, password) {
		t.Fatalf("hash contains the plaintext password")
	}

	ok, err := VerifyPassword(hash, password)
	if err != nil || !ok {
		t.Fatalf("VerifyPassword(correct) = %v, %v; want true, nil", ok, err)
	}
	ok, err = VerifyPassword(hash, "the wrong passphrase")
	if err != nil || ok {
		t.Fatalf("VerifyPassword(wrong) = %v, %v; want false, nil", ok, err)
	}
	if _, err := VerifyPassword("not-a-phc-string", password); err == nil {
		t.Fatalf("VerifyPassword on malformed hash should error")
	}
}

func TestNewOpaqueToken(t *testing.T) {
	raw, hash, err := NewOpaqueToken()
	if err != nil {
		t.Fatalf("NewOpaqueToken: %v", err)
	}
	if raw == "" || hash == "" || raw == hash {
		t.Fatalf("token or hash malformed")
	}
	if HashToken(raw) != hash {
		t.Fatalf("hash does not match HashToken(raw)")
	}
	raw2, _, _ := NewOpaqueToken()
	if raw == raw2 {
		t.Fatalf("tokens are not random")
	}
}
