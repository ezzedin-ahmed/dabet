package opaque

import (
	"fmt"
	"math/big"
	"strings"
)

// base62 alphabet, sorted ASCII so encodings are stable and URL-safe.
const alphabet = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"

var base62 = big.NewInt(62)

// encodeBase62 encodes raw bytes as base62. Leading zero bytes are
// preserved as leading '0' characters (the zero digit), one per byte, so
// decode is an exact inverse.
func encodeBase62(b []byte) string {
	zeros := 0
	for zeros < len(b) && b[zeros] == 0 {
		zeros++
	}
	var sb strings.Builder
	for range zeros {
		sb.WriteByte(alphabet[0])
	}
	n := new(big.Int).SetBytes(b[zeros:])
	if n.Sign() == 0 {
		return sb.String()
	}
	var digits []byte
	rem := new(big.Int)
	for n.Sign() > 0 {
		n.DivMod(n, base62, rem)
		digits = append(digits, alphabet[rem.Int64()])
	}
	for i := len(digits) - 1; i >= 0; i-- {
		sb.WriteByte(digits[i])
	}
	return sb.String()
}

// decodeBase62 is the inverse of encodeBase62.
func decodeBase62(s string) ([]byte, error) {
	zeros := 0
	for zeros < len(s) && s[zeros] == alphabet[0] {
		zeros++
	}
	n := new(big.Int)
	for _, c := range []byte(s[zeros:]) {
		v := strings.IndexByte(alphabet, c)
		if v < 0 {
			return nil, fmt.Errorf("opaque: invalid base62 character %q", c)
		}
		n.Mul(n, base62)
		n.Add(n, big.NewInt(int64(v)))
	}
	out := make([]byte, zeros)
	if n.Sign() > 0 {
		out = append(out, n.Bytes()...)
	}
	return out, nil
}
