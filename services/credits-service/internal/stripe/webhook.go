package stripe

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strconv"
	"strings"
	"time"
)

// DefaultTolerance is the maximum accepted webhook timestamp skew (§5.7).
const DefaultTolerance = 5 * time.Minute

// VerifySignature checks a Stripe-Signature header against the raw
// payload using Stripe's scheme: the header carries `t=<unix>,v1=<hex>`
// pairs and v1 is HMAC-SHA256(secret, "<t>.<payload>"). Any one matching
// v1 passes (Stripe sends several during secret rolls). A timestamp
// further than tolerance from now is rejected to bound replay.
func VerifySignature(payload []byte, header string, secret []byte, now time.Time, tolerance time.Duration) error {
	if header == "" {
		return fmt.Errorf("missing Stripe-Signature header")
	}
	var (
		ts   string
		sigs [][]byte
	)
	for _, part := range strings.Split(header, ",") {
		k, v, ok := strings.Cut(strings.TrimSpace(part), "=")
		if !ok {
			continue
		}
		switch k {
		case "t":
			ts = v
		case "v1":
			sig, err := hex.DecodeString(v)
			if err == nil {
				sigs = append(sigs, sig)
			}
		}
	}
	if ts == "" {
		return fmt.Errorf("signature header has no timestamp")
	}
	if len(sigs) == 0 {
		return fmt.Errorf("signature header has no v1 signature")
	}
	unix, err := strconv.ParseInt(ts, 10, 64)
	if err != nil {
		return fmt.Errorf("signature header has a malformed timestamp")
	}
	if d := now.Sub(time.Unix(unix, 0)); d > tolerance || d < -tolerance {
		return fmt.Errorf("signature timestamp outside tolerance")
	}

	expected := signature(payload, secret, ts)
	for _, sig := range sigs {
		if hmac.Equal(sig, expected) {
			return nil
		}
	}
	return fmt.Errorf("no matching v1 signature")
}

// Sign produces a valid Stripe-Signature header for payload at time t.
// Used by tests and the local e2e stub; Stripe itself does the same math.
func Sign(payload []byte, secret []byte, t time.Time) string {
	ts := strconv.FormatInt(t.Unix(), 10)
	return "t=" + ts + ",v1=" + hex.EncodeToString(signature(payload, secret, ts))
}

// signature is HMAC-SHA256(secret, "<ts>.<payload>").
func signature(payload, secret []byte, ts string) []byte {
	mac := hmac.New(sha256.New, secret)
	mac.Write([]byte(ts))
	mac.Write([]byte("."))
	mac.Write(payload)
	return mac.Sum(nil)
}
