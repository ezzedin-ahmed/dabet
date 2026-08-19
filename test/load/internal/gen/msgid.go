package gen

import (
	"strconv"
	"strings"
	"time"
)

// MessageIDPrefix is what every harness-minted message_id starts with.
const MessageIDPrefix = "ld"

// MintMessageID builds the opaque message_id for one generated record.
//
// The ideal-clock send time is carried IN the id, base-36 nanoseconds.
// This is not decoration: flagged.v1 carries flagged_at but not
// ingested_at (§4.2), so a consumer of the verdict topic has no way to
// compute the §4.6 SLI. Round-tripping the intended send time through
// the id — which the harness mints and only the harness parses, so P5
// is not violated — lets the run cross-check
// moderation_e2e_latency_seconds against the events themselves at full
// resolution.
//
// Layout: ld<run>-<nanos36>-<shard36>-<seq36>. With a 6-char run id
// this stays around 30 characters, well inside the 64-char cap of §4.2.
func MintMessageID(runID string, shard int, seq uint64, at time.Time) string {
	var b strings.Builder
	b.Grow(40)
	b.WriteString(MessageIDPrefix)
	b.WriteString(runID)
	b.WriteByte('-')
	b.WriteString(strconv.FormatInt(at.UnixNano(), 36))
	b.WriteByte('-')
	b.WriteString(strconv.FormatInt(int64(shard), 36))
	b.WriteByte('-')
	b.WriteString(strconv.FormatUint(seq, 36))
	return b.String()
}

// RunPrefix is what a tailer matches on to keep a run's verdicts apart
// from a previous run's, still on the topic under retention.
func RunPrefix(runID string) string { return MessageIDPrefix + runID + "-" }

// DecodeIntendedSend recovers the ideal-clock send time from a
// harness-minted message_id. ok is false for anything this harness did
// not mint.
func DecodeIntendedSend(messageID string) (time.Time, bool) {
	rest, ok := strings.CutPrefix(messageID, MessageIDPrefix)
	if !ok {
		return time.Time{}, false
	}
	_, after, ok := strings.Cut(rest, "-")
	if !ok {
		return time.Time{}, false
	}
	nanos, _, ok := strings.Cut(after, "-")
	if !ok {
		return time.Time{}, false
	}
	n, err := strconv.ParseInt(nanos, 36, 64)
	if err != nil || n <= 0 {
		return time.Time{}, false
	}
	return time.Unix(0, n), true
}
