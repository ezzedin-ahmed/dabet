package sink

import (
	"context"
	"encoding/json"

	"dabet/test/load/internal/gen"
)

// Null is the self-benchmark sink. It does everything the Kafka sink
// does up to and including JSON-encoding the record, then throws it
// away — so a run against it measures the generator's own ceiling
// (population sampling, text rendering, key derivation, encoding, and
// the ideal-clock scheduler) with no broker and no consumers in the
// picture.
//
// Every real run reports its send-lag distribution against this
// number. If the generator's ceiling is not comfortably above the
// offered rate, the run measured the generator, not Dabet.
type Null struct {
	Counters
	// Encode keeps the JSON marshal in the measurement, which is the
	// honest comparison against the Kafka sink. Setting it false
	// measures the population sampler alone.
	Encode bool
}

// NewNull builds a self-benchmark sink.
func NewNull(encode bool) *Null { return &Null{Encode: encode} }

// Send counts the record and discards it.
func (n *Null) Send(_ context.Context, rec gen.Record) error {
	n.accepted.Add(1)
	if n.Encode {
		val, err := json.Marshal(rec.Msg)
		if err != nil {
			n.failed.Add(1)
			return err
		}
		n.bytes.Add(int64(len(val) + len(rec.Key)))
	}
	n.acked.Add(1)
	return nil
}

// Flush is a no-op.
func (n *Null) Flush(context.Context) error { return nil }

// Close is a no-op.
func (n *Null) Close() {}
