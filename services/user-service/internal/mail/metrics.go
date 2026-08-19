package mail

import "github.com/prometheus/client_golang/prometheus"

// Outcome label values of emails_sent_total. The set is closed, which is
// what keeps the metric's cardinality at (templates × outcomes) — §4.5
// forbids labelling by address, creator id, or any other unbounded key.
const (
	OutcomeSent    = "sent"    // accepted by the SMTP server
	OutcomeFailed  = "failed"  // render, lookup, or every attempt failed
	OutcomeDropped = "dropped" // queue full; never enqueued
	OutcomeLogged  = "logged"  // mailer disabled: logged instead of sent
)

type metrics struct {
	sent  *prometheus.CounterVec
	depth prometheus.GaugeFunc
}

// newMetrics registers the mailer's metrics on reg. depth reports the
// current queue occupancy. reg may be nil (tests), in which case the
// metrics exist but are not exported.
func newMetrics(reg prometheus.Registerer, depth func() float64) *metrics {
	m := &metrics{
		sent: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "emails_sent_total",
			Help: "Outbound emails by template and outcome.",
		}, []string{"template", "outcome"}),
		depth: prometheus.NewGaugeFunc(prometheus.GaugeOpts{
			Name: "email_queue_depth",
			Help: "Emails waiting in the bounded send queue.",
		}, depth),
	}
	if reg != nil {
		reg.MustRegister(m.sent, m.depth)
	}
	return m
}
