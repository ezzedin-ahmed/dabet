// Package obs provides the standard observability surface every Dabet
// service exposes: the Prometheus metrics of docs §4.5, health endpoints,
// and slog JSON logging.
//
// # Cardinality rule (P4, docs §4.5)
//
// NEVER label a metric with message text, message_id, author_id, or
// content_id. creator_id is permitted only on credits_* metrics. Everything
// else labels by service, platform driver, detector, and outcome. Violating
// this leaks radioactive data into the metrics store and explodes series
// cardinality.
package obs
