// Package promx parses the Prometheus text exposition format and does
// the arithmetic a load run needs on top of it: counter deltas between
// two scrapes, and quantiles out of the classic cumulative histograms
// that moderation-service exports.
//
// It exists because the harness must not add metrics to the services
// (that would change what it is measuring) and there is no Prometheus
// server in the local stack — /metrics scrape-and-diff is the whole
// measurement channel.
package promx

import (
	"bufio"
	"fmt"
	"io"
	"maps"
	"math"
	"sort"
	"strconv"
	"strings"
	"time"
)

// Sample is one time series line.
type Sample struct {
	Name   string            `json:"name"`
	Labels map[string]string `json:"labels,omitempty"`
	Value  float64           `json:"value"`
}

// Snapshot is one /metrics scrape.
type Snapshot struct {
	Target  string            `json:"target"`
	At      time.Time         `json:"at"`
	Samples []Sample          `json:"samples"`
	Types   map[string]string `json:"types,omitempty"` // metric family -> counter|gauge|histogram|summary
}

// Parse reads the exposition format. Unparseable value lines are
// skipped rather than failing the scrape: a single malformed series must
// not cost the run its whole measurement.
func Parse(target string, at time.Time, r io.Reader) (*Snapshot, error) {
	snap := &Snapshot{Target: target, At: at, Types: map[string]string{}}
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 0, 64<<10), 8<<20)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" {
			continue
		}
		if strings.HasPrefix(line, "#") {
			fields := strings.Fields(line)
			if len(fields) >= 4 && fields[1] == "TYPE" {
				snap.Types[fields[2]] = fields[3]
			}
			continue
		}
		s, ok := parseSample(line)
		if !ok {
			continue
		}
		snap.Samples = append(snap.Samples, s)
	}
	if err := sc.Err(); err != nil {
		return nil, fmt.Errorf("read metrics from %s: %w", target, err)
	}
	return snap, nil
}

// parseSample decodes one `name{labels} value [timestamp]` line.
func parseSample(line string) (Sample, bool) {
	var s Sample
	name, rest := line, ""
	if i := strings.IndexAny(line, "{ "); i >= 0 {
		name, rest = line[:i], line[i:]
	} else {
		return s, false
	}
	s.Name = name
	rest = strings.TrimSpace(rest)
	if strings.HasPrefix(rest, "{") {
		end := closingBrace(rest)
		if end < 0 {
			return s, false
		}
		s.Labels = parseLabels(rest[1:end])
		rest = strings.TrimSpace(rest[end+1:])
	}
	fields := strings.Fields(rest)
	if len(fields) == 0 {
		return s, false
	}
	v, err := parseValue(fields[0])
	if err != nil {
		return s, false
	}
	s.Value = v
	return s, true
}

// parseValue handles the exposition format's Nan/+Inf spellings, which
// strconv does not accept verbatim in every case.
func parseValue(f string) (float64, error) {
	switch f {
	case "NaN", "Nan", "nan":
		return math.NaN(), nil
	case "+Inf", "Inf", "inf", "+inf":
		return math.Inf(1), nil
	case "-Inf", "-inf":
		return math.Inf(-1), nil
	}
	return strconv.ParseFloat(f, 64)
}

// closingBrace finds the `}` that closes the label set, ignoring braces
// inside quoted label values.
func closingBrace(s string) int {
	inQuote, escaped := false, false
	for i := range len(s) {
		c := s[i]
		switch {
		case escaped:
			escaped = false
		case c == '\\' && inQuote:
			escaped = true
		case c == '"':
			inQuote = !inQuote
		case c == '}' && !inQuote:
			return i
		}
	}
	return -1
}

// parseLabels decodes `a="1",b="2"`, honouring the format's \\ \" \n
// escapes so a label value containing a comma or a quote survives.
func parseLabels(s string) map[string]string {
	out := map[string]string{}
	i := 0
	for i < len(s) {
		for i < len(s) && (s[i] == ',' || s[i] == ' ') {
			i++
		}
		eq := strings.IndexByte(s[i:], '=')
		if eq < 0 {
			break
		}
		key := strings.TrimSpace(s[i : i+eq])
		i += eq + 1
		for i < len(s) && s[i] == ' ' {
			i++
		}
		if i >= len(s) || s[i] != '"' {
			break
		}
		i++
		var val strings.Builder
		for i < len(s) {
			c := s[i]
			if c == '\\' && i+1 < len(s) {
				switch s[i+1] {
				case 'n':
					val.WriteByte('\n')
				case '\\':
					val.WriteByte('\\')
				case '"':
					val.WriteByte('"')
				default:
					val.WriteByte(s[i+1])
				}
				i += 2
				continue
			}
			if c == '"' {
				i++
				break
			}
			val.WriteByte(c)
			i++
		}
		if key != "" {
			out[key] = val.String()
		}
	}
	return out
}

// matches reports whether sample labels satisfy a (partial) selector.
func matches(labels, sel map[string]string) bool {
	for k, v := range sel {
		if labels[k] != v {
			return false
		}
	}
	return true
}

// Sum totals every series of name matching the selector.
func (s *Snapshot) Sum(name string, sel map[string]string) float64 {
	total := 0.0
	for _, x := range s.Samples {
		if x.Name == name && matches(x.Labels, sel) {
			total += x.Value
		}
	}
	return total
}

// Series returns every sample of name matching the selector.
func (s *Snapshot) Series(name string, sel map[string]string) []Sample {
	var out []Sample
	for _, x := range s.Samples {
		if x.Name == name && matches(x.Labels, sel) {
			out = append(out, x)
		}
	}
	return out
}

// ByLabel sums name grouped by one label value — the shape every
// distribution in the report needs (detector hits by detector, fail
// opens by component, lag by partition).
func (s *Snapshot) ByLabel(name, label string, sel map[string]string) map[string]float64 {
	out := map[string]float64{}
	for _, x := range s.Samples {
		if x.Name == name && matches(x.Labels, sel) {
			out[x.Labels[label]] += x.Value
		}
	}
	return out
}

// Has reports whether any series of that family was exported at all.
// A metric that is declared but never observed exports nothing, which
// is a materially different finding from one that reads zero.
func (s *Snapshot) Has(name string) bool {
	for _, x := range s.Samples {
		if x.Name == name || strings.HasPrefix(x.Name, name+"_") {
			return true
		}
	}
	return false
}

// Delta subtracts a from b for counter-like families (name suffixes
// _total, _count, _sum, _bucket) and passes gauges through from b, so
// the result describes only what happened between the two scrapes.
//
// A counter that went backwards means the process restarted mid-run;
// the delta is then b's own value, and Restarted reports it so the run
// can be marked untrustworthy rather than quietly wrong.
func Delta(a, b *Snapshot) (*Snapshot, bool) {
	prior := make(map[string]float64, len(a.Samples))
	for _, x := range a.Samples {
		prior[seriesKey(x)] = x.Value
	}
	out := &Snapshot{Target: b.Target, At: b.At, Types: map[string]string{}}
	maps.Copy(out.Types, b.Types)
	restarted := false
	for _, x := range b.Samples {
		if isCounterLike(x.Name) {
			was, ok := prior[seriesKey(x)]
			if ok && x.Value < was-1e-9 {
				restarted = true
			} else if ok {
				x.Value -= was
			}
		}
		out.Samples = append(out.Samples, x)
	}
	return out, restarted
}

func isCounterLike(name string) bool {
	return strings.HasSuffix(name, "_total") ||
		strings.HasSuffix(name, "_count") ||
		strings.HasSuffix(name, "_sum") ||
		strings.HasSuffix(name, "_bucket")
}

func seriesKey(s Sample) string {
	if len(s.Labels) == 0 {
		return s.Name
	}
	keys := make([]string, 0, len(s.Labels))
	for k := range s.Labels {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var b strings.Builder
	b.WriteString(s.Name)
	for _, k := range keys {
		b.WriteByte(0)
		b.WriteString(k)
		b.WriteByte('=')
		b.WriteString(s.Labels[k])
	}
	return b.String()
}
