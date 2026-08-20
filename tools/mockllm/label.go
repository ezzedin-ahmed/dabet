package main

import (
	"regexp"
	"sort"
	"strings"
)

// This file adds the second thing the real vLLM fleet is asked to do:
// name a cluster of semantically similar messages (§8.6). clusters-job's
// VLLMLabeler sends the §8.6 system prompt and a user message beginning
// "Sample messages:", and expects {"label","description"} JSON back. Without
// this, every request got the moderation verdict shape, the label parse
// failed, and every topic fell back to a generic name — so the labelling half
// of §8.6 was never exercised, and `GET /v1/topics` could only ever return
// placeholder labels.
//
// The two request kinds are distinguished by their prompt: moderation uses a
// "Messages:" line (§7.9), labelling uses "Sample messages:".

// labelBody is the response shape clusters-job parses.
type labelBody struct {
	Label       string `json:"label"`
	Description string `json:"description"`
}

// labelMarker lets a test pin the exact label a cluster receives, the same way
// FLAGME pins a moderation verdict: any sample carrying [[label:Ticket
// resale]] votes for that label.
var labelMarker = regexp.MustCompile(`\[\[label:([^\]\[]+)\]\]`)

// markerStripper removes both the label marker and mockembed's similarity
// marker, so the frequency fallback reads the human wording rather than the
// fixture scaffolding.
var markerStripper = regexp.MustCompile(`\[\[(?:label|sim):[^\]\[]*\]\]`)

// sampleLine matches the "1. text" entries VLLMLabeler emits.
var sampleLine = regexp.MustCompile(`^\s*(\d+)\.\s?(.*)$`)

// isLabelRequest reports whether the prompt is a §8.6 labelling request.
func isLabelRequest(prompt string) bool {
	for _, line := range strings.Split(prompt, "\n") {
		if strings.HasPrefix(strings.TrimSpace(line), "Sample messages:") {
			return true
		}
	}
	return false
}

// samplesFrom extracts the numbered sample list.
func samplesFrom(prompt string) []string {
	var out []string
	inSamples := false
	for _, line := range strings.Split(prompt, "\n") {
		trimmed := strings.TrimSpace(line)
		if strings.HasPrefix(trimmed, "Sample messages:") {
			inSamples = true
			continue
		}
		if !inSamples {
			continue
		}
		if m := sampleLine.FindStringSubmatch(line); m != nil {
			out = append(out, m[2])
		}
	}
	return out
}

// stopWords are too common in chat to name a cluster after.
var stopWords = map[string]bool{
	"about": true, "after": true, "again": true, "anyone": true, "been": true,
	"before": true, "could": true, "does": true, "from": true, "have": true,
	"here": true, "just": true, "like": true, "message": true, "number": true,
	"really": true, "some": true, "that": true, "them": true, "then": true,
	"there": true, "they": true, "this": true, "very": true, "what": true,
	"when": true, "with": true, "would": true, "your": true,
}

var wordRe = regexp.MustCompile(`[a-z][a-z'-]*`)

// commonTerms returns the most frequent meaningful words across samples,
// most frequent first, ties broken alphabetically so the result is stable.
func commonTerms(samples []string, n int) []string {
	freq := map[string]int{}
	for _, s := range samples {
		seen := map[string]bool{}
		for _, w := range wordRe.FindAllString(strings.ToLower(markerStripper.ReplaceAllString(s, " ")), -1) {
			if len(w) < 4 || stopWords[w] || seen[w] {
				continue
			}
			seen[w] = true
			freq[w]++
		}
	}
	terms := make([]string, 0, len(freq))
	for w := range freq {
		terms = append(terms, w)
	}
	sort.Slice(terms, func(i, j int) bool {
		if freq[terms[i]] != freq[terms[j]] {
			return freq[terms[i]] > freq[terms[j]]
		}
		return terms[i] < terms[j]
	})
	if len(terms) > n {
		terms = terms[:n]
	}
	return terms
}

// label names a cluster deterministically. An explicit [[label:…]] marker
// wins by plurality; otherwise the two most common words are used; a cluster
// with nothing in common is named "Unlabelled cluster", which is what a real
// model would also be reduced to.
func label(prompt string) labelBody {
	samples := samplesFrom(prompt)

	votes := map[string]int{}
	for _, s := range samples {
		for _, m := range labelMarker.FindAllStringSubmatch(s, -1) {
			votes[strings.TrimSpace(m[1])]++
		}
	}
	if len(votes) > 0 {
		best, bestN := "", -1
		for v, n := range votes {
			if n > bestN || (n == bestN && v < best) {
				best, bestN = v, n
			}
		}
		return labelBody{
			Label:       best,
			Description: "Viewers talking about " + strings.ToLower(best) + ".",
		}
	}

	terms := commonTerms(samples, 2)
	if len(terms) == 0 {
		return labelBody{
			Label:       "Unlabelled cluster",
			Description: "The sample messages have no common subject.",
		}
	}
	name := strings.Join(terms, " ")
	return labelBody{
		Label:       strings.ToUpper(name[:1]) + name[1:],
		Description: "Viewers talking about " + name + ".",
	}
}
