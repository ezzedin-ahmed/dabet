package mod

import "unicode"

// Matcher is an Aho–Corasick automaton over a policy's restricted words
// with WHOLE-TOKEN match semantics: a pattern only counts when both its
// edges fall on token boundaries, so "class" does not match a restricted
// "ass" (the Scunthorpe problem, §7.4). Patterns may be multi-word
// phrases; both patterns and the scanned text must be in Normalize form,
// so inner whitespace is a single space.
//
// The automaton is compiled once per policy and cached alongside the
// policy cache entry (§7.4); Match is read-only and safe for concurrent
// use.
type Matcher struct {
	nodes []acNode
}

type acNode struct {
	next    map[rune]int32
	fail    int32
	outputs []int32 // rune lengths of patterns ending at this node
}

// NewMatcher compiles patterns (normalised internally). Empty patterns are
// dropped; a matcher over zero patterns matches nothing.
func NewMatcher(patterns []string) *Matcher {
	m := &Matcher{nodes: []acNode{{next: map[rune]int32{}}}}
	for _, p := range patterns {
		p = Normalize(p)
		if p == "" {
			continue
		}
		cur := int32(0)
		n := int32(0)
		for _, r := range p {
			n++
			nxt, ok := m.nodes[cur].next[r]
			if !ok {
				m.nodes = append(m.nodes, acNode{next: map[rune]int32{}})
				nxt = int32(len(m.nodes) - 1)
				m.nodes[cur].next[r] = nxt
			}
			cur = nxt
		}
		m.nodes[cur].outputs = append(m.nodes[cur].outputs, n)
	}

	// Breadth-first fail links; outputs are merged down the fail chain so a
	// single state carries every pattern that ends at its position.
	queue := make([]int32, 0, len(m.nodes))
	for _, v := range m.nodes[0].next {
		m.nodes[v].fail = 0
		queue = append(queue, v)
	}
	for len(queue) > 0 {
		u := queue[0]
		queue = queue[1:]
		for r, v := range m.nodes[u].next {
			f := m.nodes[u].fail
			for f != 0 {
				if _, ok := m.nodes[f].next[r]; ok {
					break
				}
				f = m.nodes[f].fail
			}
			if w, ok := m.nodes[f].next[r]; ok && w != v {
				m.nodes[v].fail = w
			} else {
				m.nodes[v].fail = 0
			}
			m.nodes[v].outputs = append(m.nodes[v].outputs, m.nodes[m.nodes[v].fail].outputs...)
			queue = append(queue, v)
		}
	}
	return m
}

// Match reports whether any pattern occurs in normalized as a whole token
// (or whole phrase). normalized must already be in Normalize form.
func (m *Matcher) Match(normalized string) bool {
	runes := []rune(normalized)
	state := int32(0)
	for i, r := range runes {
		for state != 0 {
			if _, ok := m.nodes[state].next[r]; ok {
				break
			}
			state = m.nodes[state].fail
		}
		if nxt, ok := m.nodes[state].next[r]; ok {
			state = nxt
		}
		for _, plen := range m.nodes[state].outputs {
			start := i - int(plen) + 1
			if tokenBoundaryBefore(runes, start) && tokenBoundaryAfter(runes, i) {
				return true
			}
		}
	}
	return false
}

func isTokenRune(r rune) bool { return unicode.IsLetter(r) || unicode.IsDigit(r) }

func tokenBoundaryBefore(rs []rune, start int) bool {
	return start == 0 || !isTokenRune(rs[start-1])
}

func tokenBoundaryAfter(rs []rune, last int) bool {
	return last == len(rs)-1 || !isTokenRune(rs[last+1])
}
