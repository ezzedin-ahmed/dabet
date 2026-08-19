package driver

import (
	"context"
	"errors"
	"testing"
)

func TestFromHTTPStatus(t *testing.T) {
	cases := []struct {
		status int
		want   error
	}{
		{200, nil},
		{204, nil},
		{404, ErrNotFound},
		{410, ErrGone},
		{429, ErrRateLimited},
		{401, ErrUnauthorized},
	}
	for _, c := range cases {
		if got := FromHTTPStatus(c.status); !errors.Is(got, c.want) {
			t.Errorf("FromHTTPStatus(%d) = %v, want %v", c.status, got, c.want)
		}
	}
	for _, status := range []int{500, 502, 503, 400} {
		err := FromHTTPStatus(status)
		if err == nil {
			t.Errorf("FromHTTPStatus(%d) = nil, want transient error", status)
			continue
		}
		for _, terminal := range []error{ErrNotFound, ErrGone, ErrRateLimited, ErrUnauthorized, ErrNotImplemented} {
			if errors.Is(err, terminal) {
				t.Errorf("FromHTTPStatus(%d) classified as %v, want plain transient", status, terminal)
			}
		}
	}
}

type stub struct{ name string }

func (s stub) Platform() string { return s.name }
func (s stub) Watch(context.Context, Connection, chan<- Message) error {
	return ErrNotImplemented
}
func (s stub) Delete(context.Context, Connection, string, string) error { return nil }
func (s stub) DiscoverLive(context.Context, Connection) ([]ContentRef, error) {
	return nil, ErrNotImplemented
}

func TestRegistry(t *testing.T) {
	r := NewRegistry()
	r.Register(stub{"twitch"})
	r.Register(stub{"youtube"})

	if d, ok := r.Get("twitch"); !ok || d.Platform() != "twitch" {
		t.Errorf("Get(twitch) = %v, %v", d, ok)
	}
	if _, ok := r.Get("myspace"); ok {
		t.Error("Get(myspace) should miss")
	}
	got := r.Platforms()
	if len(got) != 2 || got[0] != "twitch" || got[1] != "youtube" {
		t.Errorf("Platforms() = %v", got)
	}

	defer func() {
		if recover() == nil {
			t.Error("duplicate Register should panic")
		}
	}()
	r.Register(stub{"twitch"})
}

var _ Driver = stub{} // interface conformance per §7.2
