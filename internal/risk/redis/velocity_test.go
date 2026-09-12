package redis

import (
	"context"
	"errors"
	"testing"
	"time"

	redisclient "github.com/redis/go-redis/v9"
)

func TestObserverObserve(t *testing.T) {
	t.Parallel()
	client := &fakeCommandClient{command: redisclient.NewCmdResult(int64(2), nil)}
	observer, err := newObserver(client, 5*time.Minute)
	if err != nil {
		t.Fatalf("newObserver() error = %v", err)
	}
	observedAt := time.Date(2026, 9, 12, 12, 0, 0, 0, time.FixedZone("UTC+3", 3*60*60))
	count, err := observer.Observe(t.Context(), "source-account", "transfer-id", observedAt)
	if err != nil {
		t.Fatalf("Observe() error = %v", err)
	}
	if count != 2 {
		t.Errorf("Observe() count = %d, want 2", count)
	}
	if client.script != velocityScript || len(client.keys) != 1 || client.keys[0] != velocityKeyPrefix+"source-account" {
		t.Errorf("Eval() = (%q, %v), want velocity script and source account key", client.script, client.keys)
	}
	if len(client.args) != 4 || client.args[0] != observedAt.UTC().Add(-5*time.Minute).UnixMilli() ||
		client.args[1] != observedAt.UTC().UnixMilli() || client.args[2] != "transfer-id" || client.args[3] != int64((5*time.Minute).Milliseconds()) {
		t.Errorf("Eval() arguments = %#v, want cutoff, observed time, transfer ID, and window milliseconds", client.args)
	}
}

func TestObserverObserveRejectsInvalidInput(t *testing.T) {
	t.Parallel()
	observer, err := newObserver(&fakeCommandClient{command: redisclient.NewCmdResult(int64(1), nil)}, time.Minute)
	if err != nil {
		t.Fatalf("newObserver() error = %v", err)
	}
	if _, err := observer.Observe(t.Context(), "", "transfer-id", time.Now()); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("Observe() error = %v, want ErrInvalidArgument", err)
	}
}

func TestObserverObservePropagatesRedisError(t *testing.T) {
	t.Parallel()
	want := errors.New("redis unavailable")
	observer, err := newObserver(&fakeCommandClient{command: redisclient.NewCmdResult(nil, want)}, time.Minute)
	if err != nil {
		t.Fatalf("newObserver() error = %v", err)
	}
	if _, err := observer.Observe(t.Context(), "source-account", "transfer-id", time.Now()); !errors.Is(err, want) {
		t.Fatalf("Observe() error = %v, want redis error", err)
	}
}

func TestNewObserverRejectsInvalidConfiguration(t *testing.T) {
	t.Parallel()
	if _, err := NewObserver("", time.Minute); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("NewObserver() error = %v, want ErrInvalidArgument", err)
	}
	if _, err := NewObserver("redis:6379", time.Nanosecond); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("NewObserver() error = %v, want ErrInvalidArgument", err)
	}
}

type fakeCommandClient struct {
	command *redisclient.Cmd
	script  string
	keys    []string
	args    []any
}

func (c *fakeCommandClient) Eval(_ context.Context, script string, keys []string, args ...any) *redisclient.Cmd {
	c.script = script
	c.keys = append([]string(nil), keys...)
	c.args = append([]any(nil), args...)
	return c.command
}

func (c *fakeCommandClient) Close() error { return nil }
