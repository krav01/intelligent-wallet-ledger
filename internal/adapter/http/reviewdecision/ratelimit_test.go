package reviewdecision

import (
	"errors"
	"testing"
	"time"
)

func TestNewPrincipalRateLimiterRejectsInvalidConfiguration(t *testing.T) {
	for _, test := range []struct {
		name      string
		perMinute int
		now       func() time.Time
	}{
		{name: "zero rate", now: time.Now},
		{name: "rate above maximum", perMinute: maxRateLimitPerMinute + 1, now: time.Now},
		{name: "missing clock", perMinute: 1},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, err := NewPrincipalRateLimiter(test.perMinute, test.now)
			if !errors.Is(err, ErrInvalidRateLimit) {
				t.Errorf("NewPrincipalRateLimiter() error = %v, want ErrInvalidRateLimit", err)
			}
		})
	}
}

func TestPrincipalRateLimiterAllow(t *testing.T) {
	now := time.Date(2026, time.September, 13, 12, 0, 0, 0, time.UTC)
	limiter, err := NewPrincipalRateLimiter(6, func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	if !limiter.Allow("analyst-42") {
		t.Fatal("first request in initial burst was not allowed")
	}
	if !limiter.Allow("analyst-42") {
		t.Fatal("second request in initial burst was not allowed")
	}
	if limiter.Allow("analyst-42") {
		t.Fatal("third request in initial burst was allowed")
	}
	if !limiter.Allow("analyst-43") {
		t.Fatal("separate principal was not allowed")
	}
	now = now.Add(10 * time.Second)
	if !limiter.Allow("analyst-42") {
		t.Fatal("refilled token was not allowed")
	}
}

func TestPrincipalRateLimiterRejectsEmptySubject(t *testing.T) {
	limiter, err := NewPrincipalRateLimiter(1, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	if limiter.Allow("") {
		t.Fatal("empty subject was allowed")
	}
}
