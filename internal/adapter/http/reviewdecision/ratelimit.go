package reviewdecision

import (
	"errors"
	"sync"
	"time"

	"golang.org/x/time/rate"
)

const (
	maxRateLimitPerMinute = 600
	maxTrackedPrincipals  = 10_000
	principalIdleTTL      = 15 * time.Minute
)

// ErrInvalidRateLimit indicates that rate-limit construction input is invalid.
var ErrInvalidRateLimit = errors.New("review decision rate limit: invalid configuration")

// PrincipalRateLimiter limits commands from each verified principal independently.
type PrincipalRateLimiter struct {
	mu      sync.Mutex
	now     func() time.Time
	rate    rate.Limit
	burst   int
	entries map[string]rateLimitEntry
	nextGC  time.Time
}

type rateLimitEntry struct {
	limiter  *rate.Limiter
	lastSeen time.Time
}

// NewPrincipalRateLimiter creates an in-process token-bucket limiter for verified subjects.
func NewPrincipalRateLimiter(perMinute int, now func() time.Time) (*PrincipalRateLimiter, error) {
	if perMinute < 1 || perMinute > maxRateLimitPerMinute || now == nil {
		return nil, ErrInvalidRateLimit
	}
	burst := 2
	if perMinute == 1 {
		burst = 1
	}
	return &PrincipalRateLimiter{
		now:     now,
		rate:    rate.Limit(float64(perMinute) / time.Minute.Seconds()),
		burst:   burst,
		entries: make(map[string]rateLimitEntry),
	}, nil
}

// Allow reports whether the verified subject may submit one more command now.
func (l *PrincipalRateLimiter) Allow(subject string) bool {
	if subject == "" {
		return false
	}
	now := l.now()
	l.mu.Lock()
	defer l.mu.Unlock()

	if !now.Before(l.nextGC) {
		l.removeIdle(now)
		l.nextGC = now.Add(principalIdleTTL)
	}
	entry, ok := l.entries[subject]
	if !ok {
		if len(l.entries) == maxTrackedPrincipals {
			l.removeOldest()
		}
		entry.limiter = rate.NewLimiter(l.rate, l.burst)
	}
	entry.lastSeen = now
	l.entries[subject] = entry
	return entry.limiter.AllowN(now, 1)
}

func (l *PrincipalRateLimiter) removeIdle(now time.Time) {
	for subject, entry := range l.entries {
		if now.Sub(entry.lastSeen) >= principalIdleTTL {
			delete(l.entries, subject)
		}
	}
}

func (l *PrincipalRateLimiter) removeOldest() {
	var oldestSubject string
	var oldestSeen time.Time
	for subject, entry := range l.entries {
		if oldestSubject == "" || entry.lastSeen.Before(oldestSeen) {
			oldestSubject = subject
			oldestSeen = entry.lastSeen
		}
	}
	delete(l.entries, oldestSubject)
}
