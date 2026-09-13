package reviewdecision

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/krav01/intelligent-wallet-ledger/internal/app/reviewcase"
	"github.com/krav01/intelligent-wallet-ledger/internal/identity/oidc"
	transferpostgres "github.com/krav01/intelligent-wallet-ledger/internal/transfer/postgres"
)

func TestNewRejectsNilDependencies(t *testing.T) {
	for _, test := range []struct {
		name          string
		authenticator authenticator
		decider       decisionHandler
		limiter       principalLimiter
		logger        *slog.Logger
	}{
		{name: "missing authenticator", decider: &fakeDecider{}, limiter: allowAllLimiter{}, logger: slog.New(slog.DiscardHandler)},
		{name: "missing decider", authenticator: &fakeAuthenticator{}, limiter: allowAllLimiter{}, logger: slog.New(slog.DiscardHandler)},
		{name: "missing limiter", authenticator: &fakeAuthenticator{}, decider: &fakeDecider{}, logger: slog.New(slog.DiscardHandler)},
		{name: "missing logger", authenticator: &fakeAuthenticator{}, decider: &fakeDecider{}, limiter: allowAllLimiter{}},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, err := New(test.authenticator, test.decider, test.limiter, test.logger)
			if !errors.Is(err, ErrInvalidDependency) {
				t.Errorf("New() error = %v, want ErrInvalidDependency", err)
			}
		})
	}
}

func TestHandlerServeHTTP(t *testing.T) {
	tests := []struct {
		name         string
		auth         fakeAuthenticator
		decider      fakeDecider
		authorize    string
		body         string
		wantStatus   int
		wantSubject  string
		wantDecision reviewcase.Decision
	}{
		{
			name: "records an approved decision",
			auth: fakeAuthenticator{principal: oidc.Principal{
				Subject: "analyst-42",
				Roles:   []string{"analyst"},
			}},
			authorize:    "bearer signed-token",
			body:         `{"decision":"approved"}`,
			wantStatus:   http.StatusNoContent,
			wantSubject:  "analyst-42",
			wantDecision: reviewcase.DecisionApprove,
		},
		{
			name:       "rejects missing bearer token",
			auth:       fakeAuthenticator{},
			decider:    fakeDecider{},
			wantStatus: http.StatusUnauthorized,
		},
		{
			name:       "rejects invalid token",
			auth:       fakeAuthenticator{err: oidc.ErrInvalidToken},
			authorize:  "Bearer invalid-token",
			body:       `{"decision":"approved"}`,
			wantStatus: http.StatusUnauthorized,
		},
		{
			name: "rejects non analyst",
			auth: fakeAuthenticator{principal: oidc.Principal{
				Subject: "viewer-42",
				Roles:   []string{"viewer"},
			}},
			authorize:  "Bearer signed-token",
			body:       `{"decision":"approved"}`,
			wantStatus: http.StatusForbidden,
		},
		{
			name: "rejects unknown request field",
			auth: fakeAuthenticator{principal: oidc.Principal{
				Subject: "analyst-42",
				Roles:   []string{"analyst"},
			}},
			authorize:  "Bearer signed-token",
			body:       `{"decision":"approved","role":"analyst"}`,
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "maps closed review case to conflict",
			auth: fakeAuthenticator{principal: oidc.Principal{
				Subject: "analyst-42",
				Roles:   []string{"analyst"},
			}},
			decider:      fakeDecider{err: transferpostgres.ErrStateConflict},
			authorize:    "Bearer signed-token",
			body:         `{"decision":"declined"}`,
			wantStatus:   http.StatusConflict,
			wantSubject:  "analyst-42",
			wantDecision: reviewcase.DecisionDecline,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			handler, err := New(&test.auth, &test.decider, allowAllLimiter{}, slog.New(slog.DiscardHandler))
			if err != nil {
				t.Fatal(err)
			}
			request := httptest.NewRequestWithContext(
				t.Context(),
				http.MethodPost,
				"/v1/transfers/123e4567-e89b-12d3-a456-426614174000/review-decision",
				strings.NewReader(test.body),
			)
			request.SetPathValue("transferID", "123e4567-e89b-12d3-a456-426614174000")
			request.Header.Set("Authorization", test.authorize)
			response := httptest.NewRecorder()

			handler.ServeHTTP(response, request)

			if response.Code != test.wantStatus {
				t.Fatalf("status = %d, want %d; body = %q", response.Code, test.wantStatus, response.Body.String())
			}
			if test.decider.principal.Subject != test.wantSubject {
				t.Errorf("decider subject = %q, want %q", test.decider.principal.Subject, test.wantSubject)
			}
			if test.decider.decision != test.wantDecision {
				t.Errorf("decider decision = %q, want %q", test.decider.decision, test.wantDecision)
			}
		})
	}
}

func TestHandlerRejectsRateLimitedPrincipal(t *testing.T) {
	decider := &fakeDecider{}
	handler, err := New(
		&fakeAuthenticator{principal: oidc.Principal{Subject: "analyst-42", Roles: []string{"analyst"}}},
		decider,
		denyAllLimiter{},
		slog.New(slog.DiscardHandler),
	)
	if err != nil {
		t.Fatal(err)
	}
	request := httptest.NewRequestWithContext(
		t.Context(),
		http.MethodPost,
		"/v1/transfers/123e4567-e89b-12d3-a456-426614174000/review-decision",
		strings.NewReader(`{"decision":"approved"}`),
	)
	request.SetPathValue("transferID", "123e4567-e89b-12d3-a456-426614174000")
	request.Header.Set("Authorization", "Bearer signed-token")
	response := httptest.NewRecorder()

	handler.ServeHTTP(response, request)

	if response.Code != http.StatusTooManyRequests {
		t.Fatalf("status = %d, want %d; body = %q", response.Code, http.StatusTooManyRequests, response.Body.String())
	}
	if decider.decision != "" {
		t.Errorf("decider decision = %q, want no application call", decider.decision)
	}
}

type fakeAuthenticator struct {
	principal oidc.Principal
	err       error
}

func (a *fakeAuthenticator) Authenticate(context.Context, string) (oidc.Principal, error) {
	return a.principal, a.err
}

type fakeDecider struct {
	principal reviewcase.TrustedPrincipal
	decision  reviewcase.Decision
	err       error
}

func (d *fakeDecider) Decide(_ context.Context, principal reviewcase.TrustedPrincipal, _ string, decision reviewcase.Decision) error {
	d.principal = principal
	d.decision = decision
	return d.err
}

type allowAllLimiter struct{}

func (allowAllLimiter) Allow(string) bool { return true }

type denyAllLimiter struct{}

func (denyAllLimiter) Allow(string) bool { return false }
