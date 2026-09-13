// Package investigator produces advisory explanations for existing review cases.
package investigator

import (
	"context"
	"errors"
	"fmt"
	"strings"
)

// ErrInvalidArgument indicates malformed advisory investigation input.
var ErrInvalidArgument = errors.New("investigator: invalid argument")

// Signal is a deterministic risk contribution safe to share with an advisory provider.
type Signal struct {
	Code         string
	Contribution int
}

// Investigation contains the minimized, pseudonymous facts for one review case.
type Investigation struct {
	caseID            string
	assessmentVersion int64
	currency          string
	amountMinor       int64
	score             int
	signals           []Signal
}

// NewInvestigation validates and copies provider-safe review facts.
func NewInvestigation(caseID string, assessmentVersion int64, currency string, amountMinor int64, score int, signals []Signal) (Investigation, error) {
	if strings.TrimSpace(caseID) == "" || assessmentVersion < 2 || len(currency) != 3 || amountMinor <= 0 || score < 0 || score > 1000 {
		return Investigation{}, ErrInvalidArgument
	}
	copySignals := append([]Signal(nil), signals...)
	for _, signal := range copySignals {
		if strings.TrimSpace(signal.Code) == "" || signal.Contribution < 0 {
			return Investigation{}, ErrInvalidArgument
		}
	}
	return Investigation{caseID: caseID, assessmentVersion: assessmentVersion, currency: currency, amountMinor: amountMinor, score: score, signals: copySignals}, nil
}

// CaseID returns the pseudonymous review-case identity.
func (i Investigation) CaseID() string { return i.caseID }

// AssessmentVersion returns the deterministic assessment version.
func (i Investigation) AssessmentVersion() int64 { return i.assessmentVersion }

// Currency returns the three-letter transfer currency.
func (i Investigation) Currency() string { return i.currency }

// AmountMinor returns the transfer amount in minor units.
func (i Investigation) AmountMinor() int64 { return i.amountMinor }

// Score returns the deterministic risk score.
func (i Investigation) Score() int { return i.score }

// Signals returns a defensive copy of deterministic risk signals.
func (i Investigation) Signals() []Signal { return append([]Signal(nil), i.signals...) }

// Provider is a read-only advisory provider owned by the investigation use case.
type Provider interface {
	Explain(context.Context, Investigation) (string, error)
}

// Service obtains advisory explanations and never mutates financial state.
type Service struct{ provider Provider }

// NewService creates an advisory investigation service.
func NewService(provider Provider) (*Service, error) {
	if provider == nil {
		return nil, ErrInvalidArgument
	}
	return &Service{provider: provider}, nil
}

// Explain returns a provider explanation for the supplied, already-open review case.
func (s *Service) Explain(ctx context.Context, investigation Investigation) (string, error) {
	if s == nil || s.provider == nil || investigation.caseID == "" {
		return "", ErrInvalidArgument
	}
	explanation, err := s.provider.Explain(ctx, investigation)
	if err != nil {
		return "", fmt.Errorf("explaining review case: %w", err)
	}
	if strings.TrimSpace(explanation) == "" {
		return "", ErrInvalidArgument
	}
	return explanation, nil
}

// MockProvider returns configured advice for deterministic local tests and demos.
type MockProvider struct {
	Explanation string
	Err         error
}

// Explain returns configured advice or error without external I/O.
func (p MockProvider) Explain(_ context.Context, _ Investigation) (string, error) {
	if p.Err != nil {
		return "", p.Err
	}
	return p.Explanation, nil
}
