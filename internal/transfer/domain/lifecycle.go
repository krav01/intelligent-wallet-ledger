package domain

import (
	"errors"
	"fmt"
	"strings"
)

const (
	maxLifecycleCodeBytes = 64
	pendingRiskVersion    = 1
)

var (
	// ErrInvalidLifecycle indicates stored or requested lifecycle data violates its invariants.
	ErrInvalidLifecycle = errors.New("transfer domain: invalid lifecycle")
	// ErrInvalidTransition indicates an operation is not permitted from the current state.
	ErrInvalidTransition = errors.New("transfer domain: invalid lifecycle transition")
)

// Status describes the durable lifecycle of a transfer intent.
type Status uint8

const (
	// StatusUnknown is the invalid zero status.
	StatusUnknown Status = iota
	// StatusPendingRisk awaits a deterministic risk assessment.
	StatusPendingRisk
	// StatusApproved awaits final financial posting.
	StatusApproved
	// StatusReviewRequired awaits a future manual decision.
	StatusReviewRequired
	// StatusDeclined is a terminal deterministic risk decision.
	StatusDeclined
	// StatusCompleted is a terminal state with a posted journal entry.
	StatusCompleted
	// StatusFailed is a terminal business failure during final posting.
	StatusFailed
)

// String returns the stable wire representation of a status.
func (s Status) String() string {
	switch s {
	case StatusPendingRisk:
		return "pending_risk"
	case StatusApproved:
		return "approved"
	case StatusReviewRequired:
		return "review_required"
	case StatusDeclined:
		return "declined"
	case StatusCompleted:
		return "completed"
	case StatusFailed:
		return "failed"
	default:
		return ""
	}
}

// LifecycleParams reconstructs one lifecycle aggregate from durable data.
type LifecycleParams struct {
	Transfer          Transfer
	Status            Status
	Version           int64
	RiskPolicyVersion string
	JournalEntryID    string
	FailureReason     string
}

// Lifecycle pairs immutable transfer intent with its mutable state-machine data.
type Lifecycle struct {
	transfer          Transfer
	status            Status
	version           int64
	riskPolicyVersion string
	journalEntryID    string
	failureReason     string
}

// NewPendingLifecycle creates a newly accepted transfer awaiting risk assessment.
func NewPendingLifecycle(transfer Transfer, riskPolicyVersion string) (Lifecycle, error) {
	return NewLifecycle(LifecycleParams{
		Transfer:          transfer,
		Status:            StatusPendingRisk,
		Version:           pendingRiskVersion,
		RiskPolicyVersion: riskPolicyVersion,
	})
}

// NewLifecycle validates and reconstructs a lifecycle aggregate.
func NewLifecycle(params LifecycleParams) (Lifecycle, error) {
	lifecycle := Lifecycle{
		transfer:          params.Transfer,
		status:            params.Status,
		version:           params.Version,
		riskPolicyVersion: strings.TrimSpace(params.RiskPolicyVersion),
		journalEntryID:    strings.TrimSpace(params.JournalEntryID),
		failureReason:     strings.TrimSpace(params.FailureReason),
	}
	if err := lifecycle.validate(); err != nil {
		return Lifecycle{}, err
	}
	return lifecycle, nil
}

// Transfer returns the immutable transfer intent.
func (l Lifecycle) Transfer() Transfer { return l.transfer }

// Status returns the current lifecycle state.
func (l Lifecycle) Status() Status { return l.status }

// Version returns the positive aggregate version.
func (l Lifecycle) Version() int64 { return l.version }

// RiskPolicyVersion returns the immutable policy version chosen at acceptance.
func (l Lifecycle) RiskPolicyVersion() string { return l.riskPolicyVersion }

// JournalEntryID returns the posted journal entry ID only after completion.
func (l Lifecycle) JournalEntryID() string { return l.journalEntryID }

// FailureReason returns the stable business failure code only after failure.
func (l Lifecycle) FailureReason() string { return l.failureReason }

// Approve moves a pending or manually reviewed transfer to approved.
func (l Lifecycle) Approve() (Lifecycle, error) {
	if l.status != StatusPendingRisk && l.status != StatusReviewRequired {
		return Lifecycle{}, fmt.Errorf("%w: approve from %s", ErrInvalidTransition, l.status)
	}
	return l.transition(StatusApproved, "", "")
}

// RequireReview moves a pending transfer to manual review.
func (l Lifecycle) RequireReview() (Lifecycle, error) {
	if l.status != StatusPendingRisk {
		return Lifecycle{}, fmt.Errorf("%w: review from %s", ErrInvalidTransition, l.status)
	}
	return l.transition(StatusReviewRequired, "", "")
}

// Decline moves a pending or manually reviewed transfer to terminal decline.
func (l Lifecycle) Decline() (Lifecycle, error) {
	if l.status != StatusPendingRisk && l.status != StatusReviewRequired {
		return Lifecycle{}, fmt.Errorf("%w: decline from %s", ErrInvalidTransition, l.status)
	}
	return l.transition(StatusDeclined, "", "")
}

// Complete moves an approved transfer to terminal completion with its journal entry.
func (l Lifecycle) Complete() (Lifecycle, error) {
	if l.status != StatusApproved {
		return Lifecycle{}, fmt.Errorf("%w: complete from %s", ErrInvalidTransition, l.status)
	}
	return l.transition(StatusCompleted, l.transfer.ID(), "")
}

// Fail moves an approved transfer to terminal business failure.
func (l Lifecycle) Fail(reason string) (Lifecycle, error) {
	if l.status != StatusApproved {
		return Lifecycle{}, fmt.Errorf("%w: fail from %s", ErrInvalidTransition, l.status)
	}
	if !validLifecycleCode(strings.TrimSpace(reason)) {
		return Lifecycle{}, fmt.Errorf("%w: failure reason", ErrInvalidLifecycle)
	}
	return l.transition(StatusFailed, "", strings.TrimSpace(reason))
}

func (l Lifecycle) transition(status Status, journalEntryID, failureReason string) (Lifecycle, error) {
	next, err := NewLifecycle(LifecycleParams{
		Transfer:          l.transfer,
		Status:            status,
		Version:           l.version + 1,
		RiskPolicyVersion: l.riskPolicyVersion,
		JournalEntryID:    journalEntryID,
		FailureReason:     failureReason,
	})
	if err != nil {
		return Lifecycle{}, fmt.Errorf("transitioning lifecycle: %w", err)
	}
	return next, nil
}

func (l Lifecycle) validate() error {
	if err := l.transfer.validate(); err != nil {
		return fmt.Errorf("%w: transfer: %w", ErrInvalidLifecycle, err)
	}
	if l.version <= 0 {
		return ErrInvalidLifecycle
	}

	switch l.status {
	case StatusPendingRisk:
		if l.version != pendingRiskVersion || l.journalEntryID != "" || l.failureReason != "" {
			return ErrInvalidLifecycle
		}
	case StatusApproved, StatusReviewRequired, StatusDeclined:
		if l.version < 2 || l.journalEntryID != "" || l.failureReason != "" {
			return ErrInvalidLifecycle
		}
	case StatusCompleted:
		if l.journalEntryID != l.transfer.ID() || l.failureReason != "" ||
			(l.riskPolicyVersion != "" && !validPolicyVersion(l.riskPolicyVersion)) ||
			(l.version != pendingRiskVersion && !validPolicyVersion(l.riskPolicyVersion)) {
			return ErrInvalidLifecycle
		}
	case StatusFailed:
		if !validPolicyVersion(l.riskPolicyVersion) || l.version < 2 ||
			l.journalEntryID != "" || !validLifecycleCode(l.failureReason) {
			return ErrInvalidLifecycle
		}
	default:
		return ErrInvalidLifecycle
	}
	if l.status != StatusCompleted && !validPolicyVersion(l.riskPolicyVersion) {
		return ErrInvalidLifecycle
	}
	return nil
}

func validLifecycleCode(value string) bool {
	if len(value) == 0 || len(value) > maxLifecycleCodeBytes {
		return false
	}
	for _, character := range value {
		if character >= 'a' && character <= 'z' || character >= '0' && character <= '9' || character == '_' || character == '-' {
			continue
		}
		return false
	}
	return true
}

func validPolicyVersion(value string) bool {
	if len(value) == 0 || len(value) > maxLifecycleCodeBytes {
		return false
	}
	for _, character := range value {
		if character < '!' || character > '~' {
			return false
		}
	}
	return true
}
