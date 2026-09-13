package postgres

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestAppendReviewDecisionTxRejectsNilTransaction(t *testing.T) {
	err := AppendReviewDecisionTx(context.Background(), nil, ReviewDecision{})
	if !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("AppendReviewDecisionTx(nil) error = %v, want ErrInvalidArgument", err)
	}
}

func TestValidateReviewDecision(t *testing.T) {
	valid := ReviewDecision{
		TransferID:       "66666666-6666-4666-8666-666666666666",
		LifecycleVersion: 3,
		Decision:         "approved",
		ActorSubject:     "analyst-1",
		OccurredAt:       time.Date(2026, time.September, 13, 12, 0, 0, 0, time.UTC),
	}
	for _, test := range []struct {
		name    string
		mutate  func(*ReviewDecision)
		wantErr bool
	}{
		{name: "valid"},
		{name: "invalid transfer ID", mutate: func(record *ReviewDecision) { record.TransferID = "not-a-uuid" }, wantErr: true},
		{name: "review lifecycle version", mutate: func(record *ReviewDecision) { record.LifecycleVersion = 2 }, wantErr: true},
		{name: "invalid decision", mutate: func(record *ReviewDecision) { record.Decision = "review" }, wantErr: true},
		{name: "invalid actor subject", mutate: func(record *ReviewDecision) { record.ActorSubject = "analyst one" }, wantErr: true},
		{name: "zero occurred at", mutate: func(record *ReviewDecision) { record.OccurredAt = time.Time{} }, wantErr: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			record := valid
			if test.mutate != nil {
				test.mutate(&record)
			}
			err := validateReviewDecision(record)
			if test.wantErr && !errors.Is(err, ErrInvalidArgument) {
				t.Errorf("validateReviewDecision() error = %v, want ErrInvalidArgument", err)
			}
			if !test.wantErr && err != nil {
				t.Errorf("validateReviewDecision() error = %v, want nil", err)
			}
		})
	}
}
