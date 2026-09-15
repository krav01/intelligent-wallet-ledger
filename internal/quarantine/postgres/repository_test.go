package postgres

import (
	"errors"
	"testing"
)

func TestNewRepositoryRejectsNilPool(t *testing.T) {
	t.Parallel()

	if _, err := NewRepository(nil); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("NewRepository(nil) error = %v, want ErrInvalidArgument", err)
	}
}

func TestValidateRecord(t *testing.T) {
	t.Parallel()

	valid := Record{
		ConsumerName: "risk-worker.v1",
		Topic:        "wallet.events.v1",
		Partition:    0,
		Offset:       1,
		ReasonCode:   ReasonInvalidEnvelope,
	}
	tests := []struct {
		name    string
		change  func(*Record)
		wantErr bool
	}{
		{name: "valid"},
		{name: "consumer", change: func(record *Record) { record.ConsumerName = "RiskWorker" }, wantErr: true},
		{name: "topic", change: func(record *Record) { record.Topic = "" }, wantErr: true},
		{name: "partition", change: func(record *Record) { record.Partition = -1 }, wantErr: true},
		{name: "offset", change: func(record *Record) { record.Offset = -1 }, wantErr: true},
		{name: "reason", change: func(record *Record) { record.ReasonCode = "database_error" }, wantErr: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			record := valid
			if test.change != nil {
				test.change(&record)
			}
			err := validateRecord(record)
			if test.wantErr && !errors.Is(err, ErrInvalidArgument) {
				t.Fatalf("validateRecord() error = %v, want ErrInvalidArgument", err)
			}
			if !test.wantErr && err != nil {
				t.Fatalf("validateRecord() error = %v, want nil", err)
			}
		})
	}
}
