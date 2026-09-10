package postgres

import (
	"context"
	"errors"
	"testing"

	"github.com/krav01/intelligent-wallet-ledger/internal/event"
)

func TestReserveTxRejectsInvalidInput(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		consumer string
	}{
		{name: "nil transaction", consumer: "risk-worker"},
		{name: "uppercase consumer", consumer: "RiskWorker"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			_, err := ReserveTx(context.Background(), nil, test.consumer, event.Envelope{})
			if !errors.Is(err, ErrInvalidArgument) {
				t.Fatalf("ReserveTx() error = %v, want ErrInvalidArgument", err)
			}
		})
	}
}

func TestValidConsumerName(t *testing.T) {
	t.Parallel()
	tests := []struct {
		value string
		want  bool
	}{
		{value: "risk-worker.v1", want: true},
		{value: "1-worker", want: true},
		{value: "-worker", want: false},
		{value: "RiskWorker", want: false},
		{value: "", want: false},
	}
	for _, test := range tests {
		if got := validConsumerName(test.value); got != test.want {
			t.Errorf("validConsumerName(%q) = %t, want %t", test.value, got, test.want)
		}
	}
}
