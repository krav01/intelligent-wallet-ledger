package event_test

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/krav01/intelligent-wallet-ledger/internal/event"
)

const (
	eventID     = "11111111-1111-4111-8111-111111111111"
	aggregateID = "22222222-2222-4222-8222-222222222222"
)

func TestNewDraftAndEnvelope(t *testing.T) {
	t.Parallel()

	params := validDraftParams()
	params.AggregateID = strings.ToUpper(params.AggregateID)
	params.CorrelationID = " " + strings.ToUpper(params.CorrelationID) + " "
	params.Payload = json.RawMessage(" \n { \"amount_minor\" : 125 } \t")
	draft, err := event.NewDraft(params)
	if err != nil {
		t.Fatalf("NewDraft() error = %v", err)
	}
	params.Payload[5] = 'x'
	if string(draft.Payload()) != `{"amount_minor":125}` {
		t.Errorf("Draft.Payload() = %s, want defensive copy", draft.Payload())
	}

	envelope, err := event.NewEnvelope(strings.ToUpper(eventID), draft)
	if err != nil {
		t.Fatalf("NewEnvelope() error = %v", err)
	}
	if envelope.EventID() != eventID || envelope.AggregateID() != aggregateID {
		t.Errorf("Envelope IDs = (%q, %q), want canonical UUIDs", envelope.EventID(), envelope.AggregateID())
	}

	encoded, err := json.Marshal(envelope)
	if err != nil {
		t.Fatalf("json.Marshal() error = %v", err)
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(encoded, &fields); err != nil {
		t.Fatalf("json.Unmarshal() error = %v", err)
	}
	if len(fields) != 9 {
		t.Errorf("envelope field count = %d, want 9 without causation_id", len(fields))
	}
}

func TestNewDraftRejectsInvalidInput(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		change func(*event.DraftParams)
	}{
		{name: "event type", change: func(params *event.DraftParams) { params.EventType = "Transfer.Completed" }},
		{name: "event version", change: func(params *event.DraftParams) { params.EventVersion = 0 }},
		{name: "aggregate type", change: func(params *event.DraftParams) { params.AggregateType = "transfer.event" }},
		{name: "aggregate ID", change: func(params *event.DraftParams) { params.AggregateID = "bad" }},
		{name: "aggregate version", change: func(params *event.DraftParams) { params.AggregateVersion = 0 }},
		{name: "correlation ID", change: func(params *event.DraftParams) { params.CorrelationID = "bad" }},
		{name: "causation ID", change: func(params *event.DraftParams) { params.CausationID = "bad" }},
		{name: "occurrence time", change: func(params *event.DraftParams) { params.OccurredAt = time.Time{} }},
		{name: "payload array", change: func(params *event.DraftParams) { params.Payload = json.RawMessage(`[]`) }},
		{name: "payload malformed", change: func(params *event.DraftParams) { params.Payload = json.RawMessage(`{`) }},
		{name: "payload exponent", change: func(params *event.DraftParams) { params.Payload = json.RawMessage(`{"score":1e3}`) }},
		{
			name: "payload oversized",
			change: func(params *event.DraftParams) {
				params.Payload = json.RawMessage(`{"value":"` + strings.Repeat("x", 65536) + `"}`)
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			params := validDraftParams()
			test.change(&params)
			_, err := event.NewDraft(params)
			if !errors.Is(err, event.ErrInvalidEnvelope) {
				t.Fatalf("NewDraft() error = %v, want ErrInvalidEnvelope", err)
			}
		})
	}
}

func TestNewDraftAcceptsEventTypeWithUnderscore(t *testing.T) {
	t.Parallel()
	params := validDraftParams()
	params.EventType = "transfer.risk_assessed"

	if _, err := event.NewDraft(params); err != nil {
		t.Fatalf("NewDraft() error = %v", err)
	}
}

func TestNewDraftAcceptsPostgreSQLJSONBBoundary(t *testing.T) {
	t.Parallel()
	params := validDraftParams()
	params.Payload = json.RawMessage(`{"value":"` + strings.Repeat("x", 65523) + `"}`)
	if _, err := event.NewDraft(params); err != nil {
		t.Fatalf("NewDraft(boundary payload) error = %v", err)
	}
	params.Payload = json.RawMessage(`{"value":"` + strings.Repeat("x", 65524) + `"}`)
	if _, err := event.NewDraft(params); !errors.Is(err, event.ErrInvalidEnvelope) {
		t.Fatalf("NewDraft(oversized stored payload) error = %v, want ErrInvalidEnvelope", err)
	}
}

func TestNewEnvelopeRejectsInvalidID(t *testing.T) {
	t.Parallel()
	draft, err := event.NewDraft(validDraftParams())
	if err != nil {
		t.Fatalf("NewDraft() error = %v", err)
	}
	if _, err := event.NewEnvelope("bad", draft); !errors.Is(err, event.ErrInvalidEnvelope) {
		t.Fatalf("NewEnvelope() error = %v, want ErrInvalidEnvelope", err)
	}
}

func TestParseEnvelopeRoundTrip(t *testing.T) {
	t.Parallel()
	draft, err := event.NewDraft(validDraftParams())
	if err != nil {
		t.Fatalf("NewDraft() error = %v", err)
	}
	want, err := event.NewEnvelope(eventID, draft)
	if err != nil {
		t.Fatalf("NewEnvelope() error = %v", err)
	}
	encoded, err := json.Marshal(want)
	if err != nil {
		t.Fatalf("json.Marshal() error = %v", err)
	}
	got, err := event.ParseEnvelope(encoded)
	if err != nil {
		t.Fatalf("ParseEnvelope() error = %v", err)
	}
	if got.EventID() != want.EventID() || got.EventType() != want.EventType() ||
		got.EventVersion() != want.EventVersion() || got.AggregateID() != want.AggregateID() ||
		string(got.Payload()) != string(want.Payload()) {
		t.Errorf("ParseEnvelope() = %+v, want round trip", got)
	}
}

func TestParseEnvelopeRejectsUnknownAndTrailingData(t *testing.T) {
	t.Parallel()
	for _, encoded := range []string{
		`{"unknown":true}`,
		`{} {}`,
	} {
		if _, err := event.ParseEnvelope([]byte(encoded)); !errors.Is(err, event.ErrInvalidEnvelope) {
			t.Errorf("ParseEnvelope(%q) error = %v, want ErrInvalidEnvelope", encoded, err)
		}
	}
}

func validDraftParams() event.DraftParams {
	return event.DraftParams{
		EventType:        "transfer.completed",
		EventVersion:     1,
		AggregateType:    "transfer",
		AggregateID:      aggregateID,
		AggregateVersion: 1,
		CorrelationID:    aggregateID,
		OccurredAt:       time.Date(2026, time.September, 10, 12, 0, 0, 0, time.UTC),
		Payload:          json.RawMessage(`{"amount_minor":125}`),
	}
}
