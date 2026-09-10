// Package event defines transport-independent integration event envelopes.
package event

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"
)

const (
	maxEventTypeBytes     = 128
	maxAggregateTypeBytes = 64
	maxPayloadBytes       = 65536
)

// ErrInvalidEnvelope indicates malformed event identity, metadata, or payload.
var ErrInvalidEnvelope = errors.New("event: invalid envelope")

// DraftParams contains event data before PostgreSQL assigns its event ID.
type DraftParams struct {
	EventType        string
	EventVersion     int16
	AggregateType    string
	AggregateID      string
	AggregateVersion int64
	CorrelationID    string
	CausationID      string
	OccurredAt       time.Time
	Payload          json.RawMessage
}

// Draft is an immutable event awaiting durable identity assignment.
type Draft struct {
	eventType        string
	eventVersion     int16
	aggregateType    string
	aggregateID      string
	aggregateVersion int64
	correlationID    string
	causationID      string
	occurredAt       time.Time
	payload          json.RawMessage
}

// Envelope is an immutable, durably identified integration event.
type Envelope struct {
	eventID string
	draft   Draft
}

// NewDraft validates and creates an event draft.
func NewDraft(params DraftParams) (Draft, error) {
	payload, err := normalizePayload(params.Payload)
	if err != nil {
		return Draft{}, err
	}
	aggregateID, _ := normalizeUUID(params.AggregateID)
	correlationID, _ := normalizeUUID(params.CorrelationID)
	causationID := ""
	if params.CausationID != "" {
		var valid bool
		causationID, valid = normalizeUUID(params.CausationID)
		if !valid {
			return Draft{}, fmt.Errorf("%w: causation ID must be a UUID", ErrInvalidEnvelope)
		}
	}
	draft := Draft{
		eventType:        params.EventType,
		eventVersion:     params.EventVersion,
		aggregateType:    params.AggregateType,
		aggregateID:      aggregateID,
		aggregateVersion: params.AggregateVersion,
		correlationID:    correlationID,
		causationID:      causationID,
		occurredAt:       params.OccurredAt.UTC(),
		payload:          payload,
	}
	if err := draft.validate(); err != nil {
		return Draft{}, err
	}

	return draft, nil
}

// NewEnvelope validates a durable event ID and attaches it to a draft.
func NewEnvelope(eventID string, draft Draft) (Envelope, error) {
	canonicalID, valid := normalizeUUID(eventID)
	if !valid {
		return Envelope{}, fmt.Errorf("%w: event ID must be a UUID", ErrInvalidEnvelope)
	}
	if err := draft.validate(); err != nil {
		return Envelope{}, err
	}

	return Envelope{eventID: canonicalID, draft: draft}, nil
}

// EventID returns the durable event identifier.
func (e Envelope) EventID() string { return e.eventID }

// EventType returns the stable event contract name.
func (e Envelope) EventType() string { return e.draft.eventType }

// EventVersion returns the event payload schema version.
func (e Envelope) EventVersion() int16 { return e.draft.eventVersion }

// AggregateType returns the aggregate kind.
func (e Envelope) AggregateType() string { return e.draft.aggregateType }

// AggregateID returns the aggregate identifier.
func (e Envelope) AggregateID() string { return e.draft.aggregateID }

// AggregateVersion returns the aggregate version that emitted the event.
func (e Envelope) AggregateVersion() int64 { return e.draft.aggregateVersion }

// CorrelationID returns the operation correlation identifier.
func (e Envelope) CorrelationID() string { return e.draft.correlationID }

// CausationID returns the optional causing event identifier.
func (e Envelope) CausationID() string { return e.draft.causationID }

// OccurredAt returns when the business event occurred.
func (e Envelope) OccurredAt() time.Time { return e.draft.occurredAt }

// Payload returns a defensive copy of the event-specific JSON object.
func (e Envelope) Payload() json.RawMessage { return bytes.Clone(e.draft.payload) }

// EventType returns the stable event contract name.
func (d Draft) EventType() string { return d.eventType }

// EventVersion returns the event payload schema version.
func (d Draft) EventVersion() int16 { return d.eventVersion }

// AggregateType returns the aggregate kind.
func (d Draft) AggregateType() string { return d.aggregateType }

// AggregateID returns the aggregate identifier.
func (d Draft) AggregateID() string { return d.aggregateID }

// AggregateVersion returns the aggregate version that emitted the event.
func (d Draft) AggregateVersion() int64 { return d.aggregateVersion }

// CorrelationID returns the operation correlation identifier.
func (d Draft) CorrelationID() string { return d.correlationID }

// CausationID returns the optional causing event identifier.
func (d Draft) CausationID() string { return d.causationID }

// OccurredAt returns when the business event occurred.
func (d Draft) OccurredAt() time.Time { return d.occurredAt }

// Payload returns a defensive copy of the event-specific JSON object.
func (d Draft) Payload() json.RawMessage { return bytes.Clone(d.payload) }

// MarshalJSON writes the stable transport envelope.
func (e Envelope) MarshalJSON() ([]byte, error) {
	if !isCanonicalUUID(e.eventID) || e.draft.validate() != nil {
		return nil, ErrInvalidEnvelope
	}

	return json.Marshal(struct {
		EventID          string          `json:"event_id"`
		EventType        string          `json:"event_type"`
		EventVersion     int16           `json:"event_version"`
		AggregateType    string          `json:"aggregate_type"`
		AggregateID      string          `json:"aggregate_id"`
		AggregateVersion int64           `json:"aggregate_version"`
		CorrelationID    string          `json:"correlation_id"`
		CausationID      string          `json:"causation_id,omitempty"`
		OccurredAt       time.Time       `json:"occurred_at"`
		Payload          json.RawMessage `json:"payload"`
	}{
		EventID:          e.eventID,
		EventType:        e.EventType(),
		EventVersion:     e.EventVersion(),
		AggregateType:    e.AggregateType(),
		AggregateID:      e.AggregateID(),
		AggregateVersion: e.AggregateVersion(),
		CorrelationID:    e.CorrelationID(),
		CausationID:      e.CausationID(),
		OccurredAt:       e.OccurredAt(),
		Payload:          e.Payload(),
	})
}

func (d Draft) validate() error {
	switch {
	case !validEventType(d.eventType):
		return fmt.Errorf("%w: event type is invalid", ErrInvalidEnvelope)
	case d.eventVersion <= 0:
		return fmt.Errorf("%w: event version must be positive", ErrInvalidEnvelope)
	case !validAggregateType(d.aggregateType):
		return fmt.Errorf("%w: aggregate type is invalid", ErrInvalidEnvelope)
	case !isCanonicalUUID(d.aggregateID):
		return fmt.Errorf("%w: aggregate ID must be a UUID", ErrInvalidEnvelope)
	case d.aggregateVersion <= 0:
		return fmt.Errorf("%w: aggregate version must be positive", ErrInvalidEnvelope)
	case !isCanonicalUUID(d.correlationID):
		return fmt.Errorf("%w: correlation ID must be a UUID", ErrInvalidEnvelope)
	case d.causationID != "" && !isCanonicalUUID(d.causationID):
		return fmt.Errorf("%w: causation ID must be a UUID", ErrInvalidEnvelope)
	case d.occurredAt.IsZero():
		return fmt.Errorf("%w: occurrence time is required", ErrInvalidEnvelope)
	case !validPayload(d.payload):
		return fmt.Errorf("%w: payload must be a JSON object up to %d bytes", ErrInvalidEnvelope, maxPayloadBytes)
	default:
		return nil
	}
}

func validEventType(value string) bool {
	if value == "" || len(value) > maxEventTypeBytes {
		return false
	}
	for part := range strings.SplitSeq(value, ".") {
		if !validLowerName(part, false) {
			return false
		}
	}
	return true
}

func validAggregateType(value string) bool {
	return value != "" && len(value) <= maxAggregateTypeBytes && validLowerName(value, true)
}

func validLowerName(value string, separators bool) bool {
	for index, character := range []byte(value) {
		if character >= 'a' && character <= 'z' || index > 0 && character >= '0' && character <= '9' {
			continue
		}
		if separators && index > 0 && (character == '_' || character == '-') {
			continue
		}
		return false
	}
	return value != ""
}

func validPayload(payload json.RawMessage) bool {
	return len(payload) >= 2 && len(payload) <= maxPayloadBytes &&
		payload[0] == '{' && json.Valid(payload)
}

func normalizePayload(payload json.RawMessage) (json.RawMessage, error) {
	trimmed := bytes.TrimSpace(payload)
	if len(trimmed) < 2 || trimmed[0] != '{' {
		return nil, fmt.Errorf("%w: payload must be a JSON object up to %d bytes", ErrInvalidEnvelope, maxPayloadBytes)
	}
	var compact bytes.Buffer
	if err := json.Compact(&compact, trimmed); err != nil {
		return nil, fmt.Errorf("%w: payload must be a JSON object up to %d bytes", ErrInvalidEnvelope, maxPayloadBytes)
	}
	storedSize, supported := postgresJSONBTextSize(compact.Bytes())
	if !supported || storedSize > maxPayloadBytes {
		return nil, fmt.Errorf(
			"%w: payload must use non-exponent numbers and fit %d PostgreSQL JSONB text bytes",
			ErrInvalidEnvelope,
			maxPayloadBytes,
		)
	}

	return bytes.Clone(compact.Bytes()), nil
}

func postgresJSONBTextSize(payload []byte) (int, bool) {
	storedSize := len(payload)
	inString := false
	escaped := false
	for index := 0; index < len(payload); index++ {
		character := payload[index]
		if inString {
			switch {
			case escaped:
				escaped = false
			case character == '\\':
				escaped = true
			case character == '"':
				inString = false
			}
			continue
		}
		switch {
		case character == '"':
			inString = true
		case character == ',' || character == ':':
			storedSize++
		case character == '-' || character >= '0' && character <= '9':
			for index++; index < len(payload); index++ {
				character = payload[index]
				if character == 'e' || character == 'E' {
					return 0, false
				}
				if character == ',' || character == ']' || character == '}' {
					index--
					break
				}
			}
		}
	}

	return storedSize, true
}

func normalizeUUID(value string) (string, bool) {
	value = strings.TrimSpace(value)
	if len(value) != 36 || value[8] != '-' || value[13] != '-' || value[18] != '-' || value[23] != '-' {
		return "", false
	}
	hexValue := strings.ReplaceAll(value, "-", "")
	if len(hexValue) != 32 {
		return "", false
	}
	if _, err := hex.DecodeString(hexValue); err != nil {
		return "", false
	}

	return strings.ToLower(value), true
}

func isCanonicalUUID(value string) bool {
	canonical, valid := normalizeUUID(value)
	return valid && canonical == value
}
