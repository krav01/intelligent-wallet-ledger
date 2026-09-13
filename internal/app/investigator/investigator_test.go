package investigator

import (
	"context"
	"errors"
	"testing"
)

func TestServiceExplain(t *testing.T) {
	investigation, err := NewInvestigation("case-1", 2, "USD", 60, 600, []Signal{{Code: "amount_review_threshold", Contribution: 600}})
	if err != nil {
		t.Fatal(err)
	}
	service, err := NewService(MockProvider{Explanation: "Review the amount threshold."})
	if err != nil {
		t.Fatal(err)
	}
	got, err := service.Explain(context.Background(), investigation)
	if err != nil || got != "Review the amount threshold." {
		t.Fatalf("Explain() = (%q, %v)", got, err)
	}
}

func TestServiceExplainPropagatesProviderFailure(t *testing.T) {
	investigation, _ := NewInvestigation("case-1", 2, "USD", 60, 600, nil)
	providerErr := errors.New("provider unavailable")
	service, _ := NewService(MockProvider{Err: providerErr})
	if _, err := service.Explain(context.Background(), investigation); !errors.Is(err, providerErr) {
		t.Fatalf("Explain() error = %v, want provider error", err)
	}
}

func TestInvestigationCopiesSignals(t *testing.T) {
	signals := []Signal{{Code: "amount_review_threshold", Contribution: 600}}
	investigation, err := NewInvestigation("case-1", 2, "USD", 60, 600, signals)
	if err != nil {
		t.Fatal(err)
	}
	signals[0].Code = "changed"
	got := investigation.Signals()
	got[0].Code = "changed-again"
	if investigation.Signals()[0].Code != "amount_review_threshold" {
		t.Fatal("Signals() leaked mutable input")
	}
}
