package reviewcase

import (
	"errors"
	"testing"
)

func TestNewHandlerRejectsNilDependencies(t *testing.T) {
	if _, err := NewHandler(nil, nil); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("NewHandler() error = %v, want ErrInvalidArgument", err)
	}
}

func TestValidSubject(t *testing.T) {
	for _, test := range []struct {
		name, value string
		want        bool
	}{{"visible ASCII", "analyst-1", true}, {"empty", "", false}, {"space", "analyst one", false}} {
		t.Run(test.name, func(t *testing.T) {
			if got := validSubject(test.value); got != test.want {
				t.Errorf("validSubject(%q) = %t, want %t", test.value, got, test.want)
			}
		})
	}
}
