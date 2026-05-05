package nats

import (
	"errors"
	"fmt"
	"testing"

	"github.com/nats-io/nats.go"
)

func TestKVRetryablePredicates(t *testing.T) {
	mismatch := &nats.APIError{ErrorCode: 10071}
	other := &nats.APIError{ErrorCode: 10070}

	cases := []struct {
		name        string
		err         error
		wantMis     bool
		wantKNF     bool
		wantRetry   bool
	}{
		{"nil", nil, false, false, false},
		{"plain", errors.New("boom"), false, false, false},
		{"mismatch_api_10071", mismatch, true, false, true},
		{"other_api_10070", other, false, false, false},
		{"key_not_found", nats.ErrKeyNotFound, false, true, true},
		{"wrap_mismatch", fmt.Errorf("wrap: %w", mismatch), true, false, true},
		{"wrap_knf", fmt.Errorf("wrap: %w", nats.ErrKeyNotFound), false, true, true},
		{"deep_wrap_knf", fmt.Errorf("a: %w", fmt.Errorf("b: %w", nats.ErrKeyNotFound)), false, true, true},
		{"deep_wrap_mismatch", fmt.Errorf("a: %w", fmt.Errorf("b: %w", mismatch)), true, false, true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			gotMisLower := isMismatchErr(tc.err)
			gotMisUpper := IsMismatchErr(tc.err)
			gotKNFLower := isKeyNotFoundError(tc.err)
			gotKNFUpper := IsKeyNotFoundError(tc.err)
			gotRetry := IsRetryable(tc.err)

			if gotMisLower != tc.wantMis {
				t.Errorf("isMismatchErr: got %v want %v", gotMisLower, tc.wantMis)
			}
			if gotMisUpper != tc.wantMis {
				t.Errorf("IsMismatchErr: got %v want %v", gotMisUpper, tc.wantMis)
			}
			if gotMisLower != gotMisUpper {
				t.Errorf("mismatch lower/upper disagree: lower=%v upper=%v", gotMisLower, gotMisUpper)
			}
			if gotKNFLower != tc.wantKNF {
				t.Errorf("isKeyNotFoundError: got %v want %v", gotKNFLower, tc.wantKNF)
			}
			if gotKNFUpper != tc.wantKNF {
				t.Errorf("IsKeyNotFoundError: got %v want %v", gotKNFUpper, tc.wantKNF)
			}
			if gotKNFLower != gotKNFUpper {
				t.Errorf("knf lower/upper disagree: lower=%v upper=%v", gotKNFLower, gotKNFUpper)
			}
			if gotRetry != tc.wantRetry {
				t.Errorf("IsRetryable: got %v want %v", gotRetry, tc.wantRetry)
			}
		})
	}
}
