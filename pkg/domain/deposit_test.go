package domain

import (
	"errors"
	"testing"

	"github.com/shopspring/decimal"
)

func TestParseAndValidateDepositEvent(t *testing.T) {
	tests := []struct {
		name           string
		payload        string
		wantErr        bool
		expectedReason string
		expectedAmount string
	}{
		{
			name: "Valid USDT deposit event",
			payload: `{
				"event_id": "evt-1001",
				"user_id": 7,
				"currency": "USDT",
				"amount": "125.50",
				"created_at": "2026-10-04T10:00:00Z"
			}`,
			wantErr:        false,
			expectedAmount: "125.5",
		},
		{
			name: "Valid BTC deposit with high precision (8 decimals)",
			payload: `{
				"event_id": "evt-1002",
				"user_id": 20,
				"currency": "BTC",
				"amount": "0.00045678",
				"created_at": "2026-10-04T10:00:00Z"
			}`,
			wantErr:        false,
			expectedAmount: "0.00045678",
		},
		{
			name: "Valid IRT deposit at lower user_id boundary (1)",
			payload: `{
				"event_id": "evt-1003",
				"user_id": 1,
				"currency": "IRT",
				"amount": "50000000",
				"created_at": "2026-10-04T10:00:00Z"
			}`,
			wantErr:        false,
			expectedAmount: "50000000",
		},
		{
			name:           "Invalid - Empty payload",
			payload:        `   `,
			wantErr:        true,
			expectedReason: "empty message payload",
		},
		{
			name:           "Invalid - Corrupted JSON syntax",
			payload:        `{"event_id": "evt-bad", "user_id": 7, broken_json`,
			wantErr:        true,
			expectedReason: "malformed JSON payload",
		},
		{
			name: "Invalid - Non-numeric amount string",
			payload: `{
				"event_id": "evt-1004",
				"user_id": 5,
				"currency": "USDT",
				"amount": "not-a-number",
				"created_at": "2026-10-04T10:00:00Z"
			}`,
			wantErr:        true,
			expectedReason: "malformed JSON payload",
		},
		{
			name: "Invalid - Empty event_id",
			payload: `{
				"event_id": "   ",
				"user_id": 7,
				"currency": "USDT",
				"amount": "100.00",
				"created_at": "2026-10-04T10:00:00Z"
			}`,
			wantErr:        true,
			expectedReason: "event_id is required",
		},
		{
			name: "Invalid - user_id less than 1 (0)",
			payload: `{
				"event_id": "evt-1005",
				"user_id": 0,
				"currency": "USDT",
				"amount": "100.00",
				"created_at": "2026-10-04T10:00:00Z"
			}`,
			wantErr:        true,
			expectedReason: "user_id must be between 1 and 20",
		},
		{
			name: "Invalid - user_id greater than 20 (21)",
			payload: `{
				"event_id": "evt-1006",
				"user_id": 21,
				"currency": "USDT",
				"amount": "100.00",
				"created_at": "2026-10-04T10:00:00Z"
			}`,
			wantErr:        true,
			expectedReason: "user_id must be between 1 and 20",
		},
		{
			name: "Invalid - Unsupported currency (DOGE)",
			payload: `{
				"event_id": "evt-1007",
				"user_id": 10,
				"currency": "DOGE",
				"amount": "100.00",
				"created_at": "2026-10-04T10:00:00Z"
			}`,
			wantErr:        true,
			expectedReason: "unsupported currency",
		},
		{
			name: "Invalid - Zero amount",
			payload: `{
				"event_id": "evt-1008",
				"user_id": 10,
				"currency": "USDT",
				"amount": "0",
				"created_at": "2026-10-04T10:00:00Z"
			}`,
			wantErr:        true,
			expectedReason: "amount must be strictly greater than zero",
		},
		{
			name: "Invalid - Negative amount",
			payload: `{
				"event_id": "evt-1009",
				"user_id": 10,
				"currency": "USDT",
				"amount": "-50.25",
				"created_at": "2026-10-04T10:00:00Z"
			}`,
			wantErr:        true,
			expectedReason: "amount must be strictly greater than zero",
		},
		{
			name: "Invalid - Exceeds 18 decimal places",
			payload: `{
				"event_id": "evt-1010",
				"user_id": 10,
				"currency": "BTC",
				"amount": "0.1234567890123456789",
				"created_at": "2026-10-04T10:00:00Z"
			}`,
			wantErr:        true,
			expectedReason: "amount precision exceeds maximum 18 decimal places",
		},
		{
			name: "Invalid - Exceeds maximum allowed limit",
			payload: `{
				"event_id": "evt-1011",
				"user_id": 10,
				"currency": "IRT",
				"amount": "1000000000000000000",
				"created_at": "2026-10-04T10:00:00Z"
			}`,
			wantErr:        true,
			expectedReason: "amount exceeds maximum allowed limit",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			event, err := ParseAndValidateDepositEvent([]byte(tt.payload))

			if tt.wantErr {
				if err == nil {
					t.Fatalf("expected error containing %q, but got nil", tt.expectedReason)
				}

				var invalidErr *InvalidMessageError
				if !errors.As(err, &invalidErr) {
					t.Fatalf("expected error of type *InvalidMessageError, got %T (%v)", err, err)
				}
				return
			}

			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			expectedDec, _ := decimal.NewFromString(tt.expectedAmount)
			if !event.Amount.Equal(expectedDec) {
				t.Errorf("expected amount %s, got %s", expectedDec.String(), event.Amount.String())
			}
		})
	}
}

func TestSentinelDuplicateError(t *testing.T) {
	wrappedErr := errors.Join(ErrDuplicateEvent)
	if !errors.Is(wrappedErr, ErrDuplicateEvent) {
		t.Fatalf("expected errors.Is to detect ErrDuplicateEvent")
	}
}
