package domain

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/shopspring/decimal"
)

const (
	CurrencyUSDT = "USDT"
	CurrencyBTC  = "BTC"
	CurrencyIRT  = "IRT"
)

const (
	MinUserID        = 1
	MaxUserID        = 20
	MaxDecimalPlaces = -18
)

var maxAllowedAmount = decimal.New(1, 18)

type DepositEvent struct {
	EventID   string          `json:"event_id"`
	UserID    int             `json:"user_id"`
	Currency  string          `json:"currency"`
	Amount    decimal.Decimal `json:"amount"`
	CreatedAt time.Time       `json:"created_at"`
}

type DLQMessage struct {
	EventID        string    `json:"event_id,omitempty"`
	ErrorCode      string    `json:"error_code,omitempty"`
	SourceStream   string    `json:"source_stream,omitempty"`
	Subject        string    `json:"subject,omitempty"`
	StreamSequence uint64    `json:"stream_sequence,omitempty"`
	OriginalEvent  string    `json:"original_event"`
	Reason         string    `json:"reason"`
	Timestamp      time.Time `json:"timestamp"`
}

type LedgerDiscrepancy struct {
	UserID          int             `json:"user_id"`
	Currency        string          `json:"currency"`
	ExpectedBalance decimal.Decimal `json:"expected_balance"`
	ActualBalance   decimal.Decimal `json:"actual_balance"`
	Difference      decimal.Decimal `json:"difference"`
}

func ParseAndValidateDepositEvent(rawPayload []byte) (*DepositEvent, error) {
	if len(strings.TrimSpace(string(rawPayload))) == 0 {
		return nil, NewInvalidMessageError("", "empty message payload", nil)
	}

	var event DepositEvent
	if err := json.Unmarshal(rawPayload, &event); err != nil {
		return nil, NewInvalidMessageError("", fmt.Sprintf("malformed JSON payload: %v", err), err)
	}

	if err := event.Validate(); err != nil {
		return nil, err
	}

	return &event, nil
}

func (e *DepositEvent) Validate() error {
	if strings.TrimSpace(e.EventID) == "" {
		return NewInvalidMessageError("", "event_id is required and cannot be empty", nil)
	}

	if e.UserID < MinUserID || e.UserID > MaxUserID {
		return NewInvalidMessageError(
			e.EventID,
			fmt.Sprintf("user_id must be between %d and %d, got %d", MinUserID, MaxUserID, e.UserID),
			nil,
		)
	}

	if !isSupportedCurrency(e.Currency) {
		return NewInvalidMessageError(
			e.EventID,
			fmt.Sprintf("unsupported currency %q: allowed values are USDT, BTC, IRT", e.Currency),
			nil,
		)
	}

	if !e.Amount.IsPositive() {
		return NewInvalidMessageError(
			e.EventID,
			fmt.Sprintf("amount must be strictly greater than zero, got %s", e.Amount.String()),
			nil,
		)
	}

	if e.Amount.Exponent() < MaxDecimalPlaces {
		return NewInvalidMessageError(
			e.EventID,
			fmt.Sprintf("amount precision exceeds maximum 18 decimal places, got %s", e.Amount.String()),
			nil,
		)
	}

	if e.Amount.GreaterThanOrEqual(maxAllowedAmount) {
		return NewInvalidMessageError(
			e.EventID,
			fmt.Sprintf("amount exceeds maximum allowed limit, got %s", e.Amount.String()),
			nil,
		)
	}

	if e.CreatedAt.IsZero() {
		return NewInvalidMessageError(
			e.EventID,
			"created_at is required and cannot be zero",
			nil,
		)
	}

	return nil
}

func isSupportedCurrency(currency string) bool {
	switch currency {
	case CurrencyUSDT, CurrencyBTC, CurrencyIRT:
		return true
	default:
		return false
	}
}
