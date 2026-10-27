package domain

import (
	"bytes"
	"encoding/json"
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/shopspring/decimal"
)

const (
	CurrencyUSDT = "USDT"
	CurrencyBTC  = "BTC"
	CurrencyIRT  = "IRT"

	MinUserID = 1
	MaxUserID = 20

	MinEventIDLen       = 1
	MaxEventIDLen       = 128
	MaxDecimalScale     = 18
	MaxDecimalPrecision = 36
)

var eventIDRegex = regexp.MustCompile(`^[a-zA-Z0-9\-_:.]{1,128}$`)

type DepositEvent struct {
	EventID   string          `json:"event_id"`
	UserID    int             `json:"user_id"`
	Currency  string          `json:"currency"`
	Amount    decimal.Decimal `json:"amount"`
	CreatedAt time.Time       `json:"created_at"`
}

type DLQMessage struct {
	EventID        string    `json:"event_id"`
	ErrorCode      string    `json:"error_code"`
	SourceStream   string    `json:"source_stream"`
	Subject        string    `json:"subject"`
	StreamSequence uint64    `json:"stream_sequence"`
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

// ValidateEventID checks length (1..128) and allowed characters (alphanumeric and -_:. ).
func ValidateEventID(eventID string) error {
	if len(eventID) < MinEventIDLen || len(eventID) > MaxEventIDLen {
		return fmt.Errorf("event_id length must be between %d and %d characters", MinEventIDLen, MaxEventIDLen)
	}
	if !eventIDRegex.MatchString(eventID) {
		return fmt.Errorf("event_id contains invalid characters; allowed: a-z, A-Z, 0-9, and -_:.")
	}
	return nil
}

// ParseAndValidateAmount ensures the amount is a valid positive decimal string with at most 18 fractional digits.
func ParseAndValidateAmount(rawAmount string) (decimal.Decimal, error) {
	trimmed := strings.TrimSpace(rawAmount)
	if trimmed == "" {
		return decimal.Zero, fmt.Errorf("amount is required")
	}
	if strings.ContainsAny(trimmed, "eE") {
		return decimal.Zero, fmt.Errorf("amount must be in standard decimal format without scientific notation")
	}

	amt, err := decimal.NewFromString(trimmed)
	if err != nil {
		return decimal.Zero, fmt.Errorf("amount must be a valid decimal string: %w", err)
	}
	if !amt.IsPositive() {
		return decimal.Zero, fmt.Errorf("amount must be strictly greater than zero")
	}
	if amt.Exponent() < -MaxDecimalScale {
		return decimal.Zero, fmt.Errorf("amount cannot have more than %d decimal places", MaxDecimalScale)
	}
	// NUMERIC(36, 18) allows at most 18 integer digits
	maxAllowed := decimal.New(1, MaxDecimalPrecision-MaxDecimalScale)
	if amt.GreaterThanOrEqual(maxAllowed) {
		return decimal.Zero, fmt.Errorf("amount exceeds maximum allowed precision")
	}
	return amt, nil
}

// ValidateUserID checks that user_id falls within the allowed range.
func ValidateUserID(userID int) error {
	if userID < MinUserID || userID > MaxUserID {
		return fmt.Errorf("user_id must be between %d and %d", MinUserID, MaxUserID)
	}
	return nil
}

// ValidateCurrency checks that currency is supported.
func ValidateCurrency(currency string) error {
	switch currency {
	case CurrencyUSDT, CurrencyBTC, CurrencyIRT:
		return nil
	default:
		return fmt.Errorf("unsupported currency %q", currency)
	}
}

// NewValidatedDepositEvent constructs and validates a DepositEvent from raw inputs (used by Gateway & Publisher gRPC).
func NewValidatedDepositEvent(
	eventID string,
	userID int,
	currency string,
	rawAmount string,
	createdAt time.Time,
) (*DepositEvent, error) {
	if err := ValidateEventID(eventID); err != nil {
		return nil, NewInvalidMessageError(eventID, err.Error(), err)
	}
	if err := ValidateUserID(userID); err != nil {
		return nil, NewInvalidMessageError(eventID, err.Error(), err)
	}
	if err := ValidateCurrency(currency); err != nil {
		return nil, NewInvalidMessageError(eventID, err.Error(), err)
	}
	amt, err := ParseAndValidateAmount(rawAmount)
	if err != nil {
		return nil, NewInvalidMessageError(eventID, err.Error(), err)
	}
	if createdAt.IsZero() {
		return nil, NewInvalidMessageError(eventID, "created_at is required", nil)
	}

	return &DepositEvent{
		EventID:   eventID,
		UserID:    userID,
		Currency:  currency,
		Amount:    amt,
		CreatedAt: createdAt.UTC(),
	}, nil
}

// rawDepositEventJSON enforces that "amount" in JSON is strictly a JSON string (not a float/number).
type rawDepositEventJSON struct {
	EventID   string          `json:"event_id"`
	UserID    int             `json:"user_id"`
	Currency  string          `json:"currency"`
	Amount    json.RawMessage `json:"amount"`
	CreatedAt time.Time       `json:"created_at"`
}

// ParseAndValidateDepositEvent parses raw JSON bytes and validates all domain rules.
func ParseAndValidateDepositEvent(data []byte) (*DepositEvent, error) {
	var raw rawDepositEventJSON
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&raw); err != nil {
		return nil, NewInvalidMessageError("", fmt.Sprintf("malformed JSON payload: %v", err), err)
	}

	if len(raw.Amount) < 2 || raw.Amount[0] != '"' || raw.Amount[len(raw.Amount)-1] != '"' {
		return nil, NewInvalidMessageError(raw.EventID, "amount must be a JSON string", nil)
	}

	var amountStr string
	if err := json.Unmarshal(raw.Amount, &amountStr); err != nil {
		return nil, NewInvalidMessageError(raw.EventID, "invalid amount string", err)
	}

	return NewValidatedDepositEvent(raw.EventID, raw.UserID, raw.Currency, amountStr, raw.CreatedAt)
}
