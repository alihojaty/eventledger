package domain

import (
	"errors"
	"fmt"
)

var ErrDuplicateEvent = errors.New("duplicate deposit event")

type InvalidMessageError struct {
	EventID string
	Reason  string
	Err     error
}

func (e *InvalidMessageError) Error() string {
	if e.EventID != "" {
		return fmt.Sprintf("invalid deposit message (event_id=%s): %s", e.EventID, e.Reason)
	}
	return fmt.Sprintf("invalid deposit message: %s", e.Reason)
}

func (e *InvalidMessageError) Unwrap() error {
	return e.Err
}

func NewInvalidMessageError(eventID, reason string, err error) *InvalidMessageError {
	return &InvalidMessageError{
		EventID: eventID,
		Reason:  reason,
		Err:     err,
	}
}
