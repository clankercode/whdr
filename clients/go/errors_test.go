package whdrsub

import (
	"errors"
	"testing"
)

func TestIsFatalClassification(t *testing.T) {
	fatal := []error{
		ErrAuth,
		ErrRevoked,
		&HandlerError{Err: errors.New("boom")},
		&CursorStoreError{Err: errors.New("disk full")},
		&ConfigError{Msg: "bad url"},
	}
	for _, err := range fatal {
		if !isFatal(err) {
			t.Errorf("isFatal(%v) = false, want true", err)
		}
	}
	transient := []error{
		nil,
		ErrConnClosed,
		&HTTPError{Status: 503},
		errReconnect,
		errors.Join(ErrConnClosed, errors.New("reset by peer")),
	}
	for _, err := range transient {
		if isFatal(err) {
			t.Errorf("isFatal(%v) = true, want false", err)
		}
	}
}

func TestErrorWrappingUnwraps(t *testing.T) {
	inner := errors.New("root cause")
	he := &HandlerError{Err: inner}
	if !errors.Is(he, inner) {
		t.Fatal("HandlerError should unwrap to inner")
	}
	ce := &CursorStoreError{Err: inner}
	if !errors.Is(ce, inner) {
		t.Fatal("CursorStoreError should unwrap to inner")
	}
}
