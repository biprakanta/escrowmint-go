package escrowmint

import "errors"

var (
	ErrInsufficientQuota            = errors.New("insufficient quota")
	ErrReservationNotFound          = errors.New("reservation not found")
	ErrReservationExpired           = errors.New("reservation expired")
	ErrReservationAlreadyCommitted  = errors.New("reservation already committed")
	ErrDuplicateIdempotencyConflict = errors.New("duplicate idempotency conflict")
	ErrInvalidAmount                = errors.New("invalid amount")
	ErrInvalidTTL                   = errors.New("invalid ttl")
	ErrInvalidOwner                 = errors.New("invalid owner")
	ErrLeaseNotFound                = errors.New("lease not found")
	ErrLeaseExpired                 = errors.New("lease expired")
	ErrLeaseAlreadyReleased         = errors.New("lease already released")
	ErrLeaseOwnershipMismatch       = errors.New("lease ownership mismatch")
	ErrBackendUnavailable           = errors.New("backend unavailable")
	ErrCorruptState                 = errors.New("corrupt state")
)
