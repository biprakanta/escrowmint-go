# V1 API

This document defines the minimal v1 public API for EscrowMint Go.

## Design Rules

- every mutating operation must be atomic
- every operation should have deterministic failure semantics
- idempotent retry should be first-class
- API names should reflect quota semantics, not Redis internals

## Domain Types

### Resource

A logical shared quota bucket, such as:

- `wallet:123`
- `campaign:456`
- `inventory:item-42`

### Amount

A positive integer unit to consume or reserve.

V1 intentionally uses integer arithmetic only.

### Reservation

A temporary hold against available quota that expires automatically unless committed or canceled.

### Idempotency Key

A caller-provided stable key for retry-safe mutation requests.

## Go API

```go
package escrowmint

import "context"

type ConsumeResult struct {
    Applied     bool
    Remaining   int64
    OperationID string
}

type Reservation struct {
    ReservationID string
    Resource      string
    Amount        int64
    ExpiresAtMS   int64
    Status        string
}

type ResourceState struct {
    Resource  string
    Available int64
    Reserved  int64
    Version   int64
}

type Client interface {
    TryConsume(ctx context.Context, resource string, amount int64, opts ConsumeOptions) (ConsumeResult, error)
    Reserve(ctx context.Context, resource string, amount int64, ttlMs int64, opts ReserveOptions) (Reservation, error)
    Commit(ctx context.Context, resource string, reservationID string) (ConsumeResult, error)
    Cancel(ctx context.Context, resource string, reservationID string) (bool, error)
    GetState(ctx context.Context, resource string) (ResourceState, error)
}
```

## Error Model

V1 should use typed errors instead of string matching.

Common errors:

- `ErrInsufficientQuota`
- `ErrReservationNotFound`
- `ErrReservationExpired`
- `ErrReservationAlreadyCommitted`
- `ErrDuplicateIdempotencyConflict`
- `ErrCorruptState`
- `ErrInvalidAmount`
- `ErrInvalidTTL`
- `ErrBackendUnavailable`

## Semantics

### `TryConsume`

- succeeds only if `available >= amount`
- permanently burns quota
- returns `Applied=false` on insufficient quota
- if an idempotency key is reused with the same request, returns the original result
- if an idempotency key is reused with a conflicting request shape, returns `ErrDuplicateIdempotencyConflict`

### `Reserve`

- succeeds only if `available >= amount`
- moves units from available to reserved
- creates a reservation with expiry
- expired reservations are reclaimed lazily on the next mutation or `GetState`
- active reservations are indexed by expiry time so reclaim does not require a full reservation scan
- same reservation ID must be safe to retry

### `Commit`

- turns a valid live reservation into permanent consumption
- must be idempotent
- expired reservations must not commit
- terminal results are preserved in short-lived receipt keys for retry safety

### `Cancel`

- releases reserved units back to available
- must be safe to retry

### `GetState`

- returns current logical view for one resource
- performs bounded lazy expiry reclaim for that resource before returning
- v1 does not promise a globally linearizable read across multiple resources

## Recommended Defaults

- idempotency TTL: 24 hours
- reservation TTL: caller-defined, with a sane minimum and maximum
- all resource operations isolated by resource key

## What V1 Should Avoid

- floating-point amounts
- multi-resource atomic transactions
- implicit background workers as a hard requirement
- lock-based APIs as the main surface
