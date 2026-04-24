# EscrowMint Go

Exact, Redis-backed bounded consumption for shared quotas.

EscrowMint Go is for cases where many threads, processes, or services need to consume from the same global quota without letting it go below zero.

Good fits:

- prepaid credits
- inventory reservation
- budget caps
- worker permit pools
- campaign spend controls

## Why EscrowMint

EscrowMint is not a generic counter library. It is a quota and reservation library with application-level semantics:

- exact bounded decrement
- idempotent consume
- reservation with TTL
- commit and cancel flow
- crash recovery via lazy expiry reclaim

## Install

```bash
go get github.com/biprakanta/escrowmint-go/escrowmint
```

## Quickstart

```go
package main

import (
	"context"
	"log"

	"github.com/biprakanta/escrowmint-go/escrowmint"
)

func main() {
	ctx := context.Background()

	client, err := escrowmint.NewClient(ctx, escrowmint.Config{
		URL: "redis://localhost:6379/0",
	})
	if err != nil {
		log.Fatal(err)
	}

	result, err := client.TryConsume(ctx, "wallet:123", 5, escrowmint.ConsumeOptions{
		IdempotencyKey: "req-001",
	})
	if err != nil {
		log.Fatal(err)
	}

	log.Println(result.Applied, result.Remaining)
}
```

## Crash-Safe Reservation

```go
reservation, err := client.Reserve(ctx, "wallet:123", 10, 30000, escrowmint.ReserveOptions{})
if err != nil {
	log.Fatal(err)
}

result, err := client.Commit(ctx, "wallet:123", reservation.ReservationID)
if err != nil {
	log.Fatal(err)
}

_ = result
```

If a worker crashes after `Reserve` but before `Commit`, the held quota is released after TTL expiry on the next mutation or `GetState` call for that same resource.

## Current API

```go
client.TryConsume(ctx, resource, amount, opts)
client.Reserve(ctx, resource, amount, ttlMS, opts)
client.Commit(ctx, resource, reservationID)
client.Cancel(ctx, resource, reservationID)
client.GetState(ctx, resource)
```

## How It Works

- Redis remains the source of truth for each resource.
- Lua scripts make each operation atomic.
- Reservations move units from `available` to `reserved`.
- Expired reservations are reclaimed lazily on the next touch of that resource.

## V1 vs V2

Use the current v1 model for most workloads:

- exact correctness
- simple Redis-first deployment
- reservation lifecycle with crash recovery

Planned v2 is for very hot resources:

- escrow or chunk allocation per worker
- fewer Redis round trips on the hottest path
- more complexity in exchange for higher throughput

See [docs/V2_ESCROW.md](/Users/biprakantapal/Desktop/codex-plugins/escrowmint-go/docs/V2_ESCROW.md).

## Development

```bash
go test ./...
go test ./... -cover
go mod tidy
gofmt -w ./...
```

Notes:

- module path is [go.mod](/Users/biprakantapal/Desktop/codex-plugins/escrowmint-go/go.mod)
- tests use Docker-backed Redis integration cases
- current local coverage is over 90%

## Docs

- [V1 API](/Users/biprakantapal/Desktop/codex-plugins/escrowmint-go/docs/V1_API.md)
- [Architecture](/Users/biprakantapal/Desktop/codex-plugins/escrowmint-go/docs/ARCHITECTURE.md)
- [V2 Escrow Design](/Users/biprakantapal/Desktop/codex-plugins/escrowmint-go/docs/V2_ESCROW.md)
- [Lua Script Notes](/Users/biprakantapal/Desktop/codex-plugins/escrowmint-go/scripts/README.md)
