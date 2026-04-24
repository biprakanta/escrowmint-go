<p align="center">
  <img src="logo-transparent.png" alt="EscrowMint logo" width="180">
</p>

<h1 align="center">EscrowMint Go</h1>

<p align="center">Exact, Redis-backed bounded consumption for shared quotas.</p>

<p align="center">
  <a href="https://github.com/biprakanta/escrowmint-go/actions/workflows/ci.yml"><img alt="CI" src="https://img.shields.io/github/actions/workflow/status/biprakanta/escrowmint-go/ci.yml?branch=main&label=CI"></a>
  <img alt="Coverage" src="https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/biprakanta/escrowmint-go/badges/coverage.json">
  <a href="https://github.com/biprakanta/escrowmint-go/blob/main/LICENSE"><img alt="License" src="https://img.shields.io/github/license/biprakanta/escrowmint-go"></a>
  <a href="https://pkg.go.dev/github.com/biprakanta/escrowmint-go/escrowmint"><img alt="Go Reference" src="https://pkg.go.dev/badge/github.com/biprakanta/escrowmint-go/escrowmint.svg"></a>
</p>

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

## Chunk Lease Path

EscrowMint Go also ships an explicit chunk-lease lifecycle for hot resources:

```go
lease, err := client.AllocateChunk(ctx, "wallet:123", 100, escrowmint.AllocateChunkOptions{
	OwnerID: "worker-a",
	TTLMS:   30000,
})
if err != nil {
	log.Fatal(err)
}

result, err := client.ConsumeChunk(ctx, "wallet:123", lease.LeaseID, 5, "worker-a")
if err != nil {
	log.Fatal(err)
}

_, _ = result, lease
```

This is the authoritative distributed chunk path. It keeps chunk state in Redis and supports expiry reclaim, renew, release, and worker ownership checks.

## How It Works

- Redis remains the source of truth for each resource.
- Lua scripts make each operation atomic.
- Reservations move units from `available` to `reserved`.
- Pending reservations are indexed by expiry time in Redis.
- Expired reservations are reclaimed lazily in bounded batches on the next touch of that resource.
- Terminal reservation outcomes are moved into short-lived receipt keys so the hot reservation hash stays small.

## Direct Path and Chunk Lease Path

EscrowMint currently ships both models.

The direct path is the shared-resource path:

- `TryConsume`, `Reserve`, `Commit`, and `Cancel`
- exact bounded updates against the resource's shared state
- the simplest way to get correctness and crash recovery

The chunk lease path adds a worker-owned lease layer on top of that model:

- `AllocateChunk`, `ConsumeChunk`, `RenewChunk`, `ReleaseChunk`, and `GetChunk`
- explicit escrow or chunk allocation per worker
- better control over hot-resource ownership, refill, expiry, and reclaim
- more operational complexity than the direct path

Choose the direct path when you want the simplest exact path.

Choose the chunk lease path when a resource benefits from explicit worker-level quota management.

The current chunk lease implementation is an authoritative Redis-backed lease lifecycle. It improves the state model for hot resources, but it does not automatically become a no-Redis local fast path. If you want fewer Redis round trips than the shipped chunk API provides, you can layer an in-process chunk consumer on top of the authoritative lease lifecycle.

See [docs/CHUNK_LEASES.md](docs/CHUNK_LEASES.md).

## Development

```bash
go test ./...
go test ./... -cover
go mod tidy
gofmt -w ./...
```

Notes:

- module path is [go.mod](go.mod)
- tests use Docker-backed Redis integration cases

## Release Process

EscrowMint Go uses Conventional Commits and Release Please for semantic versioning and release notes.

- `fix:` -> patch release
- `feat:` -> minor release
- `feat!:` or `BREAKING CHANGE:` -> major release

When releasable commits land on `main`, Release Please opens or updates a release PR. Merging that PR updates [CHANGELOG.md](CHANGELOG.md), creates the `vX.Y.Z` tag, and creates the GitHub release notes. The existing tag workflow then verifies the tagged release commit.

## Docs

- [Direct Path API](docs/DIRECT_API.md)
- [Architecture](docs/ARCHITECTURE.md)
- [Chunk Lease Design](docs/CHUNK_LEASES.md)
- [Lua Script Notes](scripts/README.md)
- [Changelog](CHANGELOG.md)
- [Contributing](CONTRIBUTING.md)
