# EscrowMint Go

EscrowMint Go is the Go client library for exact, Redis-backed bounded consumption.

It is designed for cases where many threads, processes, or services need to consume from a shared global quota without allowing the value to go below zero.

Examples:

- prepaid credit consumption
- inventory reservation
- budget caps
- worker permit pools
- campaign spend controls

## Why This Exists

There are many good Redis libraries for locks, semaphores, and rate limiting. There are far fewer Go libraries that focus on this narrower contract:

- consume from a shared quota
- never overspend
- support reservations with expiry
- stay fast under distributed contention
- expose clean application-level semantics

That is the gap EscrowMint Go is intended to fill.

## Core Model

EscrowMint Go is not a generic counter library. It is a quota and reservation library.

The minimal primitives are:

- `TryConsume`
- `Reserve`
- `Commit`
- `Cancel`
- `GetState`

These operations are executed atomically in Redis via Lua scripts or Redis Functions.

## Module

The module path is `github.com/biprakanta/escrowmint-go`.

The current implementation includes:

- `TryConsume`
- `Reserve`
- `Commit`
- `Cancel`
- `GetState`
- typed sentinel errors
- Docker-backed Redis integration tests
- TTL-based lazy reservation reclamation for crash recovery

## Development

EscrowMint Go uses native Go modules for dependency management.

Common commands:

- `go test ./...`
- `go test ./... -cover`
- `go mod tidy`
- `gofmt -w ./...`

Consumers will install it with `go get` once the repository is published and tagged.

The test suite starts a temporary Redis container with Docker. Running `go test ./...` requires a working local Docker installation.

Expired reservations are released lazily on the next mutation or `GetState` call for the same resource. V1 does not run a background sweeper.

## Publishing

Go modules are published by pushing the repository and creating semantic-version tags such as `v0.1.0`.

The intended module path is [go.mod](/Users/biprakantapal/Desktop/codex-plugins/escrowmint-go/go.mod): `github.com/biprakanta/escrowmint-go`.

## Proposed Repo Layout

```text
escrowmint-go/
  README.md
  LICENSE
  go.mod
  docs/
    ARCHITECTURE.md
    V1_API.md
    V2_ESCROW.md
  scripts/
    try_consume.lua
    README.md
  .github/
    workflows/
      ci.yml
  escrowmint/
```

## V1 Priorities

1. Exact bounded decrement with idempotency
2. Reservation lifecycle with TTL
3. Clean errors and observability hooks
4. Redis Cluster-safe keying strategy
5. Go module packaging and test automation

## Future Extensions

- v2 escrow or chunk allocation for very hot resources
- Go benchmarks
- background scavenging helpers
- metrics exporters
- Redis Functions as the default backend

See [docs/V1_API.md](/Users/biprakantapal/Desktop/codex-plugins/escrowmint-go/docs/V1_API.md), [docs/ARCHITECTURE.md](/Users/biprakantapal/Desktop/codex-plugins/escrowmint-go/docs/ARCHITECTURE.md), and [docs/V2_ESCROW.md](/Users/biprakantapal/Desktop/codex-plugins/escrowmint-go/docs/V2_ESCROW.md) for the current and next-step design.
