// Package escrowmint provides Redis-backed bounded quota primitives for
// prepaid credits, reservations, top-ups, and worker-owned chunk leases.
//
// EscrowMint keeps Redis as the source of truth and uses Lua scripts to make
// each operation atomic for a single resource. Use TryConsume for exact
// bounded decrements, Reserve plus Commit or Cancel for crash-safe holds, TopUp
// to replenish a pool, and the chunk lease APIs when a hot resource needs an
// explicit worker-owned allocation lifecycle.
package escrowmint
