# Release Checklist

Use this checklist for the first stable public release and for future tagged releases.

## One-Time Setup

- Confirm GitHub Actions is allowed to create release PRs, tags, and releases
- Confirm the `badges` branch can be updated by CI
- Confirm `pkg.go.dev` can discover the public repository and module path

## Pre-Release Validation

- Ensure `main` is green in CI
- Run a local smoke test:
  - `go test ./...`
  - `go test ./... -cover`
  - `gofmt -w ./...`
- Confirm README install and quickstart examples still match the shipped API
- Confirm `go.mod` module path is final

## Release Execution

- Merge the current Release Please PR
- Confirm the `vX.Y.Z` tag is created
- Confirm the GitHub release is created with generated notes
- Confirm the tag-triggered verification workflow succeeds

## Post-Release Smoke Test

- In a clean module, run `go get github.com/biprakanta/escrowmint-go/escrowmint@vX.Y.Z`
- Import the package and execute a minimal `NewClient(...)` plus `TryConsume(...)` flow against Redis
- Confirm the module version resolves correctly in `go list -m`
- Confirm the README badge row and release links reflect the new release
