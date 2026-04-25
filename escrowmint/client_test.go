package escrowmint

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	redis "github.com/redis/go-redis/v9"
)

func TestNewClientDefaults(t *testing.T) {
	client, err := NewClient(context.Background(), Config{})
	if err != nil {
		t.Fatalf("NewClient returned error: %v", err)
	}
	t.Cleanup(func() {
		if err := client.Close(); err != nil {
			t.Fatalf("Close returned error: %v", err)
		}
	})
	if client.Config().KeyPrefix != "escrowmint" {
		t.Fatalf("unexpected key prefix: %q", client.Config().KeyPrefix)
	}
	if client.Config().Addr != "localhost:6379" {
		t.Fatalf("unexpected addr: %q", client.Config().Addr)
	}
}

func TestClientCloseIsSafe(t *testing.T) {
	client := newEmptyClient(t)
	if err := client.Close(); err != nil {
		t.Fatalf("first Close returned error: %v", err)
	}
	if err := client.Close(); err != nil {
		t.Fatalf("second Close returned error: %v", err)
	}
}

func TestNewClientRejectsInvalidURL(t *testing.T) {
	_, err := NewClient(context.Background(), Config{URL: "://bad-url"})
	if err == nil {
		t.Fatal("expected invalid URL error")
	}
}

func TestTryConsumeRejectsInvalidAmount(t *testing.T) {
	client, err := NewClient(context.Background(), Config{})
	if err != nil {
		t.Fatalf("NewClient returned error: %v", err)
	}

	_, err = client.TryConsume(context.Background(), "wallet:1", 0, ConsumeOptions{})
	if !errors.Is(err, ErrInvalidAmount) {
		t.Fatalf("expected ErrInvalidAmount, got %v", err)
	}
}

func TestTopUpRejectsInvalidAmount(t *testing.T) {
	client := newEmptyClient(t)

	_, err := client.TopUp(context.Background(), "wallet:1", 0, TopUpOptions{})
	if !errors.Is(err, ErrInvalidAmount) {
		t.Fatalf("expected ErrInvalidAmount, got %v", err)
	}
}

func TestReserveRejectsInvalidInputs(t *testing.T) {
	client := newEmptyClient(t)

	if _, err := client.Reserve(context.Background(), "wallet:1", 0, 1000, ReserveOptions{}); !errors.Is(err, ErrInvalidAmount) {
		t.Fatalf("expected ErrInvalidAmount, got %v", err)
	}
	if _, err := client.Reserve(context.Background(), "wallet:1", 1, 0, ReserveOptions{}); !errors.Is(err, ErrInvalidTTL) {
		t.Fatalf("expected ErrInvalidTTL, got %v", err)
	}
	if _, err := client.AllocateChunk(context.Background(), "wallet:1", 1, AllocateChunkOptions{}); !errors.Is(err, ErrInvalidTTL) {
		t.Fatalf("expected ErrInvalidTTL, got %v", err)
	}
	if _, err := client.AllocateChunk(context.Background(), "wallet:1", 1, AllocateChunkOptions{TTLMS: 1000}); !errors.Is(err, ErrInvalidOwner) {
		t.Fatalf("expected ErrInvalidOwner, got %v", err)
	}
}

func TestGeneratedIDsUseExpectedPrefixes(t *testing.T) {
	if got := newOperationID(); !strings.HasPrefix(got, "op-") {
		t.Fatalf("unexpected operation id %q", got)
	}
	if got := newReservationID(); !strings.HasPrefix(got, "res-") {
		t.Fatalf("unexpected reservation id %q", got)
	}
	if got := newLeaseID(); !strings.HasPrefix(got, "lease-") {
		t.Fatalf("unexpected lease id %q", got)
	}
}

func TestTryConsumeAndGetStateIntegration(t *testing.T) {
	ctx := context.Background()
	redisURL := startRedisContainer(t)
	client := newTestClient(t, redisURL)
	seedAvailable(t, redisURL, client.stateKey("wallet:123"), 10)

	result, err := client.TryConsume(ctx, "wallet:123", 3, ConsumeOptions{})
	if err != nil {
		t.Fatalf("TryConsume returned error: %v", err)
	}
	state, err := client.GetState(ctx, "wallet:123")
	if err != nil {
		t.Fatalf("GetState returned error: %v", err)
	}

	if !result.Applied || result.Remaining != 7 {
		t.Fatalf("unexpected result: %+v", result)
	}
	if state.Available != 7 || state.Version != 1 {
		t.Fatalf("unexpected state: %+v", state)
	}
}

func TestTryConsumeReturnsAppliedFalseOnInsufficientQuota(t *testing.T) {
	ctx := context.Background()
	redisURL := startRedisContainer(t)
	client := newTestClient(t, redisURL)
	seedAvailable(t, redisURL, client.stateKey("wallet:124"), 2)

	result, err := client.TryConsume(ctx, "wallet:124", 5, ConsumeOptions{})
	if err != nil {
		t.Fatalf("TryConsume returned error: %v", err)
	}
	state, err := client.GetState(ctx, "wallet:124")
	if err != nil {
		t.Fatalf("GetState returned error: %v", err)
	}

	if result.Applied {
		t.Fatalf("expected applied false, got %+v", result)
	}
	if result.Remaining != 2 || state.Available != 2 || state.Version != 0 {
		t.Fatalf("unexpected state/result: %+v %+v", result, state)
	}
}

func TestTryConsumeIsIdempotentForRetries(t *testing.T) {
	ctx := context.Background()
	redisURL := startRedisContainer(t)
	client := newTestClient(t, redisURL)
	seedAvailable(t, redisURL, client.stateKey("wallet:125"), 10)

	first, err := client.TryConsume(ctx, "wallet:125", 4, ConsumeOptions{IdempotencyKey: "req-1"})
	if err != nil {
		t.Fatalf("first TryConsume returned error: %v", err)
	}
	second, err := client.TryConsume(ctx, "wallet:125", 4, ConsumeOptions{IdempotencyKey: "req-1"})
	if err != nil {
		t.Fatalf("second TryConsume returned error: %v", err)
	}
	state, err := client.GetState(ctx, "wallet:125")
	if err != nil {
		t.Fatalf("GetState returned error: %v", err)
	}

	if first != second {
		t.Fatalf("expected identical idempotent results, got %+v and %+v", first, second)
	}
	if state.Available != 6 || state.Version != 1 {
		t.Fatalf("unexpected state: %+v", state)
	}
}

func TestTryConsumeReplaysLegacyIdempotencyRecords(t *testing.T) {
	ctx := context.Background()
	redisURL := startRedisContainer(t)
	client := newTestClient(t, redisURL)
	seedAvailable(t, redisURL, client.stateKey("wallet:125-legacy"), 6)

	redisClient := redis.NewClient(mustParseRedisURL(t, redisURL))
	defer func() {
		if err := redisClient.Close(); err != nil {
			t.Fatalf("Close returned error: %v", err)
		}
	}()

	payload, err := json.Marshal(map[string]any{
		"request_fingerprint": requestFingerprint("", "wallet:125-legacy", 4),
		"applied":             true,
		"remaining":           int64(6),
		"operation_id":        "op-legacy",
	})
	if err != nil {
		t.Fatalf("json.Marshal returned error: %v", err)
	}
	if err := redisClient.Set(
		ctx,
		client.idempotencyKey("wallet:125-legacy", "req-legacy"),
		payload,
		time.Duration(client.Config().IdempotencyTTLMS)*time.Millisecond,
	).Err(); err != nil {
		t.Fatalf("Set returned error: %v", err)
	}

	result, err := client.TryConsume(ctx, "wallet:125-legacy", 4, ConsumeOptions{IdempotencyKey: "req-legacy"})
	if err != nil {
		t.Fatalf("TryConsume returned error: %v", err)
	}
	state, err := client.GetState(ctx, "wallet:125-legacy")
	if err != nil {
		t.Fatalf("GetState returned error: %v", err)
	}

	if !result.Applied || result.Remaining != 6 || result.OperationID != "op-legacy" {
		t.Fatalf("unexpected result: %+v", result)
	}
	if state.Available != 6 || state.Version != 0 {
		t.Fatalf("unexpected state: %+v", state)
	}
}

func TestTryConsumeRejectsConflictingIdempotencyReuse(t *testing.T) {
	ctx := context.Background()
	redisURL := startRedisContainer(t)
	client := newTestClient(t, redisURL)
	seedAvailable(t, redisURL, client.stateKey("wallet:126"), 10)

	if _, err := client.TryConsume(ctx, "wallet:126", 4, ConsumeOptions{IdempotencyKey: "req-2"}); err != nil {
		t.Fatalf("TryConsume returned error: %v", err)
	}

	_, err := client.TryConsume(ctx, "wallet:126", 5, ConsumeOptions{IdempotencyKey: "req-2"})
	if !errors.Is(err, ErrDuplicateIdempotencyConflict) {
		t.Fatalf("expected ErrDuplicateIdempotencyConflict, got %v", err)
	}
}

func TestTopUpAndGetStateIntegration(t *testing.T) {
	ctx := context.Background()
	redisURL := startRedisContainer(t)
	client := newTestClient(t, redisURL)
	seedAvailable(t, redisURL, client.stateKey("wallet:127"), 10)

	result, err := client.TopUp(ctx, "wallet:127", 4, TopUpOptions{})
	if err != nil {
		t.Fatalf("TopUp returned error: %v", err)
	}
	state, err := client.GetState(ctx, "wallet:127")
	if err != nil {
		t.Fatalf("GetState returned error: %v", err)
	}

	if result.Added != 4 || result.Available != 14 {
		t.Fatalf("unexpected result: %+v", result)
	}
	if state.Available != 14 || state.Version != 1 {
		t.Fatalf("unexpected state: %+v", state)
	}
}

func TestTopUpIsIdempotentForRetries(t *testing.T) {
	ctx := context.Background()
	redisURL := startRedisContainer(t)
	client := newTestClient(t, redisURL)
	seedAvailable(t, redisURL, client.stateKey("wallet:128"), 10)

	first, err := client.TopUp(ctx, "wallet:128", 4, TopUpOptions{IdempotencyKey: "top-up-1"})
	if err != nil {
		t.Fatalf("first TopUp returned error: %v", err)
	}
	second, err := client.TopUp(ctx, "wallet:128", 4, TopUpOptions{IdempotencyKey: "top-up-1"})
	if err != nil {
		t.Fatalf("second TopUp returned error: %v", err)
	}
	state, err := client.GetState(ctx, "wallet:128")
	if err != nil {
		t.Fatalf("GetState returned error: %v", err)
	}

	if first != second {
		t.Fatalf("expected identical top-up results, got %+v and %+v", first, second)
	}
	if state.Available != 14 || state.Version != 1 {
		t.Fatalf("unexpected state: %+v", state)
	}
}

func TestTopUpRejectsConflictingIdempotencyReuse(t *testing.T) {
	ctx := context.Background()
	redisURL := startRedisContainer(t)
	client := newTestClient(t, redisURL)
	seedAvailable(t, redisURL, client.stateKey("wallet:129"), 10)

	if _, err := client.TopUp(ctx, "wallet:129", 4, TopUpOptions{IdempotencyKey: "top-up-2"}); err != nil {
		t.Fatalf("TopUp returned error: %v", err)
	}

	_, err := client.TopUp(ctx, "wallet:129", 5, TopUpOptions{IdempotencyKey: "top-up-2"})
	if !errors.Is(err, ErrDuplicateIdempotencyConflict) {
		t.Fatalf("expected ErrDuplicateIdempotencyConflict, got %v", err)
	}
}

func TestTopUpReclaimsExpiredReservationsBeforeAdding(t *testing.T) {
	ctx := context.Background()
	redisURL := startRedisContainer(t)
	client := newTestClient(t, redisURL)
	seedAvailable(t, redisURL, client.stateKey("wallet:130"), 10)

	if _, err := client.Reserve(ctx, "wallet:130", 4, 100, ReserveOptions{ReservationID: "res-topup"}); err != nil {
		t.Fatalf("Reserve returned error: %v", err)
	}

	time.Sleep(150 * time.Millisecond)

	result, err := client.TopUp(ctx, "wallet:130", 3, TopUpOptions{})
	if err != nil {
		t.Fatalf("TopUp returned error: %v", err)
	}
	state, err := client.GetState(ctx, "wallet:130")
	if err != nil {
		t.Fatalf("GetState returned error: %v", err)
	}

	if result.Added != 3 || result.Available != 13 {
		t.Fatalf("unexpected result: %+v", result)
	}
	if state.Available != 13 || state.Reserved != 0 || state.Version != 3 {
		t.Fatalf("unexpected state: %+v", state)
	}
}

func TestReserveHoldsQuotaUntilCommit(t *testing.T) {
	ctx := context.Background()
	redisURL := startRedisContainer(t)
	client := newTestClient(t, redisURL)
	seedAvailable(t, redisURL, client.stateKey("wallet:200"), 10)

	reservation, err := client.Reserve(ctx, "wallet:200", 4, 5000, ReserveOptions{})
	if err != nil {
		t.Fatalf("Reserve returned error: %v", err)
	}
	state, err := client.GetState(ctx, "wallet:200")
	if err != nil {
		t.Fatalf("GetState returned error: %v", err)
	}

	if reservation.Resource != "wallet:200" || reservation.Amount != 4 || reservation.Status != "pending" {
		t.Fatalf("unexpected reservation: %+v", reservation)
	}
	if state.Available != 6 || state.Reserved != 4 || state.Version != 1 {
		t.Fatalf("unexpected state: %+v", state)
	}
}

func TestReserveRejectsInsufficientQuota(t *testing.T) {
	ctx := context.Background()
	redisURL := startRedisContainer(t)
	client := newTestClient(t, redisURL)
	seedAvailable(t, redisURL, client.stateKey("wallet:201"), 1)

	_, err := client.Reserve(ctx, "wallet:201", 2, 5000, ReserveOptions{})
	if !errors.Is(err, ErrInsufficientQuota) {
		t.Fatalf("expected ErrInsufficientQuota, got %v", err)
	}
}

func TestReserveIsIdempotentForSameReservationID(t *testing.T) {
	ctx := context.Background()
	redisURL := startRedisContainer(t)
	client := newTestClient(t, redisURL)
	seedAvailable(t, redisURL, client.stateKey("wallet:202"), 10)

	first, err := client.Reserve(ctx, "wallet:202", 3, 5000, ReserveOptions{ReservationID: "res-1"})
	if err != nil {
		t.Fatalf("first Reserve returned error: %v", err)
	}
	second, err := client.Reserve(ctx, "wallet:202", 3, 5000, ReserveOptions{ReservationID: "res-1"})
	if err != nil {
		t.Fatalf("second Reserve returned error: %v", err)
	}
	state, err := client.GetState(ctx, "wallet:202")
	if err != nil {
		t.Fatalf("GetState returned error: %v", err)
	}

	if first != second {
		t.Fatalf("expected identical reserve results, got %+v and %+v", first, second)
	}
	if state.Available != 7 || state.Reserved != 3 || state.Version != 1 {
		t.Fatalf("unexpected state: %+v", state)
	}
}

func TestReserveRejectsConflictingReservationReuse(t *testing.T) {
	ctx := context.Background()
	redisURL := startRedisContainer(t)
	client := newTestClient(t, redisURL)
	seedAvailable(t, redisURL, client.stateKey("wallet:203"), 10)

	if _, err := client.Reserve(ctx, "wallet:203", 3, 5000, ReserveOptions{ReservationID: "res-2"}); err != nil {
		t.Fatalf("Reserve returned error: %v", err)
	}

	_, err := client.Reserve(ctx, "wallet:203", 4, 5000, ReserveOptions{ReservationID: "res-2"})
	if !errors.Is(err, ErrDuplicateIdempotencyConflict) {
		t.Fatalf("expected ErrDuplicateIdempotencyConflict, got %v", err)
	}
}

func TestCommitBurnsReservedQuota(t *testing.T) {
	ctx := context.Background()
	redisURL := startRedisContainer(t)
	client := newTestClient(t, redisURL)
	rdb := redis.NewClient(mustParseRedisURL(t, redisURL))
	seedAvailable(t, redisURL, client.stateKey("wallet:204"), 10)

	reservation, err := client.Reserve(ctx, "wallet:204", 3, 5000, ReserveOptions{ReservationID: "res-3"})
	if err != nil {
		t.Fatalf("Reserve returned error: %v", err)
	}

	result, err := client.Commit(ctx, "wallet:204", reservation.ReservationID)
	if err != nil {
		t.Fatalf("Commit returned error: %v", err)
	}
	state, err := client.GetState(ctx, "wallet:204")
	if err != nil {
		t.Fatalf("GetState returned error: %v", err)
	}

	if !result.Applied || result.Remaining != 7 {
		t.Fatalf("unexpected commit result: %+v", result)
	}
	if state.Available != 7 || state.Reserved != 0 || state.Version != 2 {
		t.Fatalf("unexpected state: %+v", state)
	}
	if got := rdb.HLen(ctx, client.reservationsKey("wallet:204")).Val(); got != 0 {
		t.Fatalf("expected no pending reservations, got %d", got)
	}
	if got := rdb.ZCard(ctx, client.expiriesKey("wallet:204")).Val(); got != 0 {
		t.Fatalf("expected no expiry index entries, got %d", got)
	}
}

func TestCommitIsIdempotent(t *testing.T) {
	ctx := context.Background()
	redisURL := startRedisContainer(t)
	client := newTestClient(t, redisURL)
	seedAvailable(t, redisURL, client.stateKey("wallet:205"), 10)

	reservation, err := client.Reserve(ctx, "wallet:205", 3, 5000, ReserveOptions{ReservationID: "res-4"})
	if err != nil {
		t.Fatalf("Reserve returned error: %v", err)
	}

	first, err := client.Commit(ctx, "wallet:205", reservation.ReservationID)
	if err != nil {
		t.Fatalf("first Commit returned error: %v", err)
	}
	second, err := client.Commit(ctx, "wallet:205", reservation.ReservationID)
	if err != nil {
		t.Fatalf("second Commit returned error: %v", err)
	}

	if first != second {
		t.Fatalf("expected identical commit results, got %+v and %+v", first, second)
	}
}

func TestCommitMissingReservationRaises(t *testing.T) {
	ctx := context.Background()
	redisURL := startRedisContainer(t)
	client := newTestClient(t, redisURL)
	seedAvailable(t, redisURL, client.stateKey("wallet:206"), 10)

	_, err := client.Commit(ctx, "wallet:206", "missing")
	if !errors.Is(err, ErrReservationNotFound) {
		t.Fatalf("expected ErrReservationNotFound, got %v", err)
	}
}

func TestCancelReleasesReservedQuota(t *testing.T) {
	ctx := context.Background()
	redisURL := startRedisContainer(t)
	client := newTestClient(t, redisURL)
	rdb := redis.NewClient(mustParseRedisURL(t, redisURL))
	seedAvailable(t, redisURL, client.stateKey("wallet:207"), 10)

	reservation, err := client.Reserve(ctx, "wallet:207", 3, 5000, ReserveOptions{ReservationID: "res-5"})
	if err != nil {
		t.Fatalf("Reserve returned error: %v", err)
	}

	canceled, err := client.Cancel(ctx, "wallet:207", reservation.ReservationID)
	if err != nil {
		t.Fatalf("Cancel returned error: %v", err)
	}
	state, err := client.GetState(ctx, "wallet:207")
	if err != nil {
		t.Fatalf("GetState returned error: %v", err)
	}

	if !canceled {
		t.Fatal("expected cancel to return true")
	}
	if state.Available != 10 || state.Reserved != 0 || state.Version != 2 {
		t.Fatalf("unexpected state: %+v", state)
	}
	if got := rdb.HLen(ctx, client.reservationsKey("wallet:207")).Val(); got != 0 {
		t.Fatalf("expected no pending reservations, got %d", got)
	}
	if got := rdb.ZCard(ctx, client.expiriesKey("wallet:207")).Val(); got != 0 {
		t.Fatalf("expected no expiry index entries, got %d", got)
	}
}

func TestCancelReturnsFalseAfterCommit(t *testing.T) {
	ctx := context.Background()
	redisURL := startRedisContainer(t)
	client := newTestClient(t, redisURL)
	seedAvailable(t, redisURL, client.stateKey("wallet:208"), 10)

	reservation, err := client.Reserve(ctx, "wallet:208", 3, 5000, ReserveOptions{ReservationID: "res-6"})
	if err != nil {
		t.Fatalf("Reserve returned error: %v", err)
	}
	if _, err := client.Commit(ctx, "wallet:208", reservation.ReservationID); err != nil {
		t.Fatalf("Commit returned error: %v", err)
	}

	canceled, err := client.Cancel(ctx, "wallet:208", reservation.ReservationID)
	if err != nil {
		t.Fatalf("Cancel returned error: %v", err)
	}
	if canceled {
		t.Fatal("expected cancel to return false after commit")
	}
}

func TestExpiredReservationReleasesQuotaOnGetState(t *testing.T) {
	ctx := context.Background()
	redisURL := startRedisContainer(t)
	client := newTestClient(t, redisURL)
	rdb := redis.NewClient(mustParseRedisURL(t, redisURL))
	seedAvailable(t, redisURL, client.stateKey("wallet:209"), 10)

	if _, err := client.Reserve(ctx, "wallet:209", 4, 100, ReserveOptions{ReservationID: "res-7"}); err != nil {
		t.Fatalf("Reserve returned error: %v", err)
	}
	time.Sleep(200 * time.Millisecond)

	state, err := client.GetState(ctx, "wallet:209")
	if err != nil {
		t.Fatalf("GetState returned error: %v", err)
	}
	if state.Available != 10 || state.Reserved != 0 || state.Version != 2 {
		t.Fatalf("unexpected state: %+v", state)
	}
	if got := rdb.HLen(ctx, client.reservationsKey("wallet:209")).Val(); got != 0 {
		t.Fatalf("expected no pending reservations, got %d", got)
	}
	if got := rdb.ZCard(ctx, client.expiriesKey("wallet:209")).Val(); got != 0 {
		t.Fatalf("expected no expiry index entries, got %d", got)
	}
}

func TestCommitExpiredReservationRaises(t *testing.T) {
	ctx := context.Background()
	redisURL := startRedisContainer(t)
	client := newTestClient(t, redisURL)
	seedAvailable(t, redisURL, client.stateKey("wallet:210"), 10)

	if _, err := client.Reserve(ctx, "wallet:210", 4, 100, ReserveOptions{ReservationID: "res-8"}); err != nil {
		t.Fatalf("Reserve returned error: %v", err)
	}
	time.Sleep(200 * time.Millisecond)

	_, err := client.Commit(ctx, "wallet:210", "res-8")
	if !errors.Is(err, ErrReservationExpired) {
		t.Fatalf("expected ErrReservationExpired, got %v", err)
	}
}

func TestTryConsumeConcurrentRequestsDoNotOverspend(t *testing.T) {
	ctx := context.Background()
	redisURL := startRedisContainer(t)
	client := newTestClient(t, redisURL)
	seedAvailable(t, redisURL, client.stateKey("wallet:211"), 50)

	var wg sync.WaitGroup
	results := make(chan ConsumeResult, 10)
	errorsCh := make(chan error, 10)

	for range 10 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			result, err := client.TryConsume(ctx, "wallet:211", 7, ConsumeOptions{})
			if err != nil {
				errorsCh <- err
				return
			}
			results <- result
		}()
	}

	wg.Wait()
	close(results)
	close(errorsCh)

	for err := range errorsCh {
		t.Fatalf("TryConsume returned error: %v", err)
	}

	var appliedCount int
	for result := range results {
		if result.Applied {
			appliedCount++
		}
	}

	state, err := client.GetState(ctx, "wallet:211")
	if err != nil {
		t.Fatalf("GetState returned error: %v", err)
	}
	if appliedCount != 7 || state.Available != 1 {
		t.Fatalf("unexpected results/state: applied=%d state=%+v", appliedCount, state)
	}
}

func TestAllocateChunkHoldsQuotaUntilRelease(t *testing.T) {
	ctx := context.Background()
	redisURL := startRedisContainer(t)
	client := newTestClient(t, redisURL)
	seedAvailable(t, redisURL, client.stateKey("wallet:300"), 20)

	lease, err := client.AllocateChunk(ctx, "wallet:300", 6, AllocateChunkOptions{
		OwnerID: "worker-a",
		TTLMS:   5000,
	})
	if err != nil {
		t.Fatalf("AllocateChunk returned error: %v", err)
	}
	state, err := client.GetState(ctx, "wallet:300")
	if err != nil {
		t.Fatalf("GetState returned error: %v", err)
	}

	if lease.OwnerID != "worker-a" || lease.Granted != 6 || lease.Remaining != 6 || lease.Status != "active" {
		t.Fatalf("unexpected lease: %+v", lease)
	}
	if state.Available != 14 {
		t.Fatalf("unexpected state: %+v", state)
	}
}

func TestAllocateChunkIsIdempotentForSameLeaseID(t *testing.T) {
	ctx := context.Background()
	redisURL := startRedisContainer(t)
	client := newTestClient(t, redisURL)
	seedAvailable(t, redisURL, client.stateKey("wallet:301"), 20)

	first, err := client.AllocateChunk(ctx, "wallet:301", 5, AllocateChunkOptions{
		OwnerID: "worker-a",
		TTLMS:   5000,
		LeaseID: "lease-1",
	})
	if err != nil {
		t.Fatalf("first AllocateChunk returned error: %v", err)
	}
	second, err := client.AllocateChunk(ctx, "wallet:301", 5, AllocateChunkOptions{
		OwnerID: "worker-a",
		TTLMS:   5000,
		LeaseID: "lease-1",
	})
	if err != nil {
		t.Fatalf("second AllocateChunk returned error: %v", err)
	}

	if first != second {
		t.Fatalf("expected identical chunk leases, got %+v and %+v", first, second)
	}
}

func TestAllocateChunkRejectsReuseOfReleasedLeaseID(t *testing.T) {
	ctx := context.Background()
	redisURL := startRedisContainer(t)
	client := newTestClient(t, redisURL)
	seedAvailable(t, redisURL, client.stateKey("wallet:302"), 20)

	lease, err := client.AllocateChunk(ctx, "wallet:302", 5, AllocateChunkOptions{
		OwnerID: "worker-a",
		TTLMS:   5000,
		LeaseID: "lease-2",
	})
	if err != nil {
		t.Fatalf("AllocateChunk returned error: %v", err)
	}
	if _, err := client.ReleaseChunk(ctx, "wallet:302", lease.LeaseID, "worker-a"); err != nil {
		t.Fatalf("ReleaseChunk returned error: %v", err)
	}

	_, err = client.AllocateChunk(ctx, "wallet:302", 5, AllocateChunkOptions{
		OwnerID: "worker-a",
		TTLMS:   5000,
		LeaseID: "lease-2",
	})
	if !errors.Is(err, ErrLeaseAlreadyReleased) {
		t.Fatalf("expected ErrLeaseAlreadyReleased, got %v", err)
	}
}

func TestConsumeChunkUpdatesRemainingWithoutTouchingAvailable(t *testing.T) {
	ctx := context.Background()
	redisURL := startRedisContainer(t)
	client := newTestClient(t, redisURL)
	seedAvailable(t, redisURL, client.stateKey("wallet:303"), 20)

	lease, err := client.AllocateChunk(ctx, "wallet:303", 8, AllocateChunkOptions{
		OwnerID: "worker-a",
		TTLMS:   5000,
	})
	if err != nil {
		t.Fatalf("AllocateChunk returned error: %v", err)
	}

	result, err := client.ConsumeChunk(ctx, "wallet:303", lease.LeaseID, 3, "worker-a")
	if err != nil {
		t.Fatalf("ConsumeChunk returned error: %v", err)
	}
	refreshed, err := client.GetChunk(ctx, "wallet:303", lease.LeaseID)
	if err != nil {
		t.Fatalf("GetChunk returned error: %v", err)
	}
	state, err := client.GetState(ctx, "wallet:303")
	if err != nil {
		t.Fatalf("GetState returned error: %v", err)
	}

	if !result.Applied || result.Remaining != 5 {
		t.Fatalf("unexpected consume result: %+v", result)
	}
	if refreshed.Remaining != 5 {
		t.Fatalf("unexpected refreshed lease: %+v", refreshed)
	}
	if state.Available != 12 {
		t.Fatalf("unexpected state: %+v", state)
	}
}

func TestConsumeChunkReturnsAppliedFalseWhenLeaseRemainingIsLow(t *testing.T) {
	ctx := context.Background()
	redisURL := startRedisContainer(t)
	client := newTestClient(t, redisURL)
	seedAvailable(t, redisURL, client.stateKey("wallet:304"), 20)

	lease, err := client.AllocateChunk(ctx, "wallet:304", 4, AllocateChunkOptions{
		OwnerID: "worker-a",
		TTLMS:   5000,
	})
	if err != nil {
		t.Fatalf("AllocateChunk returned error: %v", err)
	}

	result, err := client.ConsumeChunk(ctx, "wallet:304", lease.LeaseID, 6, "worker-a")
	if err != nil {
		t.Fatalf("ConsumeChunk returned error: %v", err)
	}
	if result.Applied || result.Remaining != 4 {
		t.Fatalf("unexpected consume result: %+v", result)
	}
}

func TestReleaseChunkReturnsUnusedQuota(t *testing.T) {
	ctx := context.Background()
	redisURL := startRedisContainer(t)
	client := newTestClient(t, redisURL)
	rdb := redis.NewClient(mustParseRedisURL(t, redisURL))
	seedAvailable(t, redisURL, client.stateKey("wallet:305"), 20)

	lease, err := client.AllocateChunk(ctx, "wallet:305", 10, AllocateChunkOptions{
		OwnerID: "worker-a",
		TTLMS:   5000,
	})
	if err != nil {
		t.Fatalf("AllocateChunk returned error: %v", err)
	}
	if _, err := client.ConsumeChunk(ctx, "wallet:305", lease.LeaseID, 4, "worker-a"); err != nil {
		t.Fatalf("ConsumeChunk returned error: %v", err)
	}

	released, err := client.ReleaseChunk(ctx, "wallet:305", lease.LeaseID, "worker-a")
	if err != nil {
		t.Fatalf("ReleaseChunk returned error: %v", err)
	}
	state, err := client.GetState(ctx, "wallet:305")
	if err != nil {
		t.Fatalf("GetState returned error: %v", err)
	}

	if released.Status != "released" || released.Remaining != 6 {
		t.Fatalf("unexpected released lease: %+v", released)
	}
	if state.Available != 16 {
		t.Fatalf("unexpected state: %+v", state)
	}
	if got := rdb.HLen(ctx, client.chunkLeasesKey("wallet:305")).Val(); got != 0 {
		t.Fatalf("expected no active chunk leases, got %d", got)
	}
	if got := rdb.ZCard(ctx, client.chunkExpiriesKey("wallet:305")).Val(); got != 0 {
		t.Fatalf("expected no active chunk expiry entries, got %d", got)
	}
}

func TestReleaseChunkIsIdempotent(t *testing.T) {
	ctx := context.Background()
	redisURL := startRedisContainer(t)
	client := newTestClient(t, redisURL)
	seedAvailable(t, redisURL, client.stateKey("wallet:306"), 20)

	lease, err := client.AllocateChunk(ctx, "wallet:306", 5, AllocateChunkOptions{
		OwnerID: "worker-a",
		TTLMS:   5000,
	})
	if err != nil {
		t.Fatalf("AllocateChunk returned error: %v", err)
	}

	first, err := client.ReleaseChunk(ctx, "wallet:306", lease.LeaseID, "worker-a")
	if err != nil {
		t.Fatalf("first ReleaseChunk returned error: %v", err)
	}
	second, err := client.ReleaseChunk(ctx, "wallet:306", lease.LeaseID, "worker-a")
	if err != nil {
		t.Fatalf("second ReleaseChunk returned error: %v", err)
	}
	if first != second {
		t.Fatalf("expected identical release receipts, got %+v and %+v", first, second)
	}
}

func TestConsumeChunkRejectsWrongOwner(t *testing.T) {
	ctx := context.Background()
	redisURL := startRedisContainer(t)
	client := newTestClient(t, redisURL)
	seedAvailable(t, redisURL, client.stateKey("wallet:307"), 20)

	lease, err := client.AllocateChunk(ctx, "wallet:307", 5, AllocateChunkOptions{
		OwnerID: "worker-a",
		TTLMS:   5000,
	})
	if err != nil {
		t.Fatalf("AllocateChunk returned error: %v", err)
	}

	_, err = client.ConsumeChunk(ctx, "wallet:307", lease.LeaseID, 1, "worker-b")
	if !errors.Is(err, ErrLeaseOwnershipMismatch) {
		t.Fatalf("expected ErrLeaseOwnershipMismatch, got %v", err)
	}
}

func TestRenewChunkExtendsExpiry(t *testing.T) {
	ctx := context.Background()
	redisURL := startRedisContainer(t)
	client := newTestClient(t, redisURL)
	seedAvailable(t, redisURL, client.stateKey("wallet:308"), 20)

	lease, err := client.AllocateChunk(ctx, "wallet:308", 5, AllocateChunkOptions{
		OwnerID: "worker-a",
		TTLMS:   150,
	})
	if err != nil {
		t.Fatalf("AllocateChunk returned error: %v", err)
	}

	time.Sleep(50 * time.Millisecond)
	renewed, err := client.RenewChunk(ctx, "wallet:308", lease.LeaseID, "worker-a", 500)
	if err != nil {
		t.Fatalf("RenewChunk returned error: %v", err)
	}
	if renewed.ExpiresAtMS <= lease.ExpiresAtMS {
		t.Fatalf("expected expiry extension, got %+v then %+v", lease, renewed)
	}

	time.Sleep(200 * time.Millisecond)
	result, err := client.ConsumeChunk(ctx, "wallet:308", lease.LeaseID, 2, "worker-a")
	if err != nil {
		t.Fatalf("ConsumeChunk returned error: %v", err)
	}
	if !result.Applied {
		t.Fatalf("expected consume to apply after renew, got %+v", result)
	}
}

func TestExpiredChunkReleasesRemainingOnGetState(t *testing.T) {
	ctx := context.Background()
	redisURL := startRedisContainer(t)
	client := newTestClient(t, redisURL)
	seedAvailable(t, redisURL, client.stateKey("wallet:309"), 20)

	lease, err := client.AllocateChunk(ctx, "wallet:309", 10, AllocateChunkOptions{
		OwnerID: "worker-a",
		TTLMS:   100,
	})
	if err != nil {
		t.Fatalf("AllocateChunk returned error: %v", err)
	}
	if _, err := client.ConsumeChunk(ctx, "wallet:309", lease.LeaseID, 4, "worker-a"); err != nil {
		t.Fatalf("ConsumeChunk returned error: %v", err)
	}

	time.Sleep(200 * time.Millisecond)
	state, err := client.GetState(ctx, "wallet:309")
	if err != nil {
		t.Fatalf("GetState returned error: %v", err)
	}
	if state.Available != 16 {
		t.Fatalf("unexpected state: %+v", state)
	}

	_, err = client.ConsumeChunk(ctx, "wallet:309", lease.LeaseID, 1, "worker-a")
	if !errors.Is(err, ErrLeaseExpired) {
		t.Fatalf("expected ErrLeaseExpired, got %v", err)
	}
}

func TestGetChunkReturnsTerminalReceiptAfterRelease(t *testing.T) {
	ctx := context.Background()
	redisURL := startRedisContainer(t)
	client := newTestClient(t, redisURL)
	seedAvailable(t, redisURL, client.stateKey("wallet:310"), 20)

	lease, err := client.AllocateChunk(ctx, "wallet:310", 5, AllocateChunkOptions{
		OwnerID: "worker-a",
		TTLMS:   5000,
	})
	if err != nil {
		t.Fatalf("AllocateChunk returned error: %v", err)
	}
	if _, err := client.ConsumeChunk(ctx, "wallet:310", lease.LeaseID, 2, "worker-a"); err != nil {
		t.Fatalf("ConsumeChunk returned error: %v", err)
	}
	if _, err := client.ReleaseChunk(ctx, "wallet:310", lease.LeaseID, "worker-a"); err != nil {
		t.Fatalf("ReleaseChunk returned error: %v", err)
	}

	chunk, err := client.GetChunk(ctx, "wallet:310", lease.LeaseID)
	if err != nil {
		t.Fatalf("GetChunk returned error: %v", err)
	}
	if chunk.Status != "released" || chunk.Remaining != 3 {
		t.Fatalf("unexpected chunk receipt: %+v", chunk)
	}
}

func TestGetChunkMissingLeaseRaises(t *testing.T) {
	ctx := context.Background()
	redisURL := startRedisContainer(t)
	client := newTestClient(t, redisURL)
	seedAvailable(t, redisURL, client.stateKey("wallet:311"), 20)

	_, err := client.GetChunk(ctx, "wallet:311", "missing")
	if !errors.Is(err, ErrLeaseNotFound) {
		t.Fatalf("expected ErrLeaseNotFound, got %v", err)
	}
}

func TestBackendUnavailableFromBadAddress(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	client, err := NewClient(ctx, Config{Addr: "127.0.0.1:1"})
	if err != nil {
		t.Fatalf("NewClient returned error: %v", err)
	}

	_, err = client.GetState(ctx, "wallet:offline")
	if !errors.Is(err, ErrBackendUnavailable) {
		t.Fatalf("expected ErrBackendUnavailable, got %v", err)
	}
}

func TestMapRedisErrorBranches(t *testing.T) {
	if err := mapRedisError(nil); err != nil {
		t.Fatalf("expected nil error, got %v", err)
	}
	if !errors.Is(mapRedisError(context.DeadlineExceeded), ErrBackendUnavailable) {
		t.Fatal("expected deadline exceeded to map to backend unavailable")
	}
	if !errors.Is(mapRedisError(context.Canceled), ErrBackendUnavailable) {
		t.Fatal("expected canceled to map to backend unavailable")
	}
	if !errors.Is(mapRedisError(errors.New("ERR INVALID_AMOUNT")), ErrInvalidAmount) {
		t.Fatal("expected invalid amount mapping")
	}
	if !errors.Is(mapRedisError(errors.New("ERR INVALID_OWNER")), ErrInvalidOwner) {
		t.Fatal("expected invalid owner mapping")
	}
	if !errors.Is(mapRedisError(errors.New("ERR INVALID_TTL")), ErrInvalidTTL) {
		t.Fatal("expected invalid ttl mapping")
	}
	if !errors.Is(mapRedisError(errors.New("ERR INSUFFICIENT_QUOTA")), ErrInsufficientQuota) {
		t.Fatal("expected insufficient quota mapping")
	}
	if !errors.Is(mapRedisError(errors.New("ERR DUPLICATE_IDEMPOTENCY_CONFLICT")), ErrDuplicateIdempotencyConflict) {
		t.Fatal("expected duplicate idempotency mapping")
	}
	if !errors.Is(mapRedisError(errors.New("ERR CORRUPT_STATE")), ErrCorruptState) {
		t.Fatal("expected corrupt state mapping")
	}
	if !errors.Is(mapRedisError(errors.New("ERR RESERVATION_NOT_FOUND")), ErrReservationNotFound) {
		t.Fatal("expected reservation not found mapping")
	}
	if !errors.Is(mapRedisError(errors.New("ERR RESERVATION_EXPIRED")), ErrReservationExpired) {
		t.Fatal("expected reservation expired mapping")
	}
	if !errors.Is(mapRedisError(errors.New("ERR RESERVATION_ALREADY_COMMITTED")), ErrReservationAlreadyCommitted) {
		t.Fatal("expected reservation already committed mapping")
	}
	if !errors.Is(mapRedisError(errors.New("ERR LEASE_NOT_FOUND")), ErrLeaseNotFound) {
		t.Fatal("expected lease not found mapping")
	}
	if !errors.Is(mapRedisError(errors.New("ERR LEASE_EXPIRED")), ErrLeaseExpired) {
		t.Fatal("expected lease expired mapping")
	}
	if !errors.Is(mapRedisError(errors.New("ERR LEASE_ALREADY_RELEASED")), ErrLeaseAlreadyReleased) {
		t.Fatal("expected lease already released mapping")
	}
	if !errors.Is(mapRedisError(errors.New("ERR OWNER_MISMATCH")), ErrLeaseOwnershipMismatch) {
		t.Fatal("expected owner mismatch mapping")
	}
	if !errors.Is(mapRedisError(errors.New("random")), ErrBackendUnavailable) {
		t.Fatal("expected generic error to map to backend unavailable")
	}
}

func TestGetStateRaisesCorruptStateForMalformedReservationPayload(t *testing.T) {
	ctx := context.Background()
	redisURL := startRedisContainer(t)
	client := newTestClient(t, redisURL)
	rdb := redis.NewClient(mustParseRedisURL(t, redisURL))
	seedAvailable(t, redisURL, client.stateKey("wallet:212"), 5)

	if err := rdb.HSet(ctx, client.reservationsKey("wallet:212"), "bad", "{not-json").Err(); err != nil {
		t.Fatalf("HSet returned error: %v", err)
	}
	if err := rdb.ZAdd(ctx, client.expiriesKey("wallet:212"), redis.Z{
		Score:  1,
		Member: "bad",
	}).Err(); err != nil {
		t.Fatalf("ZAdd returned error: %v", err)
	}

	_, err := client.GetState(ctx, "wallet:212")
	if !errors.Is(err, ErrCorruptState) {
		t.Fatalf("expected ErrCorruptState, got %v", err)
	}
}

func TestCommitRaisesCorruptStateForMalformedReceipt(t *testing.T) {
	ctx := context.Background()
	redisURL := startRedisContainer(t)
	client := newTestClient(t, redisURL)
	rdb := redis.NewClient(mustParseRedisURL(t, redisURL))
	seedAvailable(t, redisURL, client.stateKey("wallet:213"), 5)

	if err := rdb.Set(ctx, client.receiptKey("wallet:213", "res-bad"), "{not-json", time.Minute).Err(); err != nil {
		t.Fatalf("Set returned error: %v", err)
	}

	_, err := client.Commit(ctx, "wallet:213", "res-bad")
	if !errors.Is(err, ErrCorruptState) {
		t.Fatalf("expected ErrCorruptState, got %v", err)
	}
}

func TestGetChunkRaisesCorruptStateForMalformedActiveLease(t *testing.T) {
	ctx := context.Background()
	redisURL := startRedisContainer(t)
	client := newTestClient(t, redisURL)
	rdb := redis.NewClient(mustParseRedisURL(t, redisURL))
	seedAvailable(t, redisURL, client.stateKey("wallet:312"), 5)

	if err := rdb.HSet(ctx, client.chunkLeasesKey("wallet:312"), "bad", "{not-json").Err(); err != nil {
		t.Fatalf("HSet returned error: %v", err)
	}
	if err := rdb.ZAdd(ctx, client.chunkExpiriesKey("wallet:312"), redis.Z{
		Score:  float64(time.Now().Add(time.Minute).UnixMilli()),
		Member: "bad",
	}).Err(); err != nil {
		t.Fatalf("ZAdd returned error: %v", err)
	}

	_, err := client.GetChunk(ctx, "wallet:312", "bad")
	if !errors.Is(err, ErrCorruptState) {
		t.Fatalf("expected ErrCorruptState, got %v", err)
	}
}

func newEmptyClient(t *testing.T) *Client {
	t.Helper()

	client, err := NewClient(context.Background(), Config{})
	if err != nil {
		t.Fatalf("NewClient returned error: %v", err)
	}
	t.Cleanup(func() {
		if err := client.Close(); err != nil {
			t.Fatalf("Close returned error: %v", err)
		}
	})
	return client
}

func newTestClient(t *testing.T, redisURL string) *Client {
	t.Helper()

	client, err := NewClient(context.Background(), Config{URL: redisURL})
	if err != nil {
		t.Fatalf("NewClient returned error: %v", err)
	}
	t.Cleanup(func() {
		if err := client.Close(); err != nil {
			t.Fatalf("Close returned error: %v", err)
		}
	})
	return client
}

func seedAvailable(t *testing.T, redisURL string, stateKey string, amount int64) {
	t.Helper()

	rdb := redis.NewClient(mustParseRedisURL(t, redisURL))
	if err := rdb.Del(context.Background(), strings.Replace(stateKey, ":state", ":reservations", 1)).Err(); err != nil {
		t.Fatalf("Del returned error: %v", err)
	}
	if err := rdb.Del(context.Background(), strings.Replace(stateKey, ":state", ":reservation_expiries", 1)).Err(); err != nil {
		t.Fatalf("Del returned error: %v", err)
	}
	if err := rdb.Del(context.Background(), strings.Replace(stateKey, ":state", ":chunk_leases", 1)).Err(); err != nil {
		t.Fatalf("Del returned error: %v", err)
	}
	if err := rdb.Del(context.Background(), strings.Replace(stateKey, ":state", ":chunk_lease_expiries", 1)).Err(); err != nil {
		t.Fatalf("Del returned error: %v", err)
	}
	if err := rdb.HSet(context.Background(), stateKey, map[string]any{
		"available": amount,
		"reserved":  0,
		"version":   0,
	}).Err(); err != nil {
		t.Fatalf("HSet returned error: %v", err)
	}
}

func mustParseRedisURL(t *testing.T, redisURL string) *redis.Options {
	t.Helper()

	opts, err := redis.ParseURL(redisURL)
	if err != nil {
		t.Fatalf("ParseURL returned error: %v", err)
	}
	return opts
}
