package escrowmint

import (
	"context"
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
	if client.Config().KeyPrefix != "escrowmint" {
		t.Fatalf("unexpected key prefix: %q", client.Config().KeyPrefix)
	}
	if client.Config().Addr != "localhost:6379" {
		t.Fatalf("unexpected addr: %q", client.Config().Addr)
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

func TestReserveRejectsInvalidInputs(t *testing.T) {
	client := newEmptyClient(t)

	if _, err := client.Reserve(context.Background(), "wallet:1", 0, 1000, ReserveOptions{}); !errors.Is(err, ErrInvalidAmount) {
		t.Fatalf("expected ErrInvalidAmount, got %v", err)
	}
	if _, err := client.Reserve(context.Background(), "wallet:1", 1, 0, ReserveOptions{}); !errors.Is(err, ErrInvalidTTL) {
		t.Fatalf("expected ErrInvalidTTL, got %v", err)
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
	if !errors.Is(mapRedisError(errors.New("ERR INVALID_TTL")), ErrInvalidTTL) {
		t.Fatal("expected invalid ttl mapping")
	}
	if !errors.Is(mapRedisError(errors.New("ERR INSUFFICIENT_QUOTA")), ErrInsufficientQuota) {
		t.Fatal("expected insufficient quota mapping")
	}
	if !errors.Is(mapRedisError(errors.New("ERR DUPLICATE_IDEMPOTENCY_CONFLICT")), ErrDuplicateIdempotencyConflict) {
		t.Fatal("expected duplicate idempotency mapping")
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
	if !errors.Is(mapRedisError(errors.New("random")), ErrBackendUnavailable) {
		t.Fatal("expected generic error to map to backend unavailable")
	}
}

func newEmptyClient(t *testing.T) *Client {
	t.Helper()

	client, err := NewClient(context.Background(), Config{})
	if err != nil {
		t.Fatalf("NewClient returned error: %v", err)
	}
	return client
}

func newTestClient(t *testing.T, redisURL string) *Client {
	t.Helper()

	client, err := NewClient(context.Background(), Config{URL: redisURL})
	if err != nil {
		t.Fatalf("NewClient returned error: %v", err)
	}
	return client
}

func seedAvailable(t *testing.T, redisURL string, stateKey string, amount int64) {
	t.Helper()

	rdb := redis.NewClient(mustParseRedisURL(t, redisURL))
	if err := rdb.Del(context.Background(), strings.Replace(stateKey, ":state", ":reservations", 1)).Err(); err != nil {
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
