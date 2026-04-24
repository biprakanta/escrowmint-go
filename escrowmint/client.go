package escrowmint

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	redis "github.com/redis/go-redis/v9"
)

type Config struct {
	Addr             string
	URL              string
	KeyPrefix        string
	IdempotencyTTLMS int64
}

type ConsumeOptions struct {
	IdempotencyKey string
}

type ReserveOptions struct {
	ReservationID string
}

type Client struct {
	cfg        Config
	redis      redis.UniversalClient
	tryConsume *redis.Script
	reserve    *redis.Script
	commit     *redis.Script
	cancel     *redis.Script
	getState   *redis.Script
}

func NewClient(_ context.Context, cfg Config) (*Client, error) {
	if cfg.Addr == "" {
		cfg.Addr = "localhost:6379"
	}
	if cfg.KeyPrefix == "" {
		cfg.KeyPrefix = "escrowmint"
	}
	if cfg.IdempotencyTTLMS == 0 {
		cfg.IdempotencyTTLMS = 86400000
	}

	rdb, err := newRedisClient(cfg)
	if err != nil {
		return nil, err
	}

	return &Client{
		cfg:        cfg,
		redis:      rdb,
		tryConsume: redis.NewScript(tryConsumeLua),
		reserve:    redis.NewScript(reserveLua),
		commit:     redis.NewScript(commitLua),
		cancel:     redis.NewScript(cancelLua),
		getState:   redis.NewScript(getStateLua),
	}, nil
}

func (c *Client) Config() Config {
	return c.cfg
}

func (c *Client) TryConsume(ctx context.Context, resource string, amount int64, opts ConsumeOptions) (ConsumeResult, error) {
	if amount <= 0 {
		return ConsumeResult{}, ErrInvalidAmount
	}

	rawResult, err := c.tryConsume.Run(
		ctx,
		c.redis,
		[]string{
			c.stateKey(resource),
			c.reservationsKey(resource),
			c.idempotencyKey(resource, opts.IdempotencyKey),
		},
		amount,
		newOperationID(),
		c.cfg.IdempotencyTTLMS,
		requestFingerprint(resource, amount),
	).Text()
	if err != nil {
		return ConsumeResult{}, mapRedisError(err)
	}

	var result ConsumeResult
	if err := json.Unmarshal([]byte(rawResult), &result); err != nil {
		return ConsumeResult{}, fmt.Errorf("decode try_consume result: %w", err)
	}
	return result, nil
}

func (c *Client) Reserve(ctx context.Context, resource string, amount int64, ttlMS int64, opts ReserveOptions) (Reservation, error) {
	if amount <= 0 {
		return Reservation{}, ErrInvalidAmount
	}
	if ttlMS <= 0 {
		return Reservation{}, ErrInvalidTTL
	}

	reservationID := opts.ReservationID
	if reservationID == "" {
		reservationID = newReservationID()
	}

	rawResult, err := c.reserve.Run(
		ctx,
		c.redis,
		[]string{c.stateKey(resource), c.reservationsKey(resource)},
		amount,
		ttlMS,
		reservationID,
	).Text()
	if err != nil {
		return Reservation{}, mapRedisError(err)
	}

	var reservation Reservation
	if err := json.Unmarshal([]byte(rawResult), &reservation); err != nil {
		return Reservation{}, fmt.Errorf("decode reserve result: %w", err)
	}
	return reservation, nil
}

func (c *Client) Commit(ctx context.Context, resource string, reservationID string) (ConsumeResult, error) {
	rawResult, err := c.commit.Run(
		ctx,
		c.redis,
		[]string{c.stateKey(resource), c.reservationsKey(resource)},
		reservationID,
		newOperationID(),
	).Text()
	if err != nil {
		return ConsumeResult{}, mapRedisError(err)
	}

	var result ConsumeResult
	if err := json.Unmarshal([]byte(rawResult), &result); err != nil {
		return ConsumeResult{}, fmt.Errorf("decode commit result: %w", err)
	}
	return result, nil
}

func (c *Client) Cancel(ctx context.Context, resource string, reservationID string) (bool, error) {
	rawResult, err := c.cancel.Run(
		ctx,
		c.redis,
		[]string{c.stateKey(resource), c.reservationsKey(resource)},
		reservationID,
	).Text()
	if err != nil {
		return false, mapRedisError(err)
	}

	var payload struct {
		Canceled bool `json:"canceled"`
	}
	if err := json.Unmarshal([]byte(rawResult), &payload); err != nil {
		return false, fmt.Errorf("decode cancel result: %w", err)
	}
	return payload.Canceled, nil
}

func (c *Client) GetState(ctx context.Context, resource string) (ResourceState, error) {
	rawResult, err := c.getState.Run(
		ctx,
		c.redis,
		[]string{c.stateKey(resource), c.reservationsKey(resource)},
	).Text()
	if err != nil {
		return ResourceState{}, mapRedisError(err)
	}

	var payload struct {
		Available int64 `json:"available"`
		Reserved  int64 `json:"reserved"`
		Version   int64 `json:"version"`
	}
	if err := json.Unmarshal([]byte(rawResult), &payload); err != nil {
		return ResourceState{}, fmt.Errorf("decode get_state result: %w", err)
	}

	return ResourceState{
		Resource:  resource,
		Available: payload.Available,
		Reserved:  payload.Reserved,
		Version:   payload.Version,
	}, nil
}

func (c *Client) stateKey(resource string) string {
	return fmt.Sprintf("%s:{%s}:state", c.cfg.KeyPrefix, resource)
}

func (c *Client) reservationsKey(resource string) string {
	return fmt.Sprintf("%s:{%s}:reservations", c.cfg.KeyPrefix, resource)
}

func (c *Client) idempotencyKey(resource string, idempotencyKey string) string {
	if idempotencyKey == "" {
		return ""
	}
	return fmt.Sprintf("%s:{%s}:idem:%s", c.cfg.KeyPrefix, resource, idempotencyKey)
}

func newRedisClient(cfg Config) (redis.UniversalClient, error) {
	if cfg.URL != "" {
		opts, err := redis.ParseURL(cfg.URL)
		if err != nil {
			return nil, fmt.Errorf("parse redis url: %w", err)
		}
		return redis.NewClient(opts), nil
	}

	return redis.NewClient(&redis.Options{
		Addr: cfg.Addr,
		DB:   0,
	}), nil
}

func mapRedisError(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
		return fmt.Errorf("%w: %v", ErrBackendUnavailable, err)
	}

	switch {
	case strings.Contains(err.Error(), "INVALID_AMOUNT"):
		return ErrInvalidAmount
	case strings.Contains(err.Error(), "INVALID_TTL"):
		return ErrInvalidTTL
	case strings.Contains(err.Error(), "INSUFFICIENT_QUOTA"):
		return ErrInsufficientQuota
	case strings.Contains(err.Error(), "DUPLICATE_IDEMPOTENCY_CONFLICT"):
		return ErrDuplicateIdempotencyConflict
	case strings.Contains(err.Error(), "RESERVATION_NOT_FOUND"):
		return ErrReservationNotFound
	case strings.Contains(err.Error(), "RESERVATION_EXPIRED"):
		return ErrReservationExpired
	case strings.Contains(err.Error(), "RESERVATION_ALREADY_COMMITTED"):
		return ErrReservationAlreadyCommitted
	}

	var redisErr redis.Error
	if errors.As(err, &redisErr) {
		return mapRedisError(errors.New(redisErr.Error()))
	}

	return fmt.Errorf("%w: %v", ErrBackendUnavailable, err)
}

func requestFingerprint(resource string, amount int64) string {
	sum := sha256.Sum256([]byte(fmt.Sprintf("%s:%d", resource, amount)))
	return hex.EncodeToString(sum[:])
}

func newOperationID() string {
	return fmt.Sprintf("%d", time.Now().UnixNano())
}

func newReservationID() string {
	return fmt.Sprintf("res-%d", time.Now().UnixNano())
}
