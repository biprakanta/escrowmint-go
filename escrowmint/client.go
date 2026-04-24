package escrowmint

import "context"

type Config struct {
	Addr             string
	KeyPrefix        string
	IdempotencyTTLMS int64
}

type Client struct {
	cfg Config
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
	return &Client{cfg: cfg}, nil
}

func (c *Client) Config() Config {
	return c.cfg
}
