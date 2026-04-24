package escrowmint

import (
	"context"
	"testing"
)

func TestNewClientDefaults(t *testing.T) {
	client, err := NewClient(context.Background(), Config{})
	if err != nil {
		t.Fatalf("NewClient returned error: %v", err)
	}
	if client.Config().KeyPrefix != "escrowmint" {
		t.Fatalf("unexpected key prefix: %q", client.Config().KeyPrefix)
	}
}
