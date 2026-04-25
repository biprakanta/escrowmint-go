package escrowmint_test

import (
	"context"
	"fmt"
	"log"

	"github.com/biprakanta/escrowmint-go/escrowmint"
)

func ExampleClient_TryConsume() {
	ctx := context.Background()
	client, err := escrowmint.NewClient(ctx, escrowmint.Config{
		URL: "redis://localhost:6379/0",
	})
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	result, err := client.TryConsume(ctx, "wallet:123", 5, escrowmint.ConsumeOptions{
		IdempotencyKey: "req-001",
	})
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println(result.Applied, result.Remaining)
}

func ExampleClient_TopUp() {
	ctx := context.Background()
	client, err := escrowmint.NewClient(ctx, escrowmint.Config{
		URL: "redis://localhost:6379/0",
	})
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	result, err := client.TopUp(ctx, "wallet:123", 25, escrowmint.TopUpOptions{
		IdempotencyKey: "credit-001",
	})
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println(result.Added, result.Available)
}
