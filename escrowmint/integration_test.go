package escrowmint

import (
	"context"
	"fmt"
	"net"
	"os/exec"
	"testing"
	"time"

	redis "github.com/redis/go-redis/v9"
)

func startRedisContainer(t *testing.T) string {
	t.Helper()

	port := findOpenPort(t)
	containerName := fmt.Sprintf("escrowmint-go-test-%d", time.Now().UnixNano())
	cmd := exec.Command(
		"docker",
		"run",
		"--rm",
		"-d",
		"--name",
		containerName,
		"-p",
		fmt.Sprintf("%d:6379", port),
		"redis:7-alpine",
	)
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Skipf("unable to start Redis test container: %v (%s)", err, string(output))
	}

	t.Cleanup(func() {
		_ = exec.Command("docker", "rm", "-f", containerName).Run()
	})

	redisURL := fmt.Sprintf("redis://127.0.0.1:%d/0", port)
	rdb := redis.NewClient(mustParseRedisURL(t, redisURL))

	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		if err := rdb.Ping(context.Background()).Err(); err == nil {
			return redisURL
		}
		time.Sleep(250 * time.Millisecond)
	}

	t.Fatalf("Redis test container did not become ready in time")
	return ""
}

func findOpenPort(t *testing.T) int {
	t.Helper()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Listen returned error: %v", err)
	}
	defer listener.Close()

	return listener.Addr().(*net.TCPAddr).Port
}
