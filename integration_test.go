//go:build integration
// +build integration

package main

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

// TestLoadBalancingBehavior tests that messages are distributed across multiple HTTP consumers
func TestLoadBalancingBehavior(t *testing.T) {
	ctx := context.Background()

	// Start NSQd container
	nsqdContainer, nsqdAddress, nsqdHTTPAddress, err := startNSQd(ctx, t)
	if err != nil {
		t.Fatalf("Failed to start NSQd: %v", err)
	}
	defer nsqdContainer.Terminate(ctx)

	// Start our HTTP facade server
	facadePort, stopFacade := startFacadeServer(t, nsqdAddress, nsqdHTTPAddress)
	defer stopFacade()

	facadeURL := fmt.Sprintf("http://localhost:%d", facadePort)

	// Test parameters
	topic := "test-topic"
	channel := "test-channel"
	numMessages := 300
	numConsumers := 3

	// Track messages received by each consumer
	var consumersWg sync.WaitGroup
	consumerMessageCounts := make([]int32, numConsumers)
	consumerErrors := make([]error, numConsumers)

	// Start 3 HTTP SSE consumer connections
	for i := 0; i < numConsumers; i++ {
		consumersWg.Add(1)
		consumerID := i

		go func(id int) {
			defer consumersWg.Done()

			count, err := consumeMessages(facadeURL, topic, channel, numMessages)
			atomic.StoreInt32(&consumerMessageCounts[id], int32(count))
			consumerErrors[id] = err

			t.Logf("Consumer %d received %d messages", id, count)
		}(consumerID)
	}

	// Wait a bit for consumers to connect
	time.Sleep(2 * time.Second)

	// Publish 300 messages
	t.Logf("Publishing %d messages...", numMessages)
	err = publishMessages(facadeURL, topic, numMessages)
	if err != nil {
		t.Fatalf("Failed to publish messages: %v", err)
	}

	// Wait for all consumers to finish (with timeout)
	done := make(chan struct{})
	go func() {
		consumersWg.Wait()
		close(done)
	}()

	select {
	case <-done:
		t.Log("All consumers completed")
	case <-time.After(60 * time.Second):
		t.Fatal("Test timeout: consumers didn't finish in time")
	}

	// Check for errors
	for i, err := range consumerErrors {
		if err != nil {
			t.Errorf("Consumer %d error: %v", i, err)
		}
	}

	// Calculate total messages received
	var totalReceived int32
	for i, count := range consumerMessageCounts {
		totalReceived += count
		t.Logf("Consumer %d: %d messages", i, count)
	}

	// Verify all consumers got at least one message
	for i, count := range consumerMessageCounts {
		if count < 1 {
			t.Errorf("Consumer %d received no messages (expected at least 1)", i)
		}
	}

	// Verify total count equals published messages
	if totalReceived != int32(numMessages) {
		t.Errorf("Total messages received (%d) doesn't match published (%d)", totalReceived, numMessages)
	}

	t.Logf("✓ Total messages: %d/%d", totalReceived, numMessages)
	t.Logf("✓ All consumers received at least one message")
	t.Log("✓ Load balancing working correctly!")
}

// startNSQd starts an NSQd container and returns its address
func startNSQd(ctx context.Context, t *testing.T) (testcontainers.Container, string, string, error) {
	req := testcontainers.ContainerRequest{
		Image:        "nsqio/nsq:latest",
		Cmd:          []string{"/nsqd", "--broadcast-address=127.0.0.1"},
		ExposedPorts: []string{"4150/tcp", "4151/tcp"},
		WaitingFor:   wait.ForLog("TCP: listening on"),
	}

	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		return nil, "", "", err
	}

	// Get mapped ports
	tcpPort, err := container.MappedPort(ctx, "4150")
	if err != nil {
		return nil, "", "", err
	}

	httpPort, err := container.MappedPort(ctx, "4151")
	if err != nil {
		return nil, "", "", err
	}

	host, err := container.Host(ctx)
	if err != nil {
		return nil, "", "", err
	}

	nsqdAddress := fmt.Sprintf("%s:%s", host, tcpPort.Port())
	nsqdHTTPAddress := fmt.Sprintf("%s:%s", host, httpPort.Port())

	t.Logf("NSQd started at TCP=%s HTTP=%s", nsqdAddress, nsqdHTTPAddress)

	return container, nsqdAddress, nsqdHTTPAddress, nil
}

// startFacadeServer starts the HTTP facade server in a goroutine
func startFacadeServer(t *testing.T, nsqdAddr, nsqdHTTPAddr string) (int, func()) {
	// Use a random available port
	port := 18080

	// Set configuration
	*nsqdAddress = nsqdAddr
	*nsqdHTTPAddr = nsqdHTTPAddr
	*httpAddress = fmt.Sprintf(":%d", port)
	*bearerToken = "test-token"
	bearerTokenHash = [32]byte{} // Will be set by setupTestEnv

	// Initialize
	setupTestEnv()

	// Start server in background
	srv := &http.Server{
		Addr: *httpAddress,
	}

	// Setup routes
	http.HandleFunc("/api/topics/", authMiddleware(handleTopics))
	http.HandleFunc("/api/messages/", authMiddleware(handleMessages))
	http.HandleFunc("/api/consumers/", authMiddleware(handleConsumers))
	http.HandleFunc("/api/events", authMiddleware(handleConsumerEvents))
	http.HandleFunc("/admin/", authMiddleware(handleAdmin))

	go func() {
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			t.Logf("Server error: %v", err)
		}
	}()

	// Wait for server to be ready
	time.Sleep(1 * time.Second)

	t.Logf("Facade server started on port %d", port)

	stopFunc := func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		srv.Shutdown(ctx)
	}

	return port, stopFunc
}

// consumeMessages connects to SSE endpoint and consumes messages
func consumeMessages(facadeURL, topic, channel string, maxMessages int) (int, error) {
	url := fmt.Sprintf("%s/api/events?topic=%s&channel=%s", facadeURL, topic, channel)

	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return 0, err
	}
	req.Header.Set("Authorization", "Bearer test-token")

	client := &http.Client{Timeout: 0} // No timeout for SSE
	resp, err := client.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return 0, fmt.Errorf("unexpected status: %d", resp.StatusCode)
	}

	count := 0
	scanner := bufio.NewScanner(resp.Body)

	// Read SSE events
	for scanner.Scan() {
		line := scanner.Text()

		// SSE data line
		if strings.HasPrefix(line, "data: ") {
			data := strings.TrimPrefix(line, "data: ")

			var msg map[string]interface{}
			if err := json.Unmarshal([]byte(data), &msg); err != nil {
				continue
			}

			messageID, ok := msg["id"].(string)
			if !ok {
				continue
			}

			// Finish the message
			finishURL := fmt.Sprintf("%s/api/messages/%s/finish", facadeURL, messageID)
			finishReq, _ := http.NewRequest("POST", finishURL, nil)
			finishReq.Header.Set("Authorization", "Bearer test-token")

			finishResp, err := http.DefaultClient.Do(finishReq)
			if err == nil {
				finishResp.Body.Close()
			}

			count++
			if count >= maxMessages {
				return count, nil
			}
		}
	}

	return count, scanner.Err()
}

// publishMessages publishes messages to the facade
func publishMessages(facadeURL, topic string, count int) error {
	url := fmt.Sprintf("%s/api/topics/%s/messages/batch", facadeURL, topic)

	// Build batch of messages
	messages := make([]map[string]interface{}, count)
	for i := 0; i < count; i++ {
		messages[i] = map[string]interface{}{
			"id":   i,
			"data": fmt.Sprintf("message-%d", i),
		}
	}

	payload := map[string]interface{}{
		"messages": messages,
	}

	body, err := json.Marshal(payload)
	if err != nil {
		return err
	}

	req, err := http.NewRequest("POST", url, strings.NewReader(string(body)))
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer test-token")
	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusCreated {
		return fmt.Errorf("publish failed with status: %d", resp.StatusCode)
	}

	return nil
}
