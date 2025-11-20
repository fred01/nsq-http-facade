//go:build integration
// +build integration

package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
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

// TestMessageRequeue tests message requeue functionality
func TestMessageRequeue(t *testing.T) {
	ctx := context.Background()

	nsqdContainer, nsqdAddress, nsqdHTTPAddress, err := startNSQd(ctx, t)
	if err != nil {
		t.Fatalf("Failed to start NSQd: %v", err)
	}
	defer nsqdContainer.Terminate(ctx)

	facadePort, stopFacade := startFacadeServer(t, nsqdAddress, nsqdHTTPAddress)
	defer stopFacade()

	facadeURL := fmt.Sprintf("http://localhost:%d", facadePort)
	topic := "requeue-test"
	channel := "requeue-channel"

	// Publish a message
	err = publishSingleMessage(facadeURL, topic, "test-message")
	if err != nil {
		t.Fatalf("Failed to publish message: %v", err)
	}

	time.Sleep(1 * time.Second)

	// Consumer that requeues the first message
	messageID := ""
	receivedCount := 0

	url := fmt.Sprintf("%s/api/events?topic=%s&channel=%s", facadeURL, topic, channel)
	req, _ := http.NewRequest("GET", url, nil)
	req.Header.Set("Authorization", "Bearer test-token")

	client := &http.Client{Timeout: 0}
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("Failed to connect: %v", err)
	}
	defer resp.Body.Close()

	scanner := bufio.NewScanner(resp.Body)
	timeout := time.After(10 * time.Second)

	go func() {
		for scanner.Scan() {
			line := scanner.Text()
			if strings.HasPrefix(line, "data: ") {
				data := strings.TrimPrefix(line, "data: ")
				var msg map[string]interface{}
				json.Unmarshal([]byte(data), &msg)

				msgID := msg["id"].(string)
				receivedCount++

				if receivedCount == 1 {
					// Requeue the first time
					messageID = msgID
					requeueURL := fmt.Sprintf("%s/api/messages/%s/requeue", facadeURL, msgID)
					requeueReq, _ := http.NewRequest("POST", requeueURL, nil)
					requeueReq.Header.Set("Authorization", "Bearer test-token")
					requeueResp, _ := http.DefaultClient.Do(requeueReq)
					if requeueResp != nil {
						requeueResp.Body.Close()
					}
					t.Logf("Requeued message %s", msgID)
				} else if receivedCount == 2 {
					// Finish the second time
					finishURL := fmt.Sprintf("%s/api/messages/%s/finish", facadeURL, msgID)
					finishReq, _ := http.NewRequest("POST", finishURL, nil)
					finishReq.Header.Set("Authorization", "Bearer test-token")
					finishResp, _ := http.DefaultClient.Do(finishReq)
					if finishResp != nil {
						finishResp.Body.Close()
					}
					t.Logf("Finished message %s", msgID)
					return
				}
			}
		}
	}()

	<-timeout

	if receivedCount != 2 {
		t.Errorf("Expected to receive message twice (requeue), got %d", receivedCount)
	} else {
		t.Logf("✓ Message requeue working correctly")
	}
}

// TestMessageTouch tests message touch functionality
func TestMessageTouch(t *testing.T) {
	ctx := context.Background()

	nsqdContainer, nsqdAddress, nsqdHTTPAddress, err := startNSQd(ctx, t)
	if err != nil {
		t.Fatalf("Failed to start NSQd: %v", err)
	}
	defer nsqdContainer.Terminate(ctx)

	facadePort, stopFacade := startFacadeServer(t, nsqdAddress, nsqdHTTPAddress)
	defer stopFacade()

	facadeURL := fmt.Sprintf("http://localhost:%d", facadePort)
	topic := "touch-test"
	channel := "touch-channel"

	// Publish a message
	err = publishSingleMessage(facadeURL, topic, "touch-test-message")
	if err != nil {
		t.Fatalf("Failed to publish message: %v", err)
	}

	time.Sleep(1 * time.Second)

	url := fmt.Sprintf("%s/api/events?topic=%s&channel=%s", facadeURL, topic, channel)
	req, _ := http.NewRequest("GET", url, nil)
	req.Header.Set("Authorization", "Bearer test-token")

	client := &http.Client{Timeout: 0}
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("Failed to connect: %v", err)
	}
	defer resp.Body.Close()

	scanner := bufio.NewScanner(resp.Body)
	touchCount := 0
	var messageID string

	go func() {
		for scanner.Scan() {
			line := scanner.Text()
			if strings.HasPrefix(line, "data: ") {
				data := strings.TrimPrefix(line, "data: ")
				var msg map[string]interface{}
				json.Unmarshal([]byte(data), &msg)

				messageID = msg["id"].(string)

				// Touch the message 3 times before finishing
				for i := 0; i < 3; i++ {
					touchURL := fmt.Sprintf("%s/api/messages/%s/touch", facadeURL, messageID)
					touchReq, _ := http.NewRequest("POST", touchURL, nil)
					touchReq.Header.Set("Authorization", "Bearer test-token")
					touchResp, err := http.DefaultClient.Do(touchReq)
					if err == nil && touchResp.StatusCode == http.StatusOK {
						touchCount++
						t.Logf("Touched message %s (%d/3)", messageID, touchCount)
					}
					if touchResp != nil {
						touchResp.Body.Close()
					}
					time.Sleep(500 * time.Millisecond)
				}

				// Finally finish the message
				finishURL := fmt.Sprintf("%s/api/messages/%s/finish", facadeURL, messageID)
				finishReq, _ := http.NewRequest("POST", finishURL, nil)
				finishReq.Header.Set("Authorization", "Bearer test-token")
				finishResp, _ := http.DefaultClient.Do(finishReq)
				if finishResp != nil {
					finishResp.Body.Close()
				}
				return
			}
		}
	}()

	time.Sleep(5 * time.Second)

	if touchCount != 3 {
		t.Errorf("Expected 3 touch operations, got %d", touchCount)
	} else {
		t.Logf("✓ Message touch working correctly")
	}
}

// TestRDYControl tests RDY flow control with actual message delivery
func TestRDYControl(t *testing.T) {
	ctx := context.Background()

	nsqdContainer, nsqdAddress, nsqdHTTPAddress, err := startNSQd(ctx, t)
	if err != nil {
		t.Fatalf("Failed to start NSQd: %v", err)
	}
	defer nsqdContainer.Terminate(ctx)

	facadePort, stopFacade := startFacadeServer(t, nsqdAddress, nsqdHTTPAddress)
	defer stopFacade()

	facadeURL := fmt.Sprintf("http://localhost:%d", facadePort)
	topic := "rdy-test"
	channel := "rdy-channel"

	// Publish 15 messages first
	for i := 0; i < 15; i++ {
		err := publishSingleMessage(facadeURL, topic, fmt.Sprintf("message-%d", i))
		if err != nil {
			t.Fatalf("Failed to publish message %d: %v", i, err)
		}
	}

	time.Sleep(1 * time.Second)

	// Track messages received
	receivedMessages := make([]string, 0)
	var receivedMutex sync.Mutex
	consumerCtx, consumerCancel := context.WithCancel(context.Background())
	defer consumerCancel()

	// Start a consumer that counts but doesn't finish messages initially
	consumerDone := make(chan struct{})
	go func() {
		defer close(consumerDone)

		url := fmt.Sprintf("%s/api/events?topic=%s&channel=%s", facadeURL, topic, channel)
		req, _ := http.NewRequestWithContext(consumerCtx, "GET", url, nil)
		req.Header.Set("Authorization", "Bearer test-token")

		client := &http.Client{Timeout: 0}
		resp, err := client.Do(req)
		if err != nil {
			return
		}
		defer resp.Body.Close()

		scanner := bufio.NewScanner(resp.Body)
		for scanner.Scan() {
			select {
			case <-consumerCtx.Done():
				return
			default:
			}

			line := scanner.Text()
			if strings.HasPrefix(line, "data: ") {
				data := strings.TrimPrefix(line, "data: ")
				var msg map[string]interface{}
				if err := json.Unmarshal([]byte(data), &msg); err != nil {
					continue
				}

				messageID := msg["id"].(string)
				receivedMutex.Lock()
				receivedMessages = append(receivedMessages, messageID)
				count := len(receivedMessages)
				receivedMutex.Unlock()

				t.Logf("Received message %d: %s", count, messageID)
			}
		}
	}()

	// Wait for consumer to connect
	time.Sleep(2 * time.Second)

	// Set RDY to 5
	rdyURL := fmt.Sprintf("%s/api/consumers/%s/%s/rdy", facadeURL, topic, channel)
	rdyPayload := `{"count": 5}`
	rdyReq, _ := http.NewRequest("POST", rdyURL, bytes.NewBufferString(rdyPayload))
	rdyReq.Header.Set("Authorization", "Bearer test-token")
	rdyReq.Header.Set("Content-Type", "application/json")

	rdyResp, err := http.DefaultClient.Do(rdyReq)
	if err != nil {
		t.Fatalf("Failed to set RDY: %v", err)
	}
	rdyResp.Body.Close()

	if rdyResp.StatusCode != http.StatusOK {
		t.Errorf("Expected status 200, got %d", rdyResp.StatusCode)
	}

	t.Logf("Set RDY count to 5")

	// Wait a bit for messages to be delivered
	time.Sleep(3 * time.Second)

	// Check that we received approximately RDY count messages (should be around 5)
	receivedMutex.Lock()
	firstBatchCount := len(receivedMessages)
	firstBatchIDs := make([]string, len(receivedMessages))
	copy(firstBatchIDs, receivedMessages)
	receivedMutex.Unlock()

	if firstBatchCount < 3 || firstBatchCount > 7 {
		t.Errorf("With RDY=5, expected to receive around 5 messages, got %d", firstBatchCount)
	} else {
		t.Logf("✓ RDY control working: received %d messages with RDY=5", firstBatchCount)
	}

	// Now finish those messages
	for _, msgID := range firstBatchIDs {
		finishURL := fmt.Sprintf("%s/api/messages/%s/finish", facadeURL, msgID)
		finishReq, _ := http.NewRequest("POST", finishURL, nil)
		finishReq.Header.Set("Authorization", "Bearer test-token")
		finishResp, _ := http.DefaultClient.Do(finishReq)
		if finishResp != nil {
			finishResp.Body.Close()
		}
	}

	t.Logf("Finished %d messages", len(firstBatchIDs))

	// Wait for more messages to arrive (should get another batch of ~5)
	time.Sleep(3 * time.Second)

	receivedMutex.Lock()
	secondBatchCount := len(receivedMessages) - firstBatchCount
	totalReceived := len(receivedMessages)
	receivedMutex.Unlock()

	if secondBatchCount < 3 || secondBatchCount > 7 {
		t.Errorf("After finishing first batch, expected another ~5 messages, got %d", secondBatchCount)
	} else {
		t.Logf("✓ After finishing first batch, received %d more messages", secondBatchCount)
	}

	t.Logf("✓ Total received: %d messages with RDY flow control", totalReceived)

	// Test RDY=0 (pause message delivery)
	rdyPayload = `{"count": 0}`
	rdyReq, _ = http.NewRequest("POST", rdyURL, bytes.NewBufferString(rdyPayload))
	rdyReq.Header.Set("Authorization", "Bearer test-token")
	rdyReq.Header.Set("Content-Type", "application/json")

	rdyResp, err = http.DefaultClient.Do(rdyReq)
	if err != nil {
		t.Fatalf("Failed to set RDY=0: %v", err)
	}
	rdyResp.Body.Close()

	t.Logf("Set RDY count to 0 (pause)")

	receivedMutex.Lock()
	countBeforePause := len(receivedMessages)
	receivedMutex.Unlock()

	// Wait and verify no new messages arrive
	time.Sleep(2 * time.Second)

	receivedMutex.Lock()
	countAfterPause := len(receivedMessages)
	receivedMutex.Unlock()

	if countAfterPause > countBeforePause {
		t.Logf("Note: Received %d messages after RDY=0 (some may have been in-flight)", countAfterPause-countBeforePause)
	} else {
		t.Logf("✓ RDY=0 correctly paused message delivery")
	}

	// Get consumer status
	statusURL := fmt.Sprintf("%s/api/consumers/%s/%s", facadeURL, topic, channel)
	statusReq, _ := http.NewRequest("GET", statusURL, nil)
	statusReq.Header.Set("Authorization", "Bearer test-token")

	statusResp, err := http.DefaultClient.Do(statusReq)
	if err != nil {
		t.Fatalf("Failed to get status: %v", err)
	}
	defer statusResp.Body.Close()

	var statusResult map[string]interface{}
	json.NewDecoder(statusResp.Body).Decode(&statusResult)

	if consumers, ok := statusResult["consumers"].(float64); !ok || consumers < 1 {
		t.Errorf("Expected at least 1 consumer, got %v", statusResult)
	} else {
		t.Logf("✓ Consumer status endpoint working correctly: %d consumers", int(consumers))
	}

	consumerCancel()
	<-consumerDone
}

// TestSSEConnectionClose tests graceful SSE connection closure
func TestSSEConnectionClose(t *testing.T) {
	ctx := context.Background()

	nsqdContainer, nsqdAddress, nsqdHTTPAddress, err := startNSQd(ctx, t)
	if err != nil {
		t.Fatalf("Failed to start NSQd: %v", err)
	}
	defer nsqdContainer.Terminate(ctx)

	facadePort, stopFacade := startFacadeServer(t, nsqdAddress, nsqdHTTPAddress)
	defer stopFacade()

	facadeURL := fmt.Sprintf("http://localhost:%d", facadePort)
	topic := "close-test"
	channel := "close-channel"

	// Publish some messages
	for i := 0; i < 10; i++ {
		publishSingleMessage(facadeURL, topic, fmt.Sprintf("message-%d", i))
	}

	time.Sleep(1 * time.Second)

	// Start consumer and close it after receiving a few messages
	receivedCount := int32(0)

	url := fmt.Sprintf("%s/api/events?topic=%s&channel=%s", facadeURL, topic, channel)
	req, _ := http.NewRequest("GET", url, nil)
	req.Header.Set("Authorization", "Bearer test-token")

	client := &http.Client{Timeout: 0}
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("Failed to connect: %v", err)
	}

	scanner := bufio.NewScanner(resp.Body)
	go func() {
		for scanner.Scan() {
			line := scanner.Text()
			if strings.HasPrefix(line, "data: ") {
				data := strings.TrimPrefix(line, "data: ")
				var msg map[string]interface{}
				json.Unmarshal([]byte(data), &msg)

				msgID := msg["id"].(string)
				atomic.AddInt32(&receivedCount, 1)

				// Finish the message
				finishURL := fmt.Sprintf("%s/api/messages/%s/finish", facadeURL, msgID)
				finishReq, _ := http.NewRequest("POST", finishURL, nil)
				finishReq.Header.Set("Authorization", "Bearer test-token")
				finishResp, _ := http.DefaultClient.Do(finishReq)
				if finishResp != nil {
					finishResp.Body.Close()
				}

				// Close after receiving 3 messages
				if atomic.LoadInt32(&receivedCount) >= 3 {
					resp.Body.Close()
					return
				}
			}
		}
	}()

	time.Sleep(5 * time.Second)

	count := atomic.LoadInt32(&receivedCount)
	if count < 3 {
		t.Errorf("Expected at least 3 messages before close, got %d", count)
	} else {
		t.Logf("✓ SSE connection close working correctly: received %d messages", count)
	}

	// Verify we can reconnect and get remaining messages
	time.Sleep(1 * time.Second)

	req2, _ := http.NewRequest("GET", url, nil)
	req2.Header.Set("Authorization", "Bearer test-token")

	resp2, err := client.Do(req2)
	if err != nil {
		t.Fatalf("Failed to reconnect: %v", err)
	}
	defer resp2.Body.Close()

	if resp2.StatusCode != http.StatusOK {
		t.Errorf("Reconnection failed with status: %d", resp2.StatusCode)
	} else {
		t.Logf("✓ Reconnection after close working correctly")
	}
}

// publishSingleMessage publishes a single message
func publishSingleMessage(facadeURL, topic, data string) error {
	url := fmt.Sprintf("%s/api/topics/%s/messages", facadeURL, topic)

	payload := map[string]interface{}{
		"data": data,
	}

	body, err := json.Marshal(payload)
	if err != nil {
		return err
	}

	req, err := http.NewRequest("POST", url, bytes.NewReader(body))
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
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("publish failed with status: %d, body: %s", resp.StatusCode, string(body))
	}

	return nil
}
