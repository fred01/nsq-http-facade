package main

import (
	"crypto/sha256"
	"crypto/subtle"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nsqio/go-nsq"
)

var (
	nsqdAddress  = flag.String("nsqd-address", "localhost:4150", "NSQd TCP address")
	nsqdHTTPAddr = flag.String("nsqd-http-address", "localhost:4151", "NSQd HTTP address")
	httpAddress  = flag.String("http-address", ":8080", "HTTP server address")
	bearerToken  = flag.String("bearer-token", "", "Bearer token for authentication (required)")

	producer              *nsq.Producer
	consumers             = make(map[string]*nsq.Consumer)
	consumersMutex        sync.RWMutex
	activeMessages        = make(map[string]*messageWithExpiry)
	activeMessagesMutex   sync.RWMutex
	bearerTokenHash       [32]byte
	messageCleanupTicker  *time.Ticker
	messageExpiryDuration = 5 * time.Minute
	consumerIDCounter     uint64
)

// messageWithExpiry wraps an NSQ message with an expiry time
type messageWithExpiry struct {
	message *nsq.Message
	expiry  time.Time
}

func main() {
	flag.Parse()

	if *bearerToken == "" {
		log.Fatalf("Bearer token is required. Use -bearer-token flag")
	}

	// Pre-calculate bearer token hash for constant-time comparison
	bearerTokenHash = sha256.Sum256([]byte(*bearerToken))

	// Initialize NSQ producer
	config := nsq.NewConfig()
	var err error
	producer, err = nsq.NewProducer(*nsqdAddress, config)
	if err != nil {
		log.Fatalf("Failed to create producer: %v", err)
	}
	defer producer.Stop()

	// Start background cleanup for expired messages
	messageCleanupTicker = time.NewTicker(30 * time.Second)
	defer messageCleanupTicker.Stop()
	go cleanupExpiredMessages()

	// Setup HTTP routes with authentication middleware
	http.HandleFunc("/api/topics/", authMiddleware(handleTopics))
	http.HandleFunc("/api/messages/", authMiddleware(handleMessages))
	http.HandleFunc("/api/consumers/", authMiddleware(handleConsumers))
	http.HandleFunc("/api/events", authMiddleware(handleConsumerEvents))
	http.HandleFunc("/admin/", authMiddleware(handleAdmin))

	log.Printf("Starting HTTP server on %s", *httpAddress)
	log.Printf("Connected to NSQd at %s", *nsqdAddress)
	if err := http.ListenAndServe(*httpAddress, nil); err != nil {
		log.Fatalf("Failed to start HTTP server: %v", err)
	}
}

// cleanupExpiredMessages removes expired messages from the activeMessages map
func cleanupExpiredMessages() {
	for range messageCleanupTicker.C {
		now := time.Now()
		activeMessagesMutex.Lock()
		for id, msgWithExpiry := range activeMessages {
			if now.After(msgWithExpiry.expiry) {
				// Message expired, requeue it
				msgWithExpiry.message.Requeue(-1)
				delete(activeMessages, id)
				log.Printf("Cleaned up expired message: %s", id)
			}
		}
		activeMessagesMutex.Unlock()
	}
}

// authMiddleware validates bearer token using constant-time comparison
func authMiddleware(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		authHeader := r.Header.Get("Authorization")
		if authHeader == "" {
			http.Error(w, "Authorization header required", http.StatusUnauthorized)
			return
		}

		const bearerPrefix = "Bearer "
		if !strings.HasPrefix(authHeader, bearerPrefix) {
			http.Error(w, "Invalid bearer token format", http.StatusUnauthorized)
			return
		}

		sentToken := authHeader[len(bearerPrefix):]
		sentTokenHash := sha256.Sum256([]byte(sentToken))

		if subtle.ConstantTimeCompare(sentTokenHash[:], bearerTokenHash[:]) != 1 {
			http.Error(w, "Invalid bearer token", http.StatusUnauthorized)
			return
		}

		next(w, r)
	}
}

// handleTopics handles REST operations for topics (POST for publishing)
// POST /api/topics/:topic/messages - publish single message (PUB)
// POST /api/topics/:topic/messages/batch - publish multiple messages (MPUB)
func handleTopics(w http.ResponseWriter, r *http.Request) {
	path := r.URL.Path[len("/api/topics/"):]

	if path == "" {
		http.Error(w, "Topic name required", http.StatusBadRequest)
		return
	}

	// Check if batch endpoint
	if len(path) > len("/messages/batch") && path[len(path)-len("/messages/batch"):] == "/messages/batch" {
		topic := path[:len(path)-len("/messages/batch")]
		handleMpub(w, r, topic)
		return
	}

	// Check if single message endpoint
	if len(path) > len("/messages") && path[len(path)-len("/messages"):] == "/messages" {
		topic := path[:len(path)-len("/messages")]
		handlePub(w, r, topic)
		return
	}

	http.Error(w, "Invalid endpoint", http.StatusNotFound)
}

// Message structure for JSON input
type Message struct {
	Data json.RawMessage `json:"data"`
}

// MultiMessage structure for MPUB
type MultiMessage struct {
	Messages []json.RawMessage `json:"messages"`
}

// handlePub handles single message publishing (PUB)
func handlePub(w http.ResponseWriter, r *http.Request, topic string) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var msg Message
	if err := json.NewDecoder(r.Body).Decode(&msg); err != nil {
		http.Error(w, fmt.Sprintf("Invalid JSON: %v", err), http.StatusBadRequest)
		return
	}

	// Convert JSON data to bytes
	if err := producer.Publish(topic, []byte(msg.Data)); err != nil {
		http.Error(w, fmt.Sprintf("Failed to publish: %v", err), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(map[string]string{"status": "ok", "topic": topic})
}

// handleMpub handles multiple message publishing (MPUB)
func handleMpub(w http.ResponseWriter, r *http.Request, topic string) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var msg MultiMessage
	if err := json.NewDecoder(r.Body).Decode(&msg); err != nil {
		http.Error(w, fmt.Sprintf("Invalid JSON: %v", err), http.StatusBadRequest)
		return
	}

	if len(msg.Messages) == 0 {
		http.Error(w, "At least one message is required", http.StatusBadRequest)
		return
	}

	// Convert JSON messages to bytes
	var messages [][]byte
	for _, m := range msg.Messages {
		messages = append(messages, []byte(m))
	}

	if err := producer.MultiPublish(topic, messages); err != nil {
		http.Error(w, fmt.Sprintf("Failed to publish: %v", err), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(map[string]interface{}{
		"status": "ok",
		"topic":  topic,
		"count":  len(messages),
	})
}

// handleConsumers handles REST operations for consumers
// POST /api/consumers/:topic/:channel/rdy - set RDY count
// GET /api/consumers/:topic/:channel - get consumer status
func handleConsumers(w http.ResponseWriter, r *http.Request) {
	path := r.URL.Path[len("/api/consumers/"):]
	parts := splitPath(path)

	if len(parts) < 2 {
		http.Error(w, "Topic and channel required", http.StatusBadRequest)
		return
	}

	topic := parts[0]
	channel := parts[1]

	if len(parts) == 3 && parts[2] == "rdy" {
		handleConsumerRdy(w, r, topic, channel)
		return
	}

	if len(parts) == 2 {
		if r.Method == http.MethodGet {
			handleConsumerStatus(w, r, topic, channel)
			return
		}
	}

	http.Error(w, "Invalid endpoint", http.StatusNotFound)
}

// handleConsumerStatus returns aggregated status of all consumers for a topic/channel
func handleConsumerStatus(w http.ResponseWriter, r *http.Request, topic, channel string) {
	prefix := fmt.Sprintf("%s:%s:", topic, channel)

	consumersMutex.RLock()
	var matchingConsumers []*nsq.Consumer
	for key, consumer := range consumers {
		if strings.HasPrefix(key, prefix) {
			matchingConsumers = append(matchingConsumers, consumer)
		}
	}
	consumersMutex.RUnlock()

	if len(matchingConsumers) == 0 {
		http.Error(w, "No consumers found for this topic/channel", http.StatusNotFound)
		return
	}

	// Aggregate stats from all consumers
	var totalConnections, totalMessages, totalFinished, totalRequeued int
	for _, consumer := range matchingConsumers {
		stats := consumer.Stats()
		totalConnections += stats.Connections
		totalMessages += int(stats.MessagesReceived)
		totalFinished += int(stats.MessagesFinished)
		totalRequeued += int(stats.MessagesRequeued)
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"topic":       topic,
		"channel":     channel,
		"consumers":   len(matchingConsumers),
		"connections": totalConnections,
		"messages":    totalMessages,
		"finished":    totalFinished,
		"requeued":    totalRequeued,
	})
}

// RdyRequest structure for controlling consumer RDY state
type RdyRequest struct {
	Count int `json:"count"`
}

// handleConsumerRdy handles RDY control for all consumers of a topic/channel
func handleConsumerRdy(w http.ResponseWriter, r *http.Request, topic, channel string) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req RdyRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, fmt.Sprintf("Invalid JSON: %v", err), http.StatusBadRequest)
		return
	}

	prefix := fmt.Sprintf("%s:%s:", topic, channel)

	consumersMutex.RLock()
	var matchingConsumers []*nsq.Consumer
	for key, consumer := range consumers {
		if strings.HasPrefix(key, prefix) {
			matchingConsumers = append(matchingConsumers, consumer)
		}
	}
	consumersMutex.RUnlock()

	if len(matchingConsumers) == 0 {
		http.Error(w, "No consumers found for this topic/channel", http.StatusNotFound)
		return
	}

	// Apply RDY count to all consumers for this topic/channel
	for _, consumer := range matchingConsumers {
		consumer.ChangeMaxInFlight(req.Count)
	}

	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]interface{}{
		"status":    "ok",
		"consumers": len(matchingConsumers),
	})
}

// handleMessages handles message lifecycle operations
// POST /api/messages/:messageId/touch - extend message timeout
// POST /api/messages/:messageId/finish - mark message as successfully processed
// POST /api/messages/:messageId/requeue - requeue message (fail)
func handleMessages(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	path := r.URL.Path[len("/api/messages/"):]
	parts := splitPath(path)

	if len(parts) < 2 {
		http.Error(w, "Message ID and action required", http.StatusBadRequest)
		return
	}

	messageID := parts[0]
	action := parts[1]

	activeMessagesMutex.RLock()
	msgWithExpiry, exists := activeMessages[messageID]
	activeMessagesMutex.RUnlock()

	if !exists {
		http.Error(w, "Message not found or already processed", http.StatusNotFound)
		return
	}

	msg := msgWithExpiry.message

	switch action {
	case "touch":
		msg.Touch()
		// Extend expiry time when touched
		activeMessagesMutex.Lock()
		if mwe, ok := activeMessages[messageID]; ok {
			mwe.expiry = time.Now().Add(messageExpiryDuration)
		}
		activeMessagesMutex.Unlock()
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(map[string]string{"status": "ok", "action": "touched"})
	case "finish":
		msg.Finish()
		activeMessagesMutex.Lock()
		delete(activeMessages, messageID)
		activeMessagesMutex.Unlock()
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(map[string]string{"status": "ok", "action": "finished"})
	case "requeue":
		// Parse delay from query parameter (optional, in seconds)
		delayStr := r.URL.Query().Get("delay")
		var delay time.Duration = -1 // default no delay (immediate requeue)
		if delayStr != "" {
			if d, err := strconv.Atoi(delayStr); err == nil {
				delay = time.Duration(d) * time.Second
			}
		}
		msg.Requeue(delay)
		activeMessagesMutex.Lock()
		delete(activeMessages, messageID)
		activeMessagesMutex.Unlock()
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(map[string]string{"status": "ok", "action": "requeued"})
	default:
		http.Error(w, "Invalid action. Use: touch, finish, or requeue", http.StatusBadRequest)
	}
}

// handleConsumerEvents handles SSE endpoint for consuming messages
// GET /api/events?topic=<topic>&channel=<channel>
// Each HTTP client gets its own NSQ consumer for native load balancing
func handleConsumerEvents(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// Parse query parameters for topic and channel
	topic := r.URL.Query().Get("topic")
	channel := r.URL.Query().Get("channel")

	if topic == "" || channel == "" {
		http.Error(w, "Topic and channel query parameters are required", http.StatusBadRequest)
		return
	}

	// Set SSE headers
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("Access-Control-Allow-Origin", "*")

	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "Streaming unsupported", http.StatusInternalServerError)
		return
	}

	// Create a unique consumer ID for this HTTP client
	consumerID := atomic.AddUint64(&consumerIDCounter, 1)
	consumerKey := fmt.Sprintf("%s:%s:%d", topic, channel, consumerID)

	// Create a channel for messages from this consumer
	messageChan := make(chan *nsq.Message, 100)

	// Create a new NSQ consumer for this HTTP client
	config := nsq.NewConfig()
	consumer, err := nsq.NewConsumer(topic, channel, config)
	if err != nil {
		http.Error(w, fmt.Sprintf("Failed to create consumer: %v", err), http.StatusInternalServerError)
		return
	}

	// Store consumer for RDY control
	consumersMutex.Lock()
	consumers[consumerKey] = consumer
	consumersMutex.Unlock()

	// Cleanup on disconnect
	defer func() {
		consumersMutex.Lock()
		delete(consumers, consumerKey)
		consumersMutex.Unlock()
		consumer.Stop()
		close(messageChan)
		log.Printf("Consumer %s stopped and cleaned up", consumerKey)
	}()

	// Add message handler
	consumer.AddHandler(nsq.HandlerFunc(func(message *nsq.Message) error {
		// Store message for lifecycle management
		messageID := fmt.Sprintf("%s", message.ID)
		activeMessagesMutex.Lock()
		activeMessages[messageID] = &messageWithExpiry{
			message: message,
			expiry:  time.Now().Add(messageExpiryDuration),
		}
		activeMessagesMutex.Unlock()

		// Disable auto-response, let client control lifecycle
		message.DisableAutoResponse()

		select {
		case messageChan <- message:
			return nil
		default:
			// Channel full, requeue
			activeMessagesMutex.Lock()
			delete(activeMessages, messageID)
			activeMessagesMutex.Unlock()
			return fmt.Errorf("message channel full")
		}
	}))

	// Connect to NSQd
	if err := consumer.ConnectToNSQD(*nsqdAddress); err != nil {
		http.Error(w, fmt.Sprintf("Failed to connect to NSQd: %v", err), http.StatusInternalServerError)
		return
	}

	log.Printf("Consumer %s connected for topic=%s channel=%s", consumerKey, topic, channel)

	// Stream messages as SSE
	ctx := r.Context()
	for {
		select {
		case <-ctx.Done():
			// Client disconnected
			return
		case msg, ok := <-messageChan:
			if !ok {
				return
			}

			messageID := fmt.Sprintf("%s", msg.ID)

			// Convert message to JSON
			data := map[string]interface{}{
				"id":        messageID,
				"timestamp": msg.Timestamp,
				"attempts":  msg.Attempts,
				"body":      string(msg.Body),
			}

			jsonData, err := json.Marshal(data)
			if err != nil {
				log.Printf("Failed to marshal message: %v", err)
				msg.Requeue(-1)
				activeMessagesMutex.Lock()
				delete(activeMessages, messageID)
				activeMessagesMutex.Unlock()
				continue
			}

			// Send as SSE event
			fmt.Fprintf(w, "data: %s\n\n", jsonData)
			flusher.Flush()
		}
	}
}

// splitPath splits URL path by slashes
func splitPath(path string) []string {
	path = strings.Trim(path, "/")
	if path == "" {
		return []string{}
	}
	return strings.Split(path, "/")
}

// handleAdmin passes through requests to NSQd admin HTTP API
func handleAdmin(w http.ResponseWriter, r *http.Request) {
	// Strip the /admin prefix before proxying to NSQd
	targetPath := strings.TrimPrefix(r.URL.Path, "/admin")

	// Build the target URL
	targetURL := fmt.Sprintf("http://%s%s", *nsqdHTTPAddr, targetPath)
	if r.URL.RawQuery != "" {
		targetURL += "?" + r.URL.RawQuery
	}

	// Create a new request
	proxyReq, err := http.NewRequest(r.Method, targetURL, r.Body)
	if err != nil {
		http.Error(w, fmt.Sprintf("Failed to create proxy request: %v", err), http.StatusInternalServerError)
		return
	}

	// Copy headers
	for key, values := range r.Header {
		for _, value := range values {
			proxyReq.Header.Add(key, value)
		}
	}

	// Execute the request
	client := &http.Client{}
	resp, err := client.Do(proxyReq)
	if err != nil {
		http.Error(w, fmt.Sprintf("Failed to proxy request: %v", err), http.StatusBadGateway)
		return
	}
	defer resp.Body.Close()

	// Copy response headers
	for key, values := range resp.Header {
		for _, value := range values {
			w.Header().Add(key, value)
		}
	}

	// Copy status code
	w.WriteHeader(resp.StatusCode)

	// Copy response body
	io.Copy(w, resp.Body)
}
