package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/nsqio/go-nsq"
)

const testToken = "test-bearer-token"

func setupTestEnv() {
	// Set bearer token for tests
	*bearerToken = testToken
	// Pre-calculate bearer token hash for constant-time comparison
	bearerTokenHash = sha256.Sum256([]byte(testToken))
}

func TestAuthMiddleware(t *testing.T) {
	setupTestEnv()

	handler := authMiddleware(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("authenticated"))
	})

	tests := []struct {
		name           string
		authHeader     string
		expectedStatus int
	}{
		{
			name:           "Valid token",
			authHeader:     "Bearer " + testToken,
			expectedStatus: http.StatusOK,
		},
		{
			name:           "Missing token",
			authHeader:     "",
			expectedStatus: http.StatusUnauthorized,
		},
		{
			name:           "Invalid token",
			authHeader:     "Bearer wrong-token",
			expectedStatus: http.StatusUnauthorized,
		},
		{
			name:           "Invalid format",
			authHeader:     "Basic " + testToken,
			expectedStatus: http.StatusUnauthorized,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest("GET", "/test", nil)
			if tt.authHeader != "" {
				req.Header.Set("Authorization", tt.authHeader)
			}

			rr := httptest.NewRecorder()
			handler(rr, req)

			if rr.Code != tt.expectedStatus {
				t.Errorf("expected status %d, got %d", tt.expectedStatus, rr.Code)
			}
		})
	}
}

func TestSplitPath(t *testing.T) {
	tests := []struct {
		path     string
		expected []string
	}{
		{
			path:     "topic/channel",
			expected: []string{"topic", "channel"},
		},
		{
			path:     "topic/channel/rdy",
			expected: []string{"topic", "channel", "rdy"},
		},
		{
			path:     "messages/12345/finish",
			expected: []string{"messages", "12345", "finish"},
		},
		{
			path:     "",
			expected: []string{},
		},
		{
			path:     "single",
			expected: []string{"single"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.path, func(t *testing.T) {
			result := splitPath(tt.path)
			if len(result) != len(tt.expected) {
				t.Errorf("expected length %d, got %d", len(tt.expected), len(result))
				return
			}
			for i, v := range result {
				if v != tt.expected[i] {
					t.Errorf("at index %d: expected %s, got %s", i, tt.expected[i], v)
				}
			}
		})
	}
}

func TestMessageStructures(t *testing.T) {
	t.Run("Message JSON", func(t *testing.T) {
		jsonData := `{"data": {"key": "value"}}`
		var msg Message
		err := json.Unmarshal([]byte(jsonData), &msg)
		if err != nil {
			t.Fatalf("failed to unmarshal: %v", err)
		}
		if string(msg.Data) != `{"key": "value"}` {
			t.Errorf("expected data to be preserved, got: %s", msg.Data)
		}
	})

	t.Run("MultiMessage JSON", func(t *testing.T) {
		jsonData := `{"messages": ["msg1", "msg2", "msg3"]}`
		var msg MultiMessage
		err := json.Unmarshal([]byte(jsonData), &msg)
		if err != nil {
			t.Fatalf("failed to unmarshal: %v", err)
		}
		if len(msg.Messages) != 3 {
			t.Errorf("expected 3 messages, got %d", len(msg.Messages))
		}
	})

	t.Run("RdyRequest JSON", func(t *testing.T) {
		jsonData := `{"count": 10}`
		var req RdyRequest
		err := json.Unmarshal([]byte(jsonData), &req)
		if err != nil {
			t.Fatalf("failed to unmarshal: %v", err)
		}
		if req.Count != 10 {
			t.Errorf("expected count 10, got %d", req.Count)
		}
	})
}

func TestHandlePubValidation(t *testing.T) {
	setupTestEnv()

	tests := []struct {
		name           string
		method         string
		topic          string
		body           string
		expectedStatus int
	}{
		{
			name:           "Invalid method",
			method:         "GET",
			topic:          "test",
			body:           `{"data": "test"}`,
			expectedStatus: http.StatusMethodNotAllowed,
		},
		{
			name:           "Invalid JSON",
			method:         "POST",
			topic:          "test",
			body:           `invalid json`,
			expectedStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest(tt.method, "/api/topics/"+tt.topic+"/messages", bytes.NewBufferString(tt.body))
			req.Header.Set("Authorization", "Bearer "+testToken)
			req.Header.Set("Content-Type", "application/json")

			rr := httptest.NewRecorder()
			handlePub(rr, req, tt.topic)

			if rr.Code != tt.expectedStatus {
				t.Errorf("expected status %d, got %d", tt.expectedStatus, rr.Code)
			}
		})
	}
}

func TestHandleMpubValidation(t *testing.T) {
	setupTestEnv()

	tests := []struct {
		name           string
		method         string
		topic          string
		body           string
		expectedStatus int
	}{
		{
			name:           "Invalid method",
			method:         "GET",
			topic:          "test",
			body:           `{"messages": ["test"]}`,
			expectedStatus: http.StatusMethodNotAllowed,
		},
		{
			name:           "Invalid JSON",
			method:         "POST",
			topic:          "test",
			body:           `invalid json`,
			expectedStatus: http.StatusBadRequest,
		},
		{
			name:           "Empty messages",
			method:         "POST",
			topic:          "test",
			body:           `{"messages": []}`,
			expectedStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest(tt.method, "/api/topics/"+tt.topic+"/messages/batch", bytes.NewBufferString(tt.body))
			req.Header.Set("Authorization", "Bearer "+testToken)
			req.Header.Set("Content-Type", "application/json")

			rr := httptest.NewRecorder()
			handleMpub(rr, req, tt.topic)

			if rr.Code != tt.expectedStatus {
				t.Errorf("expected status %d, got %d", tt.expectedStatus, rr.Code)
			}
		})
	}
}

func TestHandleConsumerRdyValidation(t *testing.T) {
	setupTestEnv()

	tests := []struct {
		name           string
		method         string
		body           string
		expectedStatus int
	}{
		{
			name:           "Invalid method",
			method:         "GET",
			body:           `{"count": 5}`,
			expectedStatus: http.StatusMethodNotAllowed,
		},
		{
			name:           "Invalid JSON",
			method:         "POST",
			body:           `invalid json`,
			expectedStatus: http.StatusBadRequest,
		},
		{
			name:           "Consumer not found",
			method:         "POST",
			body:           `{"count": 5}`,
			expectedStatus: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest(tt.method, "/api/consumers/test/channel/rdy", bytes.NewBufferString(tt.body))
			req.Header.Set("Authorization", "Bearer "+testToken)
			req.Header.Set("Content-Type", "application/json")

			rr := httptest.NewRecorder()
			handleConsumerRdy(rr, req, "test", "channel")

			if rr.Code != tt.expectedStatus {
				t.Errorf("expected status %d, got %d", tt.expectedStatus, rr.Code)
			}
		})
	}
}

func TestHandleMessagesValidation(t *testing.T) {
	setupTestEnv()

	tests := []struct {
		name           string
		method         string
		path           string
		expectedStatus int
	}{
		{
			name:           "Invalid method",
			method:         "GET",
			path:           "/api/messages/123/finish",
			expectedStatus: http.StatusMethodNotAllowed,
		},
		{
			name:           "Invalid path",
			method:         "POST",
			path:           "/api/messages/123",
			expectedStatus: http.StatusBadRequest,
		},
		{
			name:           "Message not found",
			method:         "POST",
			path:           "/api/messages/nonexistent/finish",
			expectedStatus: http.StatusNotFound,
		},
		{
			name:           "Invalid action",
			method:         "POST",
			path:           "/api/messages/123/invalid",
			expectedStatus: http.StatusNotFound, // Will fail at message lookup first
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest(tt.method, tt.path, nil)
			req.Header.Set("Authorization", "Bearer "+testToken)

			rr := httptest.NewRecorder()
			handleMessages(rr, req)

			if rr.Code != tt.expectedStatus {
				t.Errorf("expected status %d, got %d", tt.expectedStatus, rr.Code)
			}
		})
	}
}

// TestContentTypeHeaders verifies that JSON endpoints return application/json Content-Type
func TestContentTypeHeaders(t *testing.T) {
	setupTestEnv()

	t.Run("Consumer status returns JSON Content-Type", func(t *testing.T) {
		// Create a mock consumer to test successful response
		consumersMutex.Lock()
		config := nsq.NewConfig()
		consumer, err := nsq.NewConsumer("test-topic", "test-channel", config)
		if err != nil {
			t.Fatalf("failed to create consumer: %v", err)
		}
		consumers["test-topic:test-channel:1"] = consumer
		consumersMutex.Unlock()

		defer func() {
			consumersMutex.Lock()
			delete(consumers, "test-topic:test-channel:1")
			consumersMutex.Unlock()
			consumer.Stop()
		}()

		req := httptest.NewRequest("GET", "/api/consumers/test-topic/test-channel", nil)
		req.Header.Set("Authorization", "Bearer "+testToken)

		rr := httptest.NewRecorder()
		handleConsumerStatus(rr, req, "test-topic", "test-channel")

		if rr.Code != http.StatusOK {
			t.Errorf("expected status %d, got %d", http.StatusOK, rr.Code)
		}

		contentType := rr.Header().Get("Content-Type")
		if contentType != "application/json" {
			t.Errorf("expected Content-Type application/json, got %s", contentType)
		}
	})
}
