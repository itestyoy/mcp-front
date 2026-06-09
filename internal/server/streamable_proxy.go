package server

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/stainless-api/mcp-front/internal/config"
	"github.com/stainless-api/mcp-front/internal/jsonrpc"
	"github.com/stainless-api/mcp-front/internal/log"
)

var errNoBackendSessionID = errors.New("backend did not return Mcp-Session-Id header")

// backendSSEKeepAlives holds cancel functions for background SSE connections
// that keep backend sessions alive (e.g. Playwright MCP kills sessions
// without an active SSE listener after ~8s due to a hardcoded heartbeat).
var backendSSEKeepAlives sync.Map // map[string]context.CancelFunc

// forwardStreamablePostToBackend handles POST requests for streamable-http transport
func forwardStreamablePostToBackend(ctx context.Context, w http.ResponseWriter, r *http.Request, cfg *config.MCPClientConfig) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		log.LogErrorWithFields("streamable_proxy", "Failed to read request body", map[string]any{
			"error": err.Error(),
		})
		jsonrpc.WriteError(w, nil, jsonrpc.InternalError, "Failed to read request")
		return
	}

	resp, err := sendStreamablePost(ctx, body, r.Header, cfg)
	if err != nil {
		log.LogErrorWithFields("streamable_proxy", "Backend request failed", map[string]any{
			"error": err.Error(),
			"url":   cfg.URL,
		})
		jsonrpc.WriteError(w, nil, jsonrpc.InternalError, "backend request failed")
		return
	}

	// If 404, the backend session is stale (e.g. backend restarted).
	// Re-initialize a fresh session with the backend and retry.
	if resp.StatusCode == http.StatusNotFound && r.Header.Get("Mcp-Session-Id") != "" {
		io.Copy(io.Discard, resp.Body)
		resp.Body.Close()

		log.LogInfoWithFields("streamable_proxy", "Backend session stale, re-initializing", map[string]any{
			"backendURL": cfg.URL,
		})

		newSessionID, err := initBackendSession(ctx, r.Header, cfg)
		if err != nil {
			log.LogErrorWithFields("streamable_proxy", "Failed to re-initialize backend session", map[string]any{
				"error":      err.Error(),
				"backendURL": cfg.URL,
			})
			jsonrpc.WriteError(w, nil, jsonrpc.InternalError, "backend session recovery failed")
			return
		}

		// Retry the original request with the new backend session
		retryHeaders := r.Header.Clone()
		retryHeaders.Set("Mcp-Session-Id", newSessionID)

		resp, err = sendStreamablePost(ctx, body, retryHeaders, cfg)
		if err != nil {
			log.LogErrorWithFields("streamable_proxy", "Backend request failed after session recovery", map[string]any{
				"error": err.Error(),
				"url":   cfg.URL,
			})
			jsonrpc.WriteError(w, nil, jsonrpc.InternalError, "backend request failed")
			return
		}
	}
	defer resp.Body.Close()

	contentType := resp.Header.Get("Content-Type")

	if strings.HasPrefix(contentType, "text/event-stream") {
		log.LogInfoWithFields("streamable_proxy", "Backend returned SSE stream", map[string]any{
			"status": resp.StatusCode,
		})

		for k, v := range resp.Header {
			if k == "Content-Type" || k == "Cache-Control" || k == "Connection" || k == "Mcp-Session-Id" {
				w.Header()[k] = v
			}
		}

		w.WriteHeader(resp.StatusCode)

		flusher, ok := w.(http.Flusher)
		if !ok {
			log.LogError("Response writer doesn't support flushing")
			return
		}

		streamSSEResponse(w, flusher, resp.Body, "streamable_proxy")
	} else {
		maps.Copy(w.Header(), resp.Header)

		w.WriteHeader(resp.StatusCode)

		if _, err := io.Copy(w, resp.Body); err != nil {
			log.LogErrorWithFields("streamable_proxy", "Failed to copy response body", map[string]any{
				"error": err.Error(),
			})
		}
	}
}

// sendStreamablePost sends a single POST request to the backend.
func sendStreamablePost(ctx context.Context, body []byte, srcHeaders http.Header, cfg *config.MCPClientConfig) (*http.Response, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, cfg.URL, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}

	copyRequestHeaders(req.Header, srcHeaders)
	for k, v := range cfg.Headers {
		req.Header.Set(k, v)
	}
	req.Header.Set("Accept", "application/json, text/event-stream")

	return (&http.Client{Timeout: cfg.Timeout}).Do(req)
}

// initBackendSession creates a fresh MCP session with the backend by sending
// initialize + notifications/initialized, and returns the new Mcp-Session-Id.
func initBackendSession(ctx context.Context, srcHeaders http.Header, cfg *config.MCPClientConfig) (string, error) {
	headers := srcHeaders.Clone()
	headers.Del("Mcp-Session-Id")

	// Step 1: send initialize
	initBody := []byte(`{"jsonrpc":"2.0","id":"_mcp_front_reinit","method":"initialize","params":{"protocolVersion":"2025-11-25","capabilities":{},"clientInfo":{"name":"mcp-front","version":"1.0.0"}}}`)

	resp, err := sendStreamablePost(ctx, initBody, headers, cfg)
	if err != nil {
		return "", err
	}
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("backend initialize returned status %d", resp.StatusCode)
	}

	newSessionID := resp.Header.Get("Mcp-Session-Id")
	if newSessionID == "" {
		return "", errNoBackendSessionID
	}

	log.LogInfoWithFields("streamable_proxy", "New backend session established", map[string]any{
		"backendURL": cfg.URL,
		"sessionID":  newSessionID,
	})

	// Start SSE keepalive to prevent session timeout on backends that require
	// an active listener (e.g. Playwright MCP's hardcoded heartbeat).
	startSSEKeepAlive(cfg.URL, newSessionID)

	// Step 2: send initialized notification
	notifyBody := []byte(`{"jsonrpc":"2.0","method":"notifications/initialized"}`)
	notifyHeaders := headers.Clone()
	notifyHeaders.Set("Mcp-Session-Id", newSessionID)

	resp, err = sendStreamablePost(ctx, notifyBody, notifyHeaders, cfg)
	if err != nil {
		return newSessionID, nil // session exists, notification failure is non-fatal
	}
	io.Copy(io.Discard, resp.Body)
	resp.Body.Close()

	return newSessionID, nil
}

// startSSEKeepAlive opens a background GET/SSE connection to the backend and
// responds to JSON-RPC ping requests to keep the session alive. Playwright MCP
// sends pings every 3s with a 5s timeout — if unanswered, the session is killed.
func startSSEKeepAlive(backendURL, sessionID string) {
	// Cancel any existing keepalive for this backend
	if cancel, ok := backendSSEKeepAlives.LoadAndDelete(backendURL); ok {
		cancel.(context.CancelFunc)()
	}

	ctx, cancel := context.WithCancel(context.Background())
	backendSSEKeepAlives.Store(backendURL, cancel)

	go func() {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, backendURL, nil)
		if err != nil {
			return
		}
		req.Header.Set("Accept", "text/event-stream")
		req.Header.Set("Mcp-Session-Id", sessionID)

		resp, err := (&http.Client{}).Do(req)
		if err != nil {
			return
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			return
		}

		log.LogInfoWithFields("streamable_proxy", "SSE keepalive connected", map[string]any{
			"backendURL": backendURL,
			"sessionID":  sessionID,
		})

		// Read SSE events and respond to pings
		scanner := bufio.NewScanner(resp.Body)
		for scanner.Scan() {
			line := scanner.Text()
			if !strings.HasPrefix(line, "data: ") {
				continue
			}
			data := line[6:]
			// Check if this is a ping request (contains "method":"ping")
			if !strings.Contains(data, `"ping"`) {
				continue
			}
			// Extract the request ID to build a proper response
			// Ping format: {"jsonrpc":"2.0","id":"...","method":"ping"}
			var msg struct {
				ID      any    `json:"id"`
				Method  string `json:"method"`
			}
			if err := json.Unmarshal([]byte(data), &msg); err != nil || msg.Method != "ping" {
				continue
			}
			// Respond with empty result
			pongBody, _ := json.Marshal(map[string]any{
				"jsonrpc": "2.0",
				"id":      msg.ID,
				"result":  map[string]any{},
			})
			pongReq, err := http.NewRequestWithContext(ctx, http.MethodPost, backendURL, bytes.NewReader(pongBody))
			if err != nil {
				continue
			}
			pongReq.Header.Set("Content-Type", "application/json")
			pongReq.Header.Set("Accept", "application/json, text/event-stream")
			pongReq.Header.Set("Mcp-Session-Id", sessionID)
			pongResp, err := (&http.Client{Timeout: 5 * time.Second}).Do(pongReq)
			if err != nil {
				continue
			}
			io.Copy(io.Discard, pongResp.Body)
			pongResp.Body.Close()
		}

		log.LogInfoWithFields("streamable_proxy", "SSE keepalive disconnected", map[string]any{
			"backendURL": backendURL,
		})
	}()
}
