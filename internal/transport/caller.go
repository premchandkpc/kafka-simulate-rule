package transport

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"time"
)

// HTTPCaller manages HTTP connections to downstream services.
type HTTPCaller struct {
	client *http.Client
}

// NewHTTPCaller creates a new HTTP caller with a configured connection pool.
func NewHTTPCaller(timeout time.Duration) *HTTPCaller {
	return &HTTPCaller{
		client: &http.Client{
			Timeout: timeout,
			Transport: &http.Transport{
				MaxIdleConns:        100,
				MaxIdleConnsPerHost: 20,
				IdleConnTimeout:     90 * time.Second,
				DisableCompression:  false,
			},
		},
	}
}

// Call sends a POST request to the target with the given body.
// Returns the response body.
func (c *HTTPCaller) Call(ctx context.Context, target string, body []byte) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, target, bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("caller: new request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("caller: do: %w", err)
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("caller: read body: %w", err)
	}

	if resp.StatusCode >= 400 {
		return nil, fmt.Errorf("caller: %s returned %d: %s", target, resp.StatusCode, string(respBody))
	}

	return respBody, nil
}

// Emit sends a fire-and-forget POST request.
func (c *HTTPCaller) Emit(ctx context.Context, target string, body []byte) error {
	go func() {
		emitCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		req, err := http.NewRequestWithContext(emitCtx, http.MethodPost, target, bytes.NewReader(body))
		if err != nil {
			return
		}
		req.Header.Set("Content-Type", "application/json")
		resp, err := c.client.Do(req)
		if err != nil {
			return
		}
		resp.Body.Close()
	}()
	return nil
}
