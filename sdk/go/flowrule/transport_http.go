package flowrule

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sync"
)

type httpTransport struct {
	cfg     *TransportConfig
	client  *http.Client
	baseURL string
	mu      sync.RWMutex
	closed  bool
}

func newHTTPTransport(cfg *TransportConfig) (Transport, error) {
	client := &http.Client{
		Timeout: cfg.Config.HTTPTimeout,
	}

	return &httpTransport{
		cfg:     cfg,
		client:  client,
		baseURL: cfg.Config.HTTPAddr,
	}, nil
}

func (t *httpTransport) Send(ctx context.Context, env *Envelope) error {
	t.mu.RLock()
	defer t.mu.RUnlock()

	if t.closed {
		return ErrTransportClosed
	}

	data, err := json.Marshal(env)
	if err != nil {
		return fmt.Errorf("marshal envelope: %w", err)
	}

	// POST /v1/events/{type}
	url := fmt.Sprintf("%s/v1/events/%s", t.baseURL, env.Type)

	sendCtx, cancel := context.WithTimeout(ctx, t.cfg.Config.SendTimeout)
	defer cancel()

	req, err := http.NewRequestWithContext(sendCtx, http.MethodPost, url, bytes.NewReader(data))
	if err != nil {
		return fmt.Errorf("create request: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Tenant-ID", env.TenantID)
	if env.CorrelationID != "" {
		req.Header.Set("X-Correlation-ID", env.CorrelationID)
	}
	if env.WorkflowID != "" {
		req.Header.Set("X-Workflow-ID", env.WorkflowID)
	}

	resp, err := t.client.Do(req)
	if err != nil {
		return fmt.Errorf("send request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("HTTP %d: %s", resp.StatusCode, string(body))
	}

	return nil
}

func (t *httpTransport) SendBatch(ctx context.Context, envs []*Envelope) error {
	t.mu.RLock()
	defer t.mu.RUnlock()

	if t.closed {
		return ErrTransportClosed
	}

	if len(envs) == 0 {
		return nil
	}

	// For batch, send to a batch endpoint
	// For now, send sequentially
	for _, env := range envs {
		if err := t.Send(ctx, env); err != nil {
			return err
		}
	}

	return nil
}

func (t *httpTransport) Close() error {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.closed {
		return nil
	}
	t.closed = true

	if t.client != nil {
		t.client.CloseIdleConnections()
	}
	return nil
}

func (t *httpTransport) Name() string {
	return "http"
}